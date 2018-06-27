/*
 * Copyright © 2014 TU Berlin (emma@dima.tu-berlin.de)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.emmalanguage
package compiler

import labyrinth.operators.ScalaOps
import labyrinth.partitioners._
import labyrinth.operators.InputFormatWithInputSplit
import org.emmalanguage.api.Group
import org.emmalanguage.labyrinth.LabyNode

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.typeutils.TypeUtils
import org.apache.flink.core.fs.FileInputSplit
import shapeless.::

import scala.runtime.Nothing$

trait LabyrinthLabynization extends LabyrinthCompilerBase {

  import API._
  import UniverseImplicits._

  val labyrinthLabynize: TreeTransform = TreeTransform("labyrinthLabynize", (tree: u.Tree) => {

    val outerTermName = api.TermName.fresh("OUTER")
    val outerToEncl = Map(outerTermName.toString -> enclosingOwner.name.toString)
    def name(ts: u.TermSymbol): String = {
      if (outerToEncl.keys.toList.contains(ts.name.toString)) {
        outerToEncl(ts.name.toString)
      } else {
        ts.name.toString
      }
    }

    // wrap outer block into dummy defdef to get all basic block affiliations
    val wraptree = tree match {
      case lt @ core.Let(_,_,_) => {
        val owner = enclosingOwner
        val dd = core.DefDef(
          api.DefSym(owner, outerTermName, res = api.Type.any),
          Seq(),
          Seq(),
          lt
        )
        val expr = api.Term.unit
        core.Let(Seq(), Seq(dd), expr)
      }
      case t: u.Tree => t
    }

    // --- get control flow info -- //
    val G = ControlFlow.cfg(wraptree)
    val bbParents = G.ctrl.edges.map(e => name(e.from)).toSet
    val bbChildren = G.ctrl.edges.map(e => name(e.to)).toSet
    // bbids
    val bbIdMap = scala.collection.mutable.Map[String, Int]()
    bbIdMap += (enclosingOwner.name.toString -> 0)
    var idCounter = 1
    bbChildren.foreach( e => { bbIdMap += (e -> idCounter); idCounter += 1 })
    // define dependencies
    val gDependencies = scala.collection.mutable.Map[String, Seq[String]]()
    G.ctrl.edges.foreach(n =>
      if (!gDependencies.keys.toList.contains(name(n.from))) { gDependencies += (name(n.from) -> Seq(name(n.to))) }
      else { gDependencies(name(n.from)) = gDependencies(name(n.from)) :+ name(n.to) }
    )
    val gBbDependencies = scala.collection.mutable.Map[Int, Seq[Int]]()
    gDependencies.keys.foreach(k => gBbDependencies += (bbIdMap(k) -> gDependencies(k).map(bbIdMap(_))) )
    // number of outgoing edges
    val bbIdNumOut = scala.collection.mutable.Map[Int, Int]()
    gDependencies.keys.foreach(n => bbIdNumOut += (bbIdMap(n) -> gDependencies(n).size))

    def bbIdShortcut(start: Int): Seq[Int] = {
      if (!bbIdNumOut.keys.toList.contains(start) || bbIdNumOut(start) != 1) {
        Seq(start)
      } else {
        Seq(start) ++ gBbDependencies(start).flatMap( id => bbIdShortcut(id) )
      }
    }
    // --- ofni wolf lortnoc teg -- //

    // transformation helpers
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
    val symToPhiRef = scala.collection.mutable.Map[u.TermSymbol, u.Ident]()
    // here we have to use names because we need mapping from different blocks during the transformation and we
    // generate new symbols during pattern matching (should be no problem though, as they are unique after lifting)
    val defSymNameToPhiRef = scala.collection.mutable.Map[String, u.Ident]()

    var valDefsFinal = Seq[u.ValDef]()

    // first traversal does the labyrinth labynization. second for block type correction.
    val trans1 = api.TopDown
      .withOwner
      .traverseWith {
        case Attr.inh(vd @ core.ValDef(_, core.Lambda(_,_,_)), _) => valDefsFinal = valDefsFinal :+ vd
        case Attr.inh(vd @ core.ValDef(_, rhs), _) if isAlg(rhs) => valDefsFinal = valDefsFinal :+ vd
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if prePrint(vd) && !isFun(lhs) && !isFun(owner) && !isAlg(rhs) => {

          rhs match {
            case dc @ core.DefCall(_, DB$.fromSingSrcReadText, _, Seq(Seq(core.ValRef(dbPathSym)))) if prePrint(dc) =>
              val dbPathSymRepl = replacements(dbPathSym)
              val dbPathSymReplRef = core.ValRef(dbPathSymRepl)

              //// get splits
              // bagoperator
              val bagOpSplitsVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.textSource, Seq(), Seq())
              val bagOpSplitsRefDef = valRefAndDef(owner, "inputSplits", bagOpSplitsVDrhs)

              // partitioner
              val targetParaSplits = 1
              val partSplitsVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(dbPathSym.info.typeArgs.head),
                Seq(Seq(core.Lit(targetParaSplits)))
              )
              val partSplitsRefDef = valRefAndDef(owner, "partitioner", partSplitsVDrhs)

              // typeinfo OUT for splits
              val tpeOutSplits = api.Type.apply(
                getTpe[InputFormatWithInputSplit[Any, FileInputSplit]].widen.typeConstructor,
                Seq(u.typeOf[String], getTpe[FileInputSplit])
              )
              val typeInfoOUTSplitsRefDef = getTypeInfoForTypeRefDef(
                owner,
                tpeOutSplits
              )

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoSplitsRefDef =
                getElementOrEventTypeInfoRefDef(owner, tpeOutSplits, typeInfoOUTSplitsRefDef._1)

              // LabyNode
              val labyNodeSplitsRefDef = getLabyNodeRefDef(
                owner,
                (u.typeOf[String], tpeOutSplits),
                "inputSplits",
                bagOpSplitsRefDef._1,
                bbIdMap(owner.name.toString),
                partSplitsRefDef._1,
                elementOrEventTypeInfoSplitsRefDef._1
              )

              // addInput
              val addInputSplitsRefDef = getAddInputRefDef(owner, labyNodeSplitsRefDef._1, dbPathSymReplRef)

              // setParallelism
              val SetParallelismSplitsRefDef = getSetParallelismRefDefSym(owner, addInputSplitsRefDef._1, 1)


              //// read splits
              // bagoperator
              val bagOpReadVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.textReader, Seq(), Seq())
              val bagOpReadRefDef = valRefAndDef(owner, "readSplits", bagOpReadVDrhs)

              // partitioner
              val targetParaRead = 1
              val partReadVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeOutSplits),
                Seq(Seq(core.Lit(targetParaRead)))
              )
              val partReadRefDef = valRefAndDef(owner, "partitioner", partReadVDrhs)

              // typeinfo OUT
              val tpeOutRead = u.typeOf[String]
              val typeInfoOUTReadRefDef = getTypeInfoForTypeRefDef(
                owner,
                tpeOutRead
              )

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoReadRefDef =
                getElementOrEventTypeInfoRefDef(owner, tpeOutRead, typeInfoOUTReadRefDef._1)

              // LabyNode
              val labyNodeReadRefDef = getLabyNodeRefDef(
                owner,
                (tpeOutSplits, tpeOutRead),
                "readSplits",
                bagOpReadRefDef._1,
                bbIdMap(owner.name.toString),
                partReadRefDef._1,
                elementOrEventTypeInfoReadRefDef._1
              )

              // addInput
              val addInputReadRefDef = getAddInputRefDef(owner, labyNodeReadRefDef._1, SetParallelismSplitsRefDef._1)

              // setParallelism
              val SetParallelismReadRefDefSym = getSetParallelismRefDefSym(owner, addInputReadRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpSplitsRefDef._2, partSplitsRefDef._2, typeInfoOUTSplitsRefDef._2,
                  elementOrEventTypeInfoSplitsRefDef._2, labyNodeSplitsRefDef._2, addInputSplitsRefDef._2,
                  SetParallelismSplitsRefDef._2,
                  bagOpReadRefDef._2, partReadRefDef._2, typeInfoOUTReadRefDef._2,
                  elementOrEventTypeInfoReadRefDef._2, labyNodeReadRefDef._2, addInputReadRefDef._2,
                  SetParallelismReadRefDefSym._2)
              replacements += (lhs -> SetParallelismReadRefDefSym._3)
              skip(dc)

            // singSrc to LabyNode
            case dc @ core.DefCall(_, DB$.singSrc, Seq(targ), Seq(Seq(funarg))) if prePrint(dc)=>

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.fromNothing, Seq(targ), Seq(Seq(funarg)))
              val bagOpRefDef = valRefAndDef(owner, "fromNothing", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(getTpe[labyrinth.util.Nothing]),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, targ)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, targ, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (getTpe[labyrinth.util.Nothing], targ),
                "fromNothing",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // setParallelism
              val SetParallelismRefDefSym = getSetParallelismRefDefSym(owner, labyNodeRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, SetParallelismRefDefSym._2)
              replacements += (lhs -> SetParallelismRefDefSym._3)
              skip(dc)

            // fromSingSrc to LabyNode
            case dc @ core.DefCall(_, DB$.fromSingSrcApply, Seq(targ), Seq(Seq(core.ValRef(singSrcDBsym))))
              if prePrint(dc) =>

              val singSrcDBReplSym = replacements(singSrcDBsym)
              val singSrcDBReplRef = core.ValRef(singSrcDBReplSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.fromSingSrcApply,
                Seq(targ),
                Seq(Seq())
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              val inTpe = singSrcDBsym.info.typeArgs.head

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, targ)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, targ, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, targ),
                "fromSingSrcApply",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = singSrcDBReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, singSrcDBReplRef)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            // valref map to LabyNode
            case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.map, Seq(outTpe), Seq(Seq(lbdaRef)))
              if prePrint(dc) =>

              val tgtReplSym = replacements(tgtSym)
              val tgtReplRef = core.ValRef(tgtReplSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, outTpe)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, outTpe, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, outTpe),
                "map",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = tgtReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            // parref map to LabyNode
            case dc @ core.DefCall(Some(core.ParRef(tgtSym)), DataBag.map, Seq(outTpe), Seq(Seq(lbdaRef)))
              if prePrint(dc) =>

              val tgtReplSym = replacements(tgtSym)
              val tgtReplRef = core.ValRef(tgtReplSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, outTpe)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, outTpe, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, outTpe),
                "map",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = tgtReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            // flatmap to LabyNode
            case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.flatMap, Seq(outTpe), Seq(Seq(lbdaRef)))
              if prePrint(dc)=>

              val tgtReplSym = replacements(tgtSym)
              val tgtReplRef = core.ValRef(tgtReplSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.flatMapDataBagHelper,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, outTpe)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, outTpe, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, outTpe),
                "map",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = tgtReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            // map to LabyNode
            case dc @ core.DefCall(Some(core.ValRef(tgtSym)), DataBag.map, Seq(outTpe), Seq(Seq(lbdaRef)))
              if prePrint(dc) =>

              val tgtReplSym = replacements(tgtSym)
              val tgtReplRef = core.ValRef(tgtReplSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, outTpe)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, outTpe, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, outTpe),
                "map",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = tgtReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            // parRefflatmap to LabyNode
            case dc @ core.DefCall(Some(core.ParRef(tgtSym)), DataBag.flatMap, Seq(outTpe), Seq(Seq(lbdaRef)))
              if prePrint(dc)=>

              val tgtReplSym = replacements(tgtSym)
              val tgtReplRef = core.ValRef(tgtReplSym)

              val inTpe = tgtSym.info.typeArgs.head

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.flatMapDataBagHelper,
                Seq(inTpe, outTpe),
                Seq(Seq(lbdaRef))
              )
              val bagOpRefDef = valRefAndDef(owner, "fromSingSrcApply", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(inTpe),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, outTpe)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, outTpe, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (inTpe, outTpe),
                "map",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = tgtReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            case dc @ core.DefCall
              (_, Ops.cross, Seq(tpeA, tpeB), Seq(Seq(core.ValRef(lhsSym), core.ValRef(rhsSym)))) if prePrint(dc) =>


              // =========== Labynode map to Left() ===========

              // get replacement lhs
              val lhsReplSym = replacements(lhsSym)
              val lhsReplRef = core.ValRef(lhsReplSym)

              // bagoperator
              val parSymLhs = api.ParSym(owner, api.TermName.fresh("t"), tpeA)
              val parRefLhs = core.ParRef(parSymLhs)
              val lbdaCallLhs = core.DefCall(Some(Left$.ref), Left$.apply,
                Seq(tpeA, getTpe[scala.Nothing]), Seq(Seq(parRefLhs)))
              val lbdaCAllLhsRefDef = valRefAndDef(owner, "lambda", lbdaCallLhs)
              val lbdaBodyLhs = core.Let(Seq(lbdaCAllLhsRefDef._2), Seq(), lbdaCAllLhsRefDef._1)
              val mapLambdaLhs = core.Lambda(Seq(parSymLhs), lbdaBodyLhs)
              val mapLambdaLhsRefDef = valRefAndDef(owner, "lbdaLeft", mapLambdaLhs)

              val bagOpLhsVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(tpeA, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
                Seq(Seq(mapLambdaLhsRefDef._1))
              )
              val bagOpMapLhsRefDef = valRefAndDef(owner, "mapToLeftOp", bagOpLhsVDrhs)

              // partitioner
              val targetParaLhs = 1
              val partLhsVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeA),
                Seq(Seq(core.Lit(targetParaLhs)))
              )
              val partMapLhsRefDef = valRefAndDef(owner, "partitioner", partLhsVDrhs)

              // typeinfo OUT
              val typeInfoMapLhsOUTRefDef =
                getTypeInfoForTypeRefDef(owner, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)))

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoMapLhsRefDef =
                getElementOrEventTypeInfoRefDef(
                  owner,
                  api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)),
                  typeInfoMapLhsOUTRefDef._1)

              // LabyNode
              val labyNodeMapLhsRefDef = getLabyNodeRefDef(
                owner,
                (tpeA, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
                "map",
                bagOpMapLhsRefDef._1,
                bbIdMap(owner.name.toString),
                partMapLhsRefDef._1,
                elementOrEventTypeInfoMapLhsRefDef._1
              )

              // def insideblock
              val insideBlockLhs = lhsReplSym.owner == owner

              // addInput
              val addInputMapLhsRefDef = getAddInputRefDef(owner, labyNodeMapLhsRefDef._1, lhsReplRef, insideBlockLhs)

              // setParallelism
              val setParallelismMapLhsRefDefSym = getSetParallelismRefDefSym(owner, addInputMapLhsRefDef._1, 1)


              // =========== Labynode map to Right() ===========

              // get replacement rhs
              val rhsReplSym = replacements(rhsSym)
              val rhsReplRef = core.ValRef(rhsReplSym)

              // bagoperator
              val parSymRhs = api.ParSym(owner, api.TermName.fresh("t"), tpeB)
              val parRefRhs = core.ParRef(parSymRhs)
              val lbdaCallRhs = core.DefCall(Some(Right$.ref), Right$.apply,
                Seq(getTpe[scala.Nothing], tpeB), Seq(Seq(parRefRhs)))
              val lbdaCAllRhsRefDef = valRefAndDef(owner, "lambda", lbdaCallRhs)
              val lbdaBodyRhs = core.Let(Seq(lbdaCAllRhsRefDef._2), Seq(), lbdaCAllRhsRefDef._1)
              val mapLambdaRhs = core.Lambda(Seq(parSymRhs), lbdaBodyRhs)
              val mapLambdaRhsRefDef = valRefAndDef(owner, "lbdaRight", mapLambdaRhs)

              val bagOpRhsVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(tpeB, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
                Seq(Seq(mapLambdaRhsRefDef._1))
              )
              val bagOpMapRhsRefDef = valRefAndDef(owner, "mapToRightOp", bagOpRhsVDrhs)

              // partitioner
              val targetParaRhs = 1
              val partRhsVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeB),
                Seq(Seq(core.Lit(targetParaRhs)))
              )
              val partMapRhsRefDef = valRefAndDef(owner, "partitioner", partRhsVDrhs)

              // typeinfo OUT
              val typeInfoMapRhsOUTRefDef =
                getTypeInfoForTypeRefDef(owner, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)))

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoMapRhsRefDef = getElementOrEventTypeInfoRefDef(
                owner,
                api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)),
                typeInfoMapRhsOUTRefDef._1
              )

              // LabyNode
              val labyNodeMapRhsRefDef = getLabyNodeRefDef(
                owner,
                (tpeB, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
                "map",
                bagOpMapRhsRefDef._1,
                bbIdMap(owner.name.toString),
                partMapRhsRefDef._1,
                elementOrEventTypeInfoMapRhsRefDef._1
              )

              // def insideblock
              val insideBlockRhs = rhsReplSym.owner == owner

              // addInput
              val addInputMapRhsRefDef = getAddInputRefDef(owner, labyNodeMapRhsRefDef._1, rhsReplRef, insideBlockRhs)

              // setParallelism
              val setParallelismMapRhsRefDefSym = getSetParallelismRefDefSym(owner, addInputMapRhsRefDef._1, 1)


              // =========== Labynode cross ===========
              // bagoperator

              val bagOpCrossVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.cross,
                Seq(tpeA,tpeB),
                Seq()
              )
              val bagOpCrossRefDef = valRefAndDef(owner, "crossOp", bagOpCrossVDrhs)

              // partitioner
              val targetParaCross = 1
              val partVDcross = core.Inst(
                getTpe[Always0[Any]],
                Seq(api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB))),
                Seq(Seq(core.Lit(targetParaCross)))
              )
              val partCrossRefDef = valRefAndDef(owner, "partitioner", partVDcross)

              // typeinfo OUT
              val typeInfoMapCrossOUTRefDef = getTypeInfoForTypeRefDef(
                owner,
                api.Type.apply(getTpe[org.apache.flink.api.java.tuple.Tuple2[Any,Any]], Seq(tpeA, tpeB))
              )

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoCrossRefDef =
                getElementOrEventTypeInfoRefDef(
                  owner,
                  api.Type.apply(getTpe[org.apache.flink.api.java.tuple.Tuple2[Any,Any]], Seq(tpeA, tpeB)),
                  typeInfoMapCrossOUTRefDef._1)

              // LabyNode
              val labyNodeCrossRefDef = getLabyNodeRefDef(
                owner,
                (api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(tpeA, tpeB)),
                  api.Type.apply(getTpe[org.apache.flink.api.java.tuple.Tuple2[Any,Any]], Seq(tpeA, tpeB))),
                "cross",
                bagOpCrossRefDef._1,
                bbIdMap(owner.name.toString),
                partCrossRefDef._1,
                elementOrEventTypeInfoCrossRefDef._1
              )

              // def insideblock
              val insideBlockCross1 = setParallelismMapLhsRefDefSym._3.owner == owner
              val insideBlockCross2 = setParallelismMapRhsRefDefSym._3.owner == owner

              // addInput
              val addInputCrossRefDef1 =
                getAddInputRefDef(owner, labyNodeCrossRefDef._1, setParallelismMapLhsRefDefSym._1, insideBlockCross1)
              val addInputCrossRefDef2 =
                getAddInputRefDef(owner, addInputCrossRefDef1._1, setParallelismMapRhsRefDefSym._1, insideBlockCross2)

              // setParallelism
              val setParallelismCrossRefDef = getSetParallelismRefDefSym(owner, addInputCrossRefDef2._1, 1)


              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(mapLambdaLhsRefDef._2, bagOpMapLhsRefDef._2, partMapLhsRefDef._2, typeInfoMapLhsOUTRefDef._2,
                  elementOrEventTypeInfoMapLhsRefDef._2, labyNodeMapLhsRefDef._2, addInputMapLhsRefDef._2,
                  setParallelismMapLhsRefDefSym._2,
                  mapLambdaRhsRefDef._2, bagOpMapRhsRefDef._2, partMapRhsRefDef._2, typeInfoMapRhsOUTRefDef._2,
                  elementOrEventTypeInfoMapRhsRefDef._2, labyNodeMapRhsRefDef._2, addInputMapRhsRefDef._2,
                  setParallelismMapRhsRefDefSym._2,
                  bagOpCrossRefDef._2, partCrossRefDef._2, typeInfoMapCrossOUTRefDef._2,
                  elementOrEventTypeInfoCrossRefDef._2, labyNodeCrossRefDef._2, addInputCrossRefDef1._2,
                  addInputCrossRefDef2._2, setParallelismCrossRefDef._2)
              replacements += (lhs -> setParallelismCrossRefDef._3)
              skip(dc)

            // fold1
            case dc @ core.DefCall(_, DB$.fold1, targs @ Seq(tpeA, tpeB), Seq(Seq(core.ValRef(dbSym), alg)))
              if prePrint(dc) =>

              val dbReplSym = replacements(dbSym)
              val dbReplRef = core.ValRef(dbReplSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.foldAlgHelper, targs, Seq(Seq(alg)))
              val bagOpRefDef = valRefAndDef(owner, "fold1Op", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeA),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, tpeB)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, tpeB, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (tpeA, tpeB),
                "fold1",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = dbReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            case dc @ core.DefCall
              (_, DB$.fold2, targs @ Seq(tpeA, tpeB), Seq(Seq(core.ValRef(dbSym), zero, init, plus))) if prePrint(dc)=>

              val dbReplSym = replacements(dbSym)
              val dbReplRef = core.ValRef(dbReplSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.fold, targs, Seq(Seq(zero, init, plus)))
              val bagOpRefDef = valRefAndDef(owner, "fold2Op", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeA),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(owner, tpeB)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, tpeB, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (tpeA, tpeB),
                "fold2",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = dbReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            case dc @ core.DefCall
              (_, Ops.foldGroup, Seq(tpeA, tpeB, tpeK),
              Seq(Seq(core.ValRef(dbSym), extrRef @ core.ValRef(_), alg))) if prePrint(dc) =>

              val dbReplSym = replacements(dbSym)
              val dbReplRef = core.ValRef(dbReplSym)

              // bagoperator
              val bagOpVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.foldGroupAlgHelper,
                Seq(tpeK, tpeA, tpeB), Seq(Seq(extrRef, alg))
              )
              val bagOpRefDef = valRefAndDef(owner, "foldGroupOp", bagOpVDrhs)

              // partitioner
              val targetPara = 1
              val partVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(tpeA),
                Seq(Seq(core.Lit(targetPara)))
              )
              val partRefDef = valRefAndDef(owner, "partitioner", partVDrhs)

              val tpeOut = api.Type.apply(getTpe[Group[tpeK.type, tpeB.type]], Seq(tpeK, tpeB))
              // typeinfo OUT
              val typeInfoOUTRefDef = getTypeInfoForTypeRefDef(
                owner,
                tpeOut)

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(owner, tpeOut, typeInfoOUTRefDef._1)

              // LabyNode
              val labyNodeRefDef = getLabyNodeRefDef(
                owner,
                (tpeA, tpeOut),
                "foldGroup",
                bagOpRefDef._1,
                bbIdMap(owner.name.toString),
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // def insideblock
              val insideBlock = dbReplSym.owner == owner

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef, insideBlock)

              // setParallelism
              val setParallelismRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, setParallelismRefDefSym._2)
              replacements += (lhs -> setParallelismRefDefSym._3)
              skip(dc)

            case dc @ core.DefCall(_, DB$.fromDatabagWriteCSV, Seq(dbTpe),
            Seq(Seq(core.ValRef(dbSym), core.ValRef(pathSym), core.ValRef(csvSym)))) if prePrint(dc)=>

              val dbReplSym = replacements(dbSym)
              val pathReplSym = replacements(pathSym)
              val csvReplSym = replacements(csvSym)
              val dbReplRef = core.ValRef(dbReplSym)
              val pathReplRef = core.ValRef(pathReplSym)
              val csvReplRef = core.ValRef(csvReplSym)

              val csvTpe = csvSym.info.typeArgs.head
              val pathTpe = pathSym.info.typeArgs.head

              // ========== db to Left ========= //
              // bagoperator
              val parSymLhs = api.ParSym(owner, api.TermName.fresh("t"), dbTpe)
              val parRefLhs = core.ParRef(parSymLhs)
              val lbdaCallLhs = core.DefCall(Some(Left$.ref), Left$.apply,
                Seq(dbTpe, getTpe[scala.Nothing]), Seq(Seq(parRefLhs)))
              val lbdaCAllLhsRefDef = valRefAndDef(owner, "lambda", lbdaCallLhs)
              val lbdaBodyLhs = core.Let(Seq(lbdaCAllLhsRefDef._2), Seq(), lbdaCAllLhsRefDef._1)
              val mapLambdaLhs = core.Lambda(Seq(parSymLhs), lbdaBodyLhs)
              val mapLambdaLhsRefDef = valRefAndDef(owner, "lbdaLeft", mapLambdaLhs)

              val bagOpLhsVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(dbTpe, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))),
                Seq(Seq(mapLambdaLhsRefDef._1))
              )
              val bagOpMapLhsRefDef = valRefAndDef(owner, "mapToLeftOp", bagOpLhsVDrhs)

              // partitioner
              val targetParaLhs = 1
              val partLhsVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(dbTpe),
                Seq(Seq(core.Lit(targetParaLhs)))
              )
              val partMapLhsRefDef = valRefAndDef(owner, "partitioner", partLhsVDrhs)

              // typeinfo OUT
              val typeInfoMapLhsOUTRefDef =
                getTypeInfoForTypeRefDef(owner, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe)))

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoMapLhsRefDef =
                getElementOrEventTypeInfoRefDef(
                  owner,
                  api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe)),
                  typeInfoMapLhsOUTRefDef._1)

              // LabyNode
              val labyNodeMapLhsRefDef = getLabyNodeRefDef(
                owner,
                (dbTpe, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))),
                "map",
                bagOpMapLhsRefDef._1,
                bbIdMap(owner.name.toString),
                partMapLhsRefDef._1,
                elementOrEventTypeInfoMapLhsRefDef._1
              )

              // def insideblock
              val insideBlockDb = dbReplSym.owner == owner

              // addInput
              val addInputMapLhsRefDef = getAddInputRefDef(owner, labyNodeMapLhsRefDef._1, dbReplRef, insideBlockDb)

              // setParallelism
              val SetParallelismMapLhsRefDefSym = getSetParallelismRefDefSym(owner, addInputMapLhsRefDef._1, 1)

              // ========== csv to Right ========== //
              // bagoperator
              val parSymRhs = api.ParSym(owner, api.TermName.fresh("t"), csvTpe)
              val parRefRhs = core.ParRef(parSymRhs)
              val lbdaCallRhs = core.DefCall(Some(Right$.ref), Right$.apply,
                Seq(getTpe[scala.Nothing], csvTpe), Seq(Seq(parRefRhs)))
              val lbdaCAllRhsRefDef = valRefAndDef(owner, "lambda", lbdaCallRhs)
              val lbdaBodyRhs = core.Let(Seq(lbdaCAllRhsRefDef._2), Seq(), lbdaCAllRhsRefDef._1)
              val mapLambdaRhs = core.Lambda(Seq(parSymRhs), lbdaBodyRhs)
              val mapLambdaRhsRefDef = valRefAndDef(owner, "lbdaRight", mapLambdaRhs)

              val bagOpRhsVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.map,
                Seq(csvTpe, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))),
                Seq(Seq(mapLambdaRhsRefDef._1))
              )
              val bagOpMapRhsRefDef = valRefAndDef(owner, "mapToRightOp", bagOpRhsVDrhs)

              // partitioner
              val targetParaRhs = 1
              val partRhsVDrhs = core.Inst(
                getTpe[Always0[Any]],
                Seq(csvTpe),
                Seq(Seq(core.Lit(targetParaRhs)))
              )
              val partMapRhsRefDef = valRefAndDef(owner, "partitioner", partRhsVDrhs)

              // typeinfo OUT
              val typeInfoMapRhsOUTRefDef =
                getTypeInfoForTypeRefDef(owner, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe)))

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoMapRhsRefDef = getElementOrEventTypeInfoRefDef(
                owner,
                api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe)),
                typeInfoMapRhsOUTRefDef._1
              )

              // LabyNode
              val labyNodeMapRhsRefDef = getLabyNodeRefDef(
                owner,
                (csvTpe, api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))),
                "map",
                bagOpMapRhsRefDef._1,
                bbIdMap(owner.name.toString),
                partMapRhsRefDef._1,
                elementOrEventTypeInfoMapRhsRefDef._1
              )

              // def insideblock
              val insideBlockCsv = csvReplSym.owner == owner

              // addInput
              val addInputMapRhsRefDef = getAddInputRefDef(owner, labyNodeMapRhsRefDef._1, csvReplRef, insideBlockCsv)

              // setParallelism
              val SetParallelismMapRhsRefDefSym = getSetParallelismRefDefSym(owner, addInputMapRhsRefDef._1, 1)

              // ========== toCsvString ========== //
              val bagOpToCsvStringVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.toCsvString,
                Seq(dbTpe),
                Seq()
              )
              val bagOpToCsvStringRefDef = valRefAndDef(owner, "toCsvString", bagOpToCsvStringVDrhs)

              // partitioner
              val targetParaToCsvString = 1
              val partVDToCsvString = core.Inst(
                getTpe[Always0[Any]],
                Seq(api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe))),
                Seq(Seq(core.Lit(targetParaToCsvString)))
              )
              val partToCsvStringRefDef = valRefAndDef(owner, "partitioner", partVDToCsvString)

              // typeinfo OUT
              val typeInfoMapToCsvStringOUTRefDef = getTypeInfoForTypeRefDef(
                owner,
                pathTpe // String
              )

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoToCsvStringRefDef =
                getElementOrEventTypeInfoRefDef(
                  owner,
                  pathTpe,
                  typeInfoMapToCsvStringOUTRefDef._1)

              // LabyNode
              val labyNodeToCsvStringRefDef = getLabyNodeRefDef(
                owner,
                (api.Type.apply(getTpe[scala.util.Either[Any,Any]], Seq(dbTpe, csvTpe)), pathTpe),
                "toCsvString",
                bagOpToCsvStringRefDef._1,
                bbIdMap(owner.name.toString),
                partToCsvStringRefDef._1,
                elementOrEventTypeInfoToCsvStringRefDef._1
              )

              // def insideblock
              val insideBlockToCsvString1 = SetParallelismMapLhsRefDefSym._3.owner == owner
              val insideBlockToCsvString2 = SetParallelismMapRhsRefDefSym._3.owner == owner

              // addInput
              val addInputToCsvStringRefDef1 =
                getAddInputRefDef(owner, labyNodeToCsvStringRefDef._1, SetParallelismMapLhsRefDefSym._1)
              val addInputToCsvStringRefDef2 =
                getAddInputRefDef(owner, addInputToCsvStringRefDef1._1, SetParallelismMapRhsRefDefSym._1)

              // setParallelism
              val setParallelismToCsvStringRefDefSym = getSetParallelismRefDefSym(owner, addInputToCsvStringRefDef2._1, 1)

              // ========== stringSink ========== //
              // bagoperator

              val bagOpWriteStringVDrhs = core.DefCall(
                Some(ScalaOps$.ref),
                ScalaOps$.writeString,
                Seq(),
                Seq()
              )
              val bagOpWriteStringRefDef = valRefAndDef(owner, "writeString", bagOpWriteStringVDrhs)

              // partitioner
              val targetParaWriteString = 1
              val partVDWriteString = core.Inst(
                getTpe[Always0[Any]],
                Seq(pathTpe),
                Seq(Seq(core.Lit(targetParaWriteString)))
              )
              val partWriteStringRefDef = valRefAndDef(owner, "partitioner", partVDWriteString)

              // typeinfo OUT
              val typeInfoMapWriteStringOUTRefDef = getTypeInfoForTypeRefDef(
                owner,
                u.typeOf[Unit]
              )

              // ElementOrEventTypeInfo
              val elementOrEventTypeInfoWriteStringRefDef =
                getElementOrEventTypeInfoRefDef(
                  owner,
                  u.typeOf[Unit],
                  typeInfoMapWriteStringOUTRefDef._1)

              // LabyNode
              val labyNodeWriteStringRefDef = getLabyNodeRefDef(
                owner,
                (pathTpe, u.typeOf[Unit]),
                "stringFileSink",
                bagOpWriteStringRefDef._1,
                bbIdMap(owner.name.toString),
                partWriteStringRefDef._1,
                elementOrEventTypeInfoWriteStringRefDef._1
              )

              // def insideblock
              val insideBlockSinkPath = pathReplSym.owner == owner
              val insideBlockSinkCsv = setParallelismToCsvStringRefDefSym._3.owner == owner

              // addInput
              val addInputWriteStringRefDef1 =
                getAddInputRefDef(owner, labyNodeWriteStringRefDef._1, pathReplRef)
              val addInputWriteStringRefDef2 =
                getAddInputRefDef(owner, addInputWriteStringRefDef1._1, setParallelismToCsvStringRefDefSym._1)

              // setParallelism
              val setParallelismWriteStringRefDefSym =
                getSetParallelismRefDefSym(owner, addInputWriteStringRefDef2._1, 1)

              // put everything into a block
              valDefsFinal = valDefsFinal ++
                Seq(
                  // data to Left()
                  mapLambdaLhsRefDef._2, bagOpMapLhsRefDef._2, partMapLhsRefDef._2, typeInfoMapLhsOUTRefDef._2,
                  elementOrEventTypeInfoMapLhsRefDef._2, labyNodeMapLhsRefDef._2, addInputMapLhsRefDef._2,
                  SetParallelismMapLhsRefDefSym._2,
                  // csv to Right()
                  mapLambdaRhsRefDef._2, bagOpMapRhsRefDef._2, partMapRhsRefDef._2, typeInfoMapRhsOUTRefDef._2,
                  elementOrEventTypeInfoMapRhsRefDef._2, labyNodeMapRhsRefDef._2, addInputMapRhsRefDef._2,
                  SetParallelismMapRhsRefDefSym._2,
                  // to csvString
                  bagOpToCsvStringRefDef._2, partToCsvStringRefDef._2, typeInfoMapToCsvStringOUTRefDef._2,
                  elementOrEventTypeInfoToCsvStringRefDef._2, labyNodeToCsvStringRefDef._2,
                  addInputToCsvStringRefDef1._2, addInputToCsvStringRefDef2._2, setParallelismToCsvStringRefDefSym._2,
                  // StringSink
                  bagOpWriteStringRefDef._2, partWriteStringRefDef._2, typeInfoMapWriteStringOUTRefDef._2,
                  elementOrEventTypeInfoWriteStringRefDef._2, labyNodeWriteStringRefDef._2,
                  addInputWriteStringRefDef1._2, addInputWriteStringRefDef2._2,
                  setParallelismWriteStringRefDefSym._2
                )
              replacements += (lhs -> setParallelismWriteStringRefDefSym._3)
              skip(dc)
          }
        }

        case Attr.inh(dd @ core.DefDef(_, _, pars, core.Let(_,_,_)), owner::_) =>

          val bbid = bbIdMap(dd.symbol.name.toString)

          // create a phinode for every parameter
          val phiDefs = pars.map(
            s => s.flatMap{
              case pd @ core.ParDef(sym, _) =>

                val tpeOut = sym.info.typeArgs.head

                // phinode name
                val phinodeName = sym.name + "Phi"
                val nmLit = core.Lit(phinodeName)

                // phinode bbid
                val bbidLit = core.Lit(bbid)

                // partitioner
                val targetPara = 1
                val part = core.Inst(
                  getTpe[Always0[Any]],
                  Seq(tpeOut),
                  Seq(Seq(core.Lit(targetPara)))
                )
                val partPhiRefDef = valRefAndDef(owner, "partitioner", part)

                // phinode inSer
                val inserLit = core.Lit(null)

                // typeinfo OUT
                val typeInfoRefDef = getTypeInfoForTypeRefDef(
                  owner,
                  tpeOut
                )
                // ElementOrEventTypeInfo
                val elementOrEventTypeInfoRefDef = getElementOrEventTypeInfoRefDef(
                  owner,
                  tpeOut,
                  typeInfoRefDef._1
                )

                val phiDC = core.DefCall(
                  Some(LabyStatics$.ref),
                  LabyStatics$.phi,
                  Seq(tpeOut),
                  Seq(Seq(nmLit, bbidLit, partPhiRefDef._1, inserLit, elementOrEventTypeInfoRefDef._1))
                )
                val phiSym = newValSym(dd.symbol, phinodeName, phiDC)
                val phiDCRefDef = valRefAndDef(phiSym, phiDC)

                // save mapping from ParRef to PhiNode for later addInput
                replacements += (sym -> phiSym)
                symToPhiRef += (sym -> phiDCRefDef._1)
                defSymNameToPhiRef += (dd.symbol.name.toString -> phiDCRefDef._1)

                // prepend valdefs to body letblock and return letblock
                Seq(partPhiRefDef._2, typeInfoRefDef._2, elementOrEventTypeInfoRefDef._2, phiDCRefDef._2)
            })

          // prepend new defdefs to old defdefs
          var nDefs = Seq[u.ValDef]()
          phiDefs.foreach(s => nDefs = nDefs ++ s)
          valDefsFinal = valDefsFinal ++ nDefs

        // create condNode when encountering if statements
        case Attr.inh(cnd @ core.Branch(cond @ core.ValRef(condSym),
        thn @ core.DefCall(_, thnSym, _, _),
        els @ core.DefCall(_, elsSym, _, _)
        ), owner::_) =>
          println(cond, condSym, thn, els)

          val condReplSym = replacements(condSym)
          val condReplRef = core.ValRef(condReplSym)

          val thnIds = bbIdShortcut(bbIdMap(thnSym.name.toString))
          val elsIds = bbIdShortcut(bbIdMap(elsSym.name.toString))
          val thnIdsDC = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(u.typeOf[Int]), Seq(thnIds.map(core.Lit(_))))
          val elsIdsDC = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(u.typeOf[Int]), Seq(elsIds.map(core.Lit(_))))
          val thnIdsDCRefDef = valRefAndDef(owner, "seq", thnIdsDC)
          val elsIdsDCRefDef = valRefAndDef(owner, "seq", elsIdsDC)
          val condOp = core.DefCall(Some(ScalaOps$.ref), ScalaOps$.condNode, Seq(),
            Seq(Seq(
              thnIdsDCRefDef._1,
              elsIdsDCRefDef._1
            )))
          val condOpRefDef = valRefAndDef(owner, "condOp", condOp)

          val tpeIn = u.typeOf[scala.Boolean]
          val tpeOut = getTpe[labyrinth.util.Unit]

          // partitioner
          val targetParaLhs = 1
          val part = core.Inst(
            getTpe[Always0[Any]],
            Seq(tpeIn),
            Seq(Seq(core.Lit(targetParaLhs)))
          )
          val partRefDef = valRefAndDef(owner, "partitioner", part)

          // typeinfo OUT
          val typeInfoOUTRefDef =
            getTypeInfoForTypeRefDef(owner, tpeOut)

          // ElementOrEventTypeInfo
          val elementOrEventTypeInfoRefDef =
            getElementOrEventTypeInfoRefDef(
              owner,
              tpeOut,
              typeInfoOUTRefDef._1)

          // LabyNode
          val labyNodeRefDef = getLabyNodeRefDef(
            owner,
            (tpeIn, tpeOut),
            "condNode",
            condOpRefDef._1,
            bbIdMap(owner.name.toString),
            partRefDef._1,
            elementOrEventTypeInfoRefDef._1
          )

          // def insideblock
          val insideBlock = condReplSym.owner == owner

          // addInput
          val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, condReplRef)

          // setParallelism
          val setParallelismMapLhsRefDefSym = getSetParallelismRefDefSym(owner, addInputRefDef._1, 1)

          // put everything into block
          valDefsFinal = valDefsFinal ++
            Seq(thnIdsDCRefDef._2, elsIdsDCRefDef._2, condOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
              elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, setParallelismMapLhsRefDefSym._2)

        // add inputs to phi nodes when encountering defcalls
        case Attr.inh(dc @ core.DefCall(tgt, methSym, targs, args), owner :: _)
          if !meta(dc).all.all.contains(SkipTraversal) && !isFun(owner) && /*TODO verify this:*/ !isFun(owner.owner) &&
        !(args.size == 1 && args.head.isEmpty) /*skip if no arguments*/ && args.nonEmpty && !isAlg(dc) =>

          val insideBlock = false

          var addInputRefs = Seq[u.Ident]()
          val addInputDefs = args.flatMap(
            s => s.map{
              case core.ParRef(sym) =>
                val phiRef = defSymNameToPhiRef(dc.symbol.name.toString)
                // TODO insideblock
                val addInp = getAddInputRefDef(owner, phiRef, core.ParRef(replacements(sym)), insideBlock)
                addInputRefs = addInputRefs :+ addInp._1
                addInp._2
              case core.ValRef(sym) =>
                val phiRef = defSymNameToPhiRef(dc.symbol.name.toString)
                // TODO insideblock
                val addInp = getAddInputRefDef(owner, phiRef, core.ValRef(replacements(sym)), insideBlock)
                addInputRefs = addInputRefs :+ addInp._1
                addInp._2
            }
          )

          valDefsFinal = valDefsFinal ++ addInputDefs

      }._tree(tree)

    val flatTrans = core.Let(valDefsFinal)

    // add Labyrinth statics
    val labyStaticsTrans = flatTrans match {
      case core.Let(valdefs, _, _) => {
        println
        // assert(valdefs.nonEmpty, "Programm should have valdefs!")
        // val owner = valdefs.head.symbol.owner
        val owner = enclosingOwner

        // terminal basic block id: find block which has no outgoing edges
        val terminalSet = bbChildren.filter(!bbParents.contains(_))
        val terminalBbid = if (bbParents.isEmpty) bbIdMap(enclosingOwner.name.toString) else bbIdMap(terminalSet.head)

        // before code
        val custSerDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.registerCustomSerializer, Seq(), Seq())
        val customSerDCRefDef = valRefAndDef(owner, "registerCustomSerializer", custSerDC)

        val termIdDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.setTerminalBbid, Seq(),
          Seq(Seq(core.Lit(terminalBbid))))
        val termIdDCRefDef = valRefAndDef(owner, "terminalBbId", termIdDC)

        // kickoff blocks: start with enclosingOwner and add child of each block that has only one child
        val startBbIds = bbIdShortcut(bbIdMap(enclosingOwner.name.toString))
        val startingBasicBlocks = startBbIds.map(core.Lit(_))
        val kickOffWorldCup2018SourceDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.setKickoffSource, Seq(),
          Seq(startingBasicBlocks))
        val kickOffWorldCup2018SourceDCRefDef = valRefAndDef(owner, "kickOffSource", kickOffWorldCup2018SourceDC)

        // after code
        val transAllDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.translateAll, Seq(), Seq())
        val transAllDCRefDef = valRefAndDef(owner, "translateAll", transAllDC)

        val envImplDC = core.DefCall(
          Some(core.Ref(api.Sym.predef)),
          api.Sym.implicitly,
          Seq(StreamExecutionEnvironment$.tpe),
          Seq()
        )
        val envImplDCRefDef = valRefAndDef(owner, "implEnv", envImplDC)

        val execDC = core.DefCall(Some(envImplDCRefDef._1), StreamExecutionEnvironment$.execute, Seq(), Seq(Seq()))
        val execDCRefDef = valRefAndDef(owner, "envExecute", execDC)

        val newVals = Seq(customSerDCRefDef._2, termIdDCRefDef._2, kickOffWorldCup2018SourceDCRefDef._2) ++
          valdefs ++
          Seq(transAllDCRefDef._2, envImplDCRefDef._2, execDCRefDef._2)

        core.Let(newVals, Seq(), execDCRefDef._1)
      }
    }

    println("+++++++++++++++++++++++++++++++++++++++++++++++")
    postPrint(labyStaticsTrans)
    labyStaticsTrans
  })

  def getTypeInfoForTypeRefDef(owner: u.Symbol, tpe: u.Type): (u.Ident, u.ValDef) = {
    val typeInfoOUTVDrhs = core.DefCall(
      Option(Memo$.ref),
      Memo$.typeInfoForType,
      Seq(tpe),
      Seq()
    )
    valRefAndDef(owner, "typeInfo", typeInfoOUTVDrhs)}

  def getElementOrEventTypeInfoRefDef(owner: u.Symbol, tpe: u.Type, typeInfo: u.Ident): (u.Ident, u.ValDef) = {
    val eleveVDrhs = core.Inst(
      ElementOrEventAPI.tpe,
      Seq(tpe),
      Seq(Seq(typeInfo))
    )
    valRefAndDef(owner, "elementOrEventTypeInfo", eleveVDrhs)
  }

  def getLabyNodeRefDef(
    owner: u.Symbol,
    tpeInOut: (u.Type, u.Type),
    name: String,
    bagOpRef: u.Ident,
    bbId: Int,
    partRef: u.Ident,
    elementOrEventTypeInfoRef: u.Ident
  ): (u.Ident, u.ValDef) = {
    val labyNodeVDrhs = core.Inst(
      LabyNodeAPI.tpe,
      Seq(tpeInOut._1, tpeInOut._2),
      Seq(Seq(
        core.Lit(name),           // name
        bagOpRef,                 // bagoperator
        core.Lit(bbId),           // bbId
        partRef,                  // partitioner
        core.Lit(null),           // inputSerializer
        elementOrEventTypeInfoRef // elemTypeInfo
      ))
    )
    valRefAndDef(owner, "labyNode", labyNodeVDrhs)
  }

  def getAddInputRefDef(
    owner: u.Symbol,
    tgtRef: u.Ident,
    singSrcDBReplRef: u.Ident,
    insideBlock: Boolean = true
  ) : (u.Ident, u.ValDef) = {
    val condOut = !insideBlock
    val addInputVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.addInput,
      Seq(),
      Seq(Seq(singSrcDBReplRef, core.Lit(insideBlock), core.Lit(condOut)))
    )
    valRefAndDef(owner, "addInput", addInputVDrhs)
  }

  def getSetParallelismRefDefSym(owner: u.Symbol, tgtRef: u.Ident, parallelism: Int):
  (u.Ident, u.ValDef, u.TermSymbol) = {
    val setParVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.setParallelism,
      Seq(),
      Seq(Seq(core.Lit(parallelism)))
    )
    val prlSym = newValSym(owner, "setPrllzm", setParVDrhs)
    val refDef = valRefAndDef(prlSym, setParVDrhs)
    (refDef._1, refDef._2, prlSym)
  }

  object ScalaOps$ extends ModuleAPI {
    lazy val sym = api.Sym[ScalaOps.type].asModule

    val condNode = op("condNode")
    val cross = op("cross")
    val flatMapDataBagHelper = op("flatMapDataBagHelper")
    val fold = op("fold")
    val foldAlgHelper = op("foldAlgHelper")
    val foldGroupAlgHelper = op("foldGroupAlgHelper")
    val fromNothing = op("fromNothing")
    val fromSingSrcApply = op("fromSingSrcApply")
    val map = op("map")
    val reduce = op("reduce")
    val textReader = op("textReader")
    val textSource = op("textSource")
    val toCsvString = op("toCsvString")
    val writeString = op("writeString")


    override def ops = Set()
  }

  //  object FlinkScala$ extends ModuleAPI {
  //    lazy val sym = api.Sym[org.apache.flink.api.scala.`package`.type].asModule
  //
  //    val createTypeInformation = op("createTypeInformation")
  //
  //    override def ops = Set()
  //  }

  object Memo$ extends ModuleAPI {
    lazy val sym = api.Sym[Memo.type].asModule

    val typeInfoForType = op("typeInfoForType")

    override def ops = Set()

  }

  object Left$ extends ModuleAPI {
    lazy val sym = api.Sym[scala.util.Left[Any, Any]].asClass.companion.asModule

    val apply = op("apply")

    override def ops = Set()
  }

  object Right$ extends ModuleAPI {
    lazy val sym = api.Sym[scala.util.Right[Any, Any]].asClass.companion.asModule

    val apply = op("apply")

    override def ops = Set()
  }

  object StreamExecutionEnvironment$ extends ClassAPI {
    lazy val sym = api.Sym[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment].asClass

    val execute = op("execute", List(0))

    override def ops = Set()
  }

  object Tuple2API extends ClassAPI {
    lazy val sym = api.Sym[org.apache.flink.api.java.tuple.Tuple2[Any, Any]].asClass

    override def ops = Set()

  }

  object TypeInformationAPI extends ClassAPI {
    lazy val sym = api.Sym[org.apache.flink.api.common.typeinfo.TypeInformation[Any]].asClass

    val createSerializer = op("createSerializer")

    override def ops = Set()
  }

  object ElementOrEventAPI extends ClassAPI {
    lazy val sym = api.Sym[labyrinth.ElementOrEventTypeInfo[Any]].asClass
    override def ops = Set()
  }

  object LabyStatics$ extends ModuleAPI {
    lazy val sym = api.Sym[org.emmalanguage.labyrinth.operators.LabyStatics.type ].asModule

    val phi = op("phi")
    val registerCustomSerializer = op("registerCustomSerializer")
    val setKickoffSource = op("setKickoffSource")
    val setTerminalBbid = op("setTerminalBbid")
    val translateAll = op("translateAll")

    override def ops = Set()

  }

  object LabyNodeAPI extends ClassAPI {
    lazy val sym = api.Sym[labyrinth.LabyNode[Any,Any]].asClass

    val setParallelism = op("setParallelism")
    val addInput = op("addInput", List(3))
    override def ops = Set()
  }

  def getTpe[T: u.WeakTypeTag] : u.Type = api.Sym[T].asClass.toTypeConstructor.widen
}


// ======================================================================================= \\
// ======================================================================================= \\
// all of this copied and adjusted from FlinkDataSet.scala to avoid anonymous class errors \\
// ======================================================================================= \\
// ======================================================================================= \\

object Memo {
  private val memo = collection.mutable.Map.empty[Any, Any]

  { // initialize memo table with standard types
    import org.apache.flink.api.scala._
    // standard Scala types
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Unit]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Boolean]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Char]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Byte]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Short]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Int]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Long]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Float]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Double]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[String]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[BigInt]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[BigDecimal]], createTypeInformation)
    // standard Java types
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Void]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Boolean]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Character]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Byte]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Short]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Integer]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Long]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Float]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.lang.Double]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.math.BigInteger]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[java.math.BigDecimal]], createTypeInformation)
    // Used Types (should probably be removed later)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[labyrinth.util.Nothing]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Seq[Int]]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Seq[String]]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[org.apache.flink.api.java.tuple.Tuple2[Int, String]]],
      createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[org.emmalanguage.api.CSV]], createTypeInformation)
    memoizeTypeInfo(
      implicitly[org.emmalanguage.api.Meta[scala.util.Either[(String, Long),org.emmalanguage.io.csv.CSV]]],
      createTypeInformation
    )
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[(String, Long)]], createTypeInformation)
    memoizeTypeInfo(
      implicitly[org.emmalanguage.api.Meta[org.emmalanguage.api.Group[String,Long]]],
      createTypeInformation
    )
    memoizeTypeInfo(
      implicitly[org.emmalanguage.api.Meta[
        org.emmalanguage.labyrinth.operators.InputFormatWithInputSplit[String,org.apache.flink.core.fs.FileInputSplit]]
        ],
      createTypeInformation
    )
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[labyrinth.util.Unit]], createTypeInformation)
  }

  def memoizeTypeInfo[T](implicit meta: org.emmalanguage.api.Meta[T], info: TypeInformation[T])
  : TypeInformation[T] = {
    val tpe = fix(meta.tpe).toString
    val res = memo.getOrElseUpdate(tpe, info)
    res.asInstanceOf[TypeInformation[T]]
  }

  implicit def typeInfoForType[T](implicit meta: org.emmalanguage.api.Meta[T]): TypeInformation[T] = {
    val tpe = fix(meta.tpe).toString
    if (memo.contains(tpe)) memo(tpe).asInstanceOf[TypeInformation[T]]
    else throw new RuntimeException(
      s"""
        |Cannot find TypeInformation for type $tpe.
        |Try calling `FlinkDataSet.memoizeTypeInfo[$tpe]` explicitly before the `emma.onFlink` quote.
      """.stripMargin.trim
    )
  }

  private def fix(tpe: scala.reflect.runtime.universe.Type): scala.reflect.runtime.universe.Type =
    tpe.dealias.map(t => {
      if (t =:= scala.reflect.runtime.universe.typeOf[java.lang.String]) scala.reflect.runtime.universe.typeOf[String]
      else t
    })
}