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

  val labyrinthLabynize = TreeTransform("labyrinthLabynize", (tree: u.Tree) => {
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
    val defs = scala.collection.mutable.Map[u.TermSymbol, u.ValDef]()

    println("___")
    println("XXXXXXXXXXXXXXXXXXXXXXXX1")
    println("==0tree Labynization==")
    println(tree)
    println("XXXXXXXXXXXXXXXXXXXXXXXX1")

    // first traversal does the labyrinth labynization. second for block type correction.
    val trans1 = api.TopDown.unsafe
      .withOwner
      .transformWith {
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
                1,
                partSplitsRefDef._1,
                elementOrEventTypeInfoSplitsRefDef._1
              )

              // addInput
              val addInputSplitsRefDef = getAddInputRefDef(owner, labyNodeSplitsRefDef._1, dbPathSymReplRef)

              // setParallelism
              val SetParallelismSplitsRefDef = getSetParallelismRefDef(owner, addInputSplitsRefDef._1, 1)


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
                1,
                partReadRefDef._1,
                elementOrEventTypeInfoReadRefDef._1
              )

              // addInput
              val addInputReadRefDef = getAddInputRefDef(owner, labyNodeReadRefDef._1, SetParallelismSplitsRefDef._1)

              // setParallelism
              val SetParallelismReadRefDef = getSetParallelismRefDef(owner, addInputReadRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpSplitsRefDef._2, partSplitsRefDef._2, typeInfoOUTSplitsRefDef._2,
                  elementOrEventTypeInfoSplitsRefDef._2, labyNodeSplitsRefDef._2, addInputSplitsRefDef._2,
                  SetParallelismSplitsRefDef._2,
                  bagOpReadRefDef._2, partReadRefDef._2, typeInfoOUTReadRefDef._2,
                  elementOrEventTypeInfoReadRefDef._2, labyNodeReadRefDef._2, addInputReadRefDef._2,
                  SetParallelismReadRefDef._2),
                Seq(),
                SetParallelismReadRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, labyNodeRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, singSrcDBReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, tgtReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2, elementOrEventTypeInfoRefDef._2,
                  labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partMapLhsRefDef._1,
                elementOrEventTypeInfoMapLhsRefDef._1
              )

              // addInput
              val addInputMapLhsRefDef = getAddInputRefDef(owner, labyNodeMapLhsRefDef._1, lhsReplRef)

              // setParallelism
              val SetParallelismMapLhsRefDef = getSetParallelismRefDef(owner, addInputMapLhsRefDef._1, 1)


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
                1,
                partMapRhsRefDef._1,
                elementOrEventTypeInfoMapRhsRefDef._1
              )

              // addInput
              val addInputMapRhsRefDef = getAddInputRefDef(owner, labyNodeMapRhsRefDef._1, rhsReplRef)

              // setParallelism
              val SetParallelismMapRhsRefDef = getSetParallelismRefDef(owner, addInputMapRhsRefDef._1, 1)


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
                1,
                partCrossRefDef._1,
                elementOrEventTypeInfoCrossRefDef._1
              )

              // addInput
              val addInputCrossRefDef1 =
                getAddInputRefDef(owner, labyNodeCrossRefDef._1, SetParallelismMapLhsRefDef._1)
              val addInputCrossRefDef2 =
                getAddInputRefDef(owner, addInputCrossRefDef1._1, SetParallelismMapRhsRefDef._1)

              // setParallelism
              val SetParallelismCrossRefDef = getSetParallelismRefDef(owner, addInputCrossRefDef2._1, 1)


              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(mapLambdaLhsRefDef._2, bagOpMapLhsRefDef._2, partMapLhsRefDef._2, typeInfoMapLhsOUTRefDef._2,
                  elementOrEventTypeInfoMapLhsRefDef._2, labyNodeMapLhsRefDef._2, addInputMapLhsRefDef._2,
                  SetParallelismMapLhsRefDef._2,
                  mapLambdaRhsRefDef._2, bagOpMapRhsRefDef._2, partMapRhsRefDef._2, typeInfoMapRhsOUTRefDef._2,
                  elementOrEventTypeInfoMapRhsRefDef._2, labyNodeMapRhsRefDef._2, addInputMapRhsRefDef._2,
                  SetParallelismMapRhsRefDef._2,
                  bagOpCrossRefDef._2, partCrossRefDef._2, typeInfoMapCrossOUTRefDef._2,
                  elementOrEventTypeInfoCrossRefDef._2, labyNodeCrossRefDef._2, addInputCrossRefDef1._2,
                  addInputCrossRefDef2._2, SetParallelismCrossRefDef._2),
                Seq(),
                SetParallelismCrossRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2


            case dc @ core.DefCall
              (_, Ops.foldGroup, Seq(tpeA, tpeB, tpeK),
              Seq(Seq(core.ValRef(dbSym), extrRef @ core.ValRef(_), alg))) if prePrint(dc) =>

              val dbSymRepl = replacements(dbSym)
              val dbReplRef = core.ValRef(dbSymRepl)

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
                1,
                partRefDef._1,
                elementOrEventTypeInfoRefDef._1
              )

              // addInput
              val addInputRefDef = getAddInputRefDef(owner, labyNodeRefDef._1, dbReplRef)

              // setParallelism
              val SetParallelismRefDef = getSetParallelismRefDef(owner, addInputRefDef._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoOUTRefDef._2,
                  elementOrEventTypeInfoRefDef._2, labyNodeRefDef._2, addInputRefDef._2, SetParallelismRefDef._2),
                Seq(),
                SetParallelismRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

            case dc @ core.DefCall(_, DB$.fromDatabagWriteCSV, Seq(dbTpe),
            Seq(Seq(core.ValRef(dbSym), core.ValRef(pathSym), core.ValRef(csvSym)))) if prePrint(dc)=>

              val dbSymRepl = replacements(dbSym)
              val pathSymRepl = replacements(pathSym)
              val csvSymRepl = replacements(csvSym)
              val dbReplRef = core.ValRef(dbSymRepl)
              val pathReplRef = core.ValRef(pathSymRepl)
              val csvReplRef = core.ValRef(csvSymRepl)

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
                1,
                partMapLhsRefDef._1,
                elementOrEventTypeInfoMapLhsRefDef._1
              )

              // addInput
              val addInputMapLhsRefDef = getAddInputRefDef(owner, labyNodeMapLhsRefDef._1, dbReplRef)

              // setParallelism
              val SetParallelismMapLhsRefDef = getSetParallelismRefDef(owner, addInputMapLhsRefDef._1, 1)

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
                1,
                partMapRhsRefDef._1,
                elementOrEventTypeInfoMapRhsRefDef._1
              )

              // addInput
              val addInputMapRhsRefDef = getAddInputRefDef(owner, labyNodeMapRhsRefDef._1, csvReplRef)

              // setParallelism
              val SetParallelismMapRhsRefDef = getSetParallelismRefDef(owner, addInputMapRhsRefDef._1, 1)

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
                1,
                partToCsvStringRefDef._1,
                elementOrEventTypeInfoToCsvStringRefDef._1
              )

              // addInput
              val addInputToCsvStringRefDef1 =
                getAddInputRefDef(owner, labyNodeToCsvStringRefDef._1, SetParallelismMapLhsRefDef._1)
              val addInputToCsvStringRefDef2 =
                getAddInputRefDef(owner, addInputToCsvStringRefDef1._1, SetParallelismMapRhsRefDef._1)

              // setParallelism
              val setParallelismToCsvStringRefDef = getSetParallelismRefDef(owner, addInputToCsvStringRefDef2._1, 1)

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
                1,
                partWriteStringRefDef._1,
                elementOrEventTypeInfoWriteStringRefDef._1
              )

              // addInput
              val addInputWriteStringRefDef1 =
                getAddInputRefDef(owner, labyNodeWriteStringRefDef._1, pathReplRef)
              val addInputWriteStringRefDef2 =
                getAddInputRefDef(owner, addInputWriteStringRefDef1._1, setParallelismToCsvStringRefDef._1)

              // setParallelism
              val SetParallelismWriteStringRefDef = getSetParallelismRefDef(owner, addInputWriteStringRefDef2._1, 1)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(
                  // data to Left()
                  mapLambdaLhsRefDef._2, bagOpMapLhsRefDef._2, partMapLhsRefDef._2, typeInfoMapLhsOUTRefDef._2,
                  elementOrEventTypeInfoMapLhsRefDef._2, labyNodeMapLhsRefDef._2, addInputMapLhsRefDef._2,
                  SetParallelismMapLhsRefDef._2,
                  // csv to Right()
                  mapLambdaRhsRefDef._2, bagOpMapRhsRefDef._2, partMapRhsRefDef._2, typeInfoMapRhsOUTRefDef._2,
                  elementOrEventTypeInfoMapRhsRefDef._2, labyNodeMapRhsRefDef._2, addInputMapRhsRefDef._2,
                  SetParallelismMapRhsRefDef._2,
                  // to csvString
                  bagOpToCsvStringRefDef._2, partToCsvStringRefDef._2, typeInfoMapToCsvStringOUTRefDef._2,
                  elementOrEventTypeInfoToCsvStringRefDef._2, labyNodeToCsvStringRefDef._2,
                  addInputToCsvStringRefDef1._2, addInputToCsvStringRefDef2._2, setParallelismToCsvStringRefDef._2,
                  // StringSink
                  bagOpWriteStringRefDef._2, partWriteStringRefDef._2, typeInfoMapWriteStringOUTRefDef._2,
                  elementOrEventTypeInfoWriteStringRefDef._2, labyNodeWriteStringRefDef._2,
                  addInputWriteStringRefDef1._2, addInputWriteStringRefDef2._2,
                  SetParallelismWriteStringRefDef._2
                ),
                Seq(),
                SetParallelismWriteStringRefDef._1
              )
              val blockSym = newSymbol(owner, "setPrllzm", blockVDrhs)
              val blockRefDef = valRefAndDef(blockSym, blockVDrhs)
              replacements += (lhs -> blockSym)

              //postPrint(blockRefDef._2)

              blockRefDef._2

            case _ => vd
          }
        }

        case Attr.inh(vr @ core.ValRef(sym), _) =>
          if (prePrint(vr) && replacements.keys.toList.contains(sym)) {
            val nvr = core.ValRef(replacements(sym))
            skip(nvr)
            nvr
          } else {
            vr
          }

      }._tree(tree)

    // second traversal to correct block types
    // Background: scala does not change block types if expression type changes
    // (see internal/Trees.scala - Tree.copyAttrs)
    val trans2 = api.TopDown.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(lb @ core.Let(valdefs, defdefs, expr), _) if lb.tpe != expr.tpe =>
          val nlb = core.Let(valdefs, defdefs, expr)
          nlb
      }._tree(trans1)

    // add LabyNode.translateAll() and env.execute
    val fin = trans2 match {
      case core.Let(valdefs, defdefs, _) => {
        assert(valdefs.nonEmpty, "Programm should have valdefs!")
        val owner = valdefs.head.symbol.owner

        val terminalBbid = 1

        // before code
        val custSerDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.registerCustomSerializer, Seq(), Seq())
        val customSerDCRefDef = valRefAndDef(owner, "registerCustomSerializer", custSerDC)

        val termIdDC = core.DefCall(Some(LabyStatics$.ref), LabyStatics$.setTerminalBbid, Seq(),
          Seq(Seq(core.Lit(terminalBbid))))
        val termIdDCRefDef = valRefAndDef(owner, "terminalBbId", termIdDC)

        val startingBasicBlocks = Seq(core.Lit(1))
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

        val execDC = core.DefCall(Some(envImplDCRefDef._1), StreamExecutionEnvironment$.execute, Seq(), Seq() )
        val execDCRefDef = valRefAndDef(owner, "envExecute", execDC)

        val newVals = Seq(customSerDCRefDef._2, termIdDCRefDef._2, kickOffWorldCup2018SourceDCRefDef._2) ++
          valdefs ++
          Seq(transAllDCRefDef._2, envImplDCRefDef._2, execDCRefDef._2)

        core.Let(newVals, defdefs, execDCRefDef._1)
      }
    }

    println("+++++++++++++++++++++++++++++++++++++++++++++++")
    postPrint(fin)
    fin
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
    valRefAndDef(owner, "ElementOrEventTypeInfo", eleveVDrhs)
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
    valRefAndDef(owner, "LabyNode", labyNodeVDrhs)
  }

  def getAddInputRefDef(
    owner: u.Symbol,
    tgtRef: u.Ident,
    singSrcDBReplRef: u.Ident,
    insideBlock: Boolean = true,
    condOut: Boolean = false
  ) : (u.Ident, u.ValDef) = {
    val addInputVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.addInput,
      Seq(),
      Seq(Seq(singSrcDBReplRef, core.Lit(insideBlock), core.Lit(condOut)))
    )
    valRefAndDef(owner, "addInput", addInputVDrhs)
  }

  def getSetParallelismRefDef(owner: u.Symbol, tgtRef: u.Ident, parallelism: Int): (u.Ident, u.ValDef) = {
    val setParVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNodeAPI.setParallelism,
      Seq(),
      Seq(Seq(core.Lit(parallelism)))
    )
    valRefAndDef(owner, "setParallelism", setParVDrhs)
  }

  object ScalaOps$ extends ModuleAPI {
    lazy val sym = api.Sym[ScalaOps.type].asModule

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

    val execute = op("execute")

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