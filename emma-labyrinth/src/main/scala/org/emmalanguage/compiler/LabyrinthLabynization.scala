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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.scala.typeutils.TypeUtils
import shapeless.::

import scala.runtime.Nothing$

trait LabyrinthLabynization extends LabyrinthCompilerBase {

  import API._
  import UniverseImplicits._

  val labyrinthLabynize = TreeTransform("labyrinthLabynize", (tree: u.Tree) => {
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
    val defs = scala.collection.mutable.Map[u.TermSymbol, u.ValDef]()

    println("___")
    println("==0tree Labynization==")
    println(tree)
    println("==0tree==")

    // first traversal does the labyrinth normalization. second for block type correction.
    val transOut = api.TopDown.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if prePrint(vd) && !isFun(lhs) && !isFun(owner) && !isAlg(rhs) => {

          rhs match {

            // singSrc to LabyNode
            case core.DefCall(Some(DB$.ref), DB$.singSrc, Seq(targ), Seq(Seq(funarg))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2

            // fromSingSrc to LabyNode
            case core.DefCall(Some(DB$.ref), DB$.fromSingSrcApply, Seq(targ), Seq(Seq(core.ValRef(singSrcDBsym)))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2

            // map to LabyNode
            case core.DefCall(Some(core.ValRef(tgtSym)), DataBag.map, Seq(outTpe), Seq(Seq(lbdaRef))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2

            // flatmap to LabyNode
            case core.DefCall(Some(core.ValRef(tgtSym)), DataBag.flatMap, Seq(outTpe), Seq(Seq(lbdaRef))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2

            case core.DefCall
              (_, Ops.cross, Seq(tpeA, tpeB), Seq(Seq(core.ValRef(lhsSym), core.ValRef(rhsSym)))) =>


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

              postPrint(blockRefDef._2)

              blockRefDef._2

            // fold1
            case core.DefCall(_, DB$.fold1, targs @ Seq(tpeA, tpeB), Seq(Seq(core.ValRef(dbSym), alg))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2


            // TODO fold2
            case core.DefCall (_, DB$.fold2, targs @ Seq(tpeA, tpeB), Seq(Seq(core.ValRef(dbSym), zero, init, plus))) =>

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

              postPrint(blockRefDef._2)

              blockRefDef._2

            case _ => vd
          }
        }
      }._tree(tree)

    postPrint(transOut)
    transOut
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
    val reduce = op("reduce")
    val fromNothing = op("fromNothing")
    val fromSingSrcApply = op("fromSingSrcApply")
    val map = op("map")


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