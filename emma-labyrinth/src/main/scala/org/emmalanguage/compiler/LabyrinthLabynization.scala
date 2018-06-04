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

            // flatmap to LabyNode
            case core.DefCall(Some(core.ValRef(tgtSym)), DataBag.flatMap, Seq(outTpe), Seq(Seq(lbdaRef))) =>

              vd

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
      ElementOrEvent.tpe,
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
      LabyNode.tpe,
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
      LabyNode.addInput,
      Seq(),
      Seq(Seq(singSrcDBReplRef, core.Lit(insideBlock), core.Lit(condOut)))
    )
    valRefAndDef(owner, "addInput", addInputVDrhs)
  }

  def getSetParallelismRefDef(owner: u.Symbol, tgtRef: u.Ident, parallelism: Int): (u.Ident, u.ValDef) = {
    val setParVDrhs = core.DefCall(
      Some(tgtRef),
      LabyNode.setParallelism,
      Seq(),
      Seq(Seq(core.Lit(parallelism)))
    )
    valRefAndDef(owner, "setParallelism", setParVDrhs)
  }

  object ScalaOps$ extends ModuleAPI {
    lazy val sym = api.Sym[ScalaOps.type].asModule

    val flatMapDataBagHelper = op("flatMapDataBagHelper")
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

  object TypeInformation extends ClassAPI {
    lazy val sym = api.Sym[org.apache.flink.api.common.typeinfo.TypeInformation[Any]].asClass

    val createSerializer = op("createSerializer")

    override def ops = Set()
  }

  object ElementOrEvent extends ClassAPI {
    lazy val sym = api.Sym[labyrinth.ElementOrEventTypeInfo[Any]].asClass
    override def ops = Set()
  }

  object LabyNode extends ClassAPI {
    lazy val sym = api.Sym[labyrinth.LabyNode[Any,Any]].asClass

    val setParallelism = op("setParallelism")
    val addInput = op("addInput", List(3))
    override def ops = Set()
  }

  def getTpe[T: u.WeakTypeTag] : u.Type = api.Sym[T].asClass.toTypeConstructor

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
    // Labyrinth Types
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[labyrinth.util.Nothing]], createTypeInformation)
    memoizeTypeInfo(implicitly[org.emmalanguage.api.Meta[Seq[Int]]], createTypeInformation)
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