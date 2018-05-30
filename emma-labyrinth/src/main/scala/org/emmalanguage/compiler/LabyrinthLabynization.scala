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

    println("___")
    println("==0tree==")
    println(tree)
    println("==0tree==")

    // first traversal does the labyrinth normalization. second for block type correction.
    api.TopDown.unsafe
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

              // typeinfo IN
              val typeInfoINVDrhs = core.DefCall(
                Option(FlinkScala$.ref),
                FlinkScala$.createTypeInformation,
                Seq(getTpe[labyrinth.util.Nothing]),
                Seq()
              )
              val typeInfoINRefDef = valRefAndDef(owner, "typeInfo", typeInfoINVDrhs)

              // executionConfig
              val confVDrhs = core.Inst(getTpe[org.apache.flink.api.common.ExecutionConfig])
              val confRefDef = valRefAndDef(owner, "ExecConf", confVDrhs)

              // createSerializer
              val serlzrVDrhs = core.DefCall(
                Some(typeInfoINRefDef._1),
                TypeInformation.createSerializer,
                Seq(),
                Seq(Seq(confRefDef._1))
              )
              val serlzrRefDef = valRefAndDef(owner, "Serializer", serlzrVDrhs)

              // typeinfo OUT
              val typeInfoOUTVDrhs = core.DefCall(
                Option(FlinkScala$.ref),
                FlinkScala$.createTypeInformation,
                Seq(targ),
                Seq()
              )
              val typeInfoOUTRefDef = valRefAndDef(owner, "typeInfo", typeInfoOUTVDrhs)

              // ElementOrEventTypeInfo
              val eleveVDrhs = core.Inst(
                ElementOrEvent.tpe,
                Seq(targ),
                Seq(Seq(typeInfoOUTRefDef._1))
              )
              val eleveRefDef = valRefAndDef(owner, "ElementOrEventTypeInfo", eleveVDrhs)

              // LabyNode
              val labyNodeVDrhs = core.Inst(
                LabyNode.tpe,
                Seq(getTpe[labyrinth.util.Nothing], targ),
                Seq(Seq(
                  core.Lit("fromNothing"), // name
                  bagOpRefDef._1,
                  core.Lit(1), // bbId
                  partRefDef._1,
                  serlzrRefDef._1,
                  eleveRefDef._1
                ))
              )
              val labyNodeRefDef = valRefAndDef(owner, "LabyNode", labyNodeVDrhs)

              // setParallelism
              val setParVDrhs = core.DefCall(
                Some(labyNodeRefDef._1),
                LabyNode.setParallelism,
                Seq(),
                Seq(Seq(core.Lit(1)))
              )
              val setParRefDef = valRefAndDef(owner, "setParallelism", setParVDrhs)

              // put everything into a block
              val blockVDrhs = core.Let(
                Seq(bagOpRefDef._2, partRefDef._2, typeInfoINRefDef._2, confRefDef._2, serlzrRefDef._2,
                  typeInfoOUTRefDef._2, eleveRefDef._2, labyNodeRefDef._2, setParRefDef._2),
                Seq(),
                setParRefDef._1
              )
              val blockRefDef = valRefAndDef(owner, "LabyEnd", blockVDrhs)

              blockRefDef._2

            case _ => vd
          }
        }
      }._tree(tree)

  })

  object ScalaOps$ extends ModuleAPI {
    lazy val sym = api.Sym[ScalaOps.type].asModule

    val fromNothing = op("fromNothing")

    override def ops = Set()
  }

  object FlinkScala$ extends ModuleAPI {
    lazy val sym = api.Sym[FlinkScala$.type].asModule

    def createTypeInformationWrapper[T: scala.reflect.ClassTag]: TypeInformation[T] =
      //org.apache.flink.api.scala.createTypeInformation
      macro org.apache.flink.api.scala.typeutils.TypeUtils.createTypeInfo[T]

    val createTypeInformation = op("createTypeInformationWrapper")

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

    override def ops = Set()
  }

  def getTpe[T: u.WeakTypeTag] : u.Type = api.Sym[T].asClass.toTypeConstructor
}