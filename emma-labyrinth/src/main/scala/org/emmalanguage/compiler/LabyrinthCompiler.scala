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

import com.typesafe.config.Config
import shapeless.::

trait LabyrinthCompiler extends Compiler {

  import UniverseImplicits._
  // import API._

  lazy val StreamExecutionEnvironment = api.Type[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]

  val core = Core.Lang

  def transformations(implicit cfg: Config): Seq[TreeTransform] = Seq(
    // lifting
    Lib.expand,
    Core.lift,
    // optimizations
    Core.cse iff "emma.compiler.opt.cse" is true,
    Optimizations.foldFusion iff "emma.compiler.opt.fold-fusion" is true,
    Optimizations.addCacheCalls iff "emma.compiler.opt.auto-cache" is true,
    // backend
    Comprehension.combine,
    Core.unnest,
    // labyrinth transformations
    nonbag2bag

    // lowering
//    Core.trampoline iff "emma.compiler.lower" is "trampoline"
//
//    // Core.dscfInv iff "emma.compiler.lower" is "dscfInv",
//
//    removeShadowedThis
  ) filterNot (_ == noop)

  // non-bag variables to DataBag
  val nonbag2bag = TreeTransform("nonbag2bag",
    api.TopDown.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if !meta(vd).all.all.contains(SkipTraversal) =>
          println(vd)
          // println(meta(vd).all.all)
          val seqRhs = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(rhs.tpe), Seq(Seq(rhs)))
          val seqRefDef = valRefAndDef(owner, "Seq", seqRhs)
          skip(seqRefDef._2)

          val databagRhs = core.DefCall(
            Some(API.DataBag$.ref), API.DataBag$.apply, Seq(rhs.tpe), Seq(Seq(seqRefDef._1))
          )
          val databagRefDef = valRefAndDef(owner, "Res", databagRhs)
          skip(databagRefDef._2)

          // dummy ValDef with letblock on rhs - gonna be eliminated by unnest
          val dummyRhs = core.Let(Seq(seqRefDef._2, databagRefDef._2), Seq(), databagRefDef._1)
          // println(meta(seqRefDef._2).all.all)
          // println(meta(dummyRhs.stats.head).all.all)
          // println(meta(databagRefDef._2).all.all)
          val dummySym = newSymbol(owner, "a_dummy", dummyRhs)
          val dummy = core.ValDef(dummySym, dummyRhs)
          skip(dummy)
          // println(meta(dummy).all.all)

          println("out: ")
          println(dummy)

          // dummy match {
          //  case core.ValDef(_, core.Let(stats, _, _)) => {
          //    println(stats.head)
          //    println(meta(stats.head).all.all)
          //  }
          //  case _ => ()
          // }

          println

          dummy
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _) if !meta(vd).all.all.contains(SkipTraversal) =>
          println(vd)
          core.ValDef(lhs, rhs)
      }._tree
  )

  private def newSymbol(own: u.Symbol, name: String, rhs: u.Tree): u.TermSymbol = {
    api.ValSym(own, api.TermName.fresh(name), rhs.tpe)
  }

  private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
    val lhs = api.ValSym(own, api.TermName.fresh(name), rhs.tpe)
    (core.Ref(lhs), core.ValDef(lhs, rhs))
  }

  object Seq$ extends ModuleAPI {

    lazy val sym = api.Sym[Seq.type].asModule

    val apply = op("apply")

    override def ops = Set()
  }

  case class SkipTraversal()
  def skip(t: u.Tree): Unit = {
    meta(t).update(SkipTraversal)
  }
}


