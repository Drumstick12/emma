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

//import cats.instances.all._

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
  val nonbag2bag = TreeTransform("nonbag2bag", (tree: u.Tree) => {
    val seen = scala.collection.mutable.Map[u.TermSymbol, Option[u.TermSymbol]]()

    val firstRun = api.TopDown.unsafe
      .withOwner
      .transformWith {

        // TODO switch refs on ValDef rhs
        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if !meta(vd).all.all.contains(SkipTraversal) && refsKnown(rhs, seen) =>
          rhs match {
            case core.ValRef(sym) if seen.keys.toList.contains (sym) =>
              val nvr = core.ValRef (seen(sym).get)
              val ns = newSymbol(owner, "newRef", nvr)
              val nvd = core.ValDef(ns, nvr)
              skip(nvd)
              seen += (lhs -> Some(ns))
              nvd
          }

        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if !meta(vd).all.all.contains(SkipTraversal) && !refsKnown(rhs, seen) =>

          // transform   a = 1   to   db = Databag(Seq(1))
          val seqRhs = core.DefCall(Some(Seq$.ref), Seq$.apply, Seq(rhs.tpe), Seq(Seq(rhs)))
          val seqRefDef = valRefAndDef(owner, "Seq", seqRhs)
          skip(seqRefDef._2)

          val databagRhs = core.DefCall(
            Some(API.DataBag$.ref), API.DataBag$.apply, Seq(rhs.tpe), Seq(Seq(seqRefDef._1))
          )
          val databagRefDef = valRefAndDef(owner, "DataBag", databagRhs)
          skip(databagRefDef._2)

          // dummy ValDef with letblock on rhs - gonna be eliminated by unnest
          val dummyRhs = core.Let(Seq(seqRefDef._2, databagRefDef._2), Seq(), databagRefDef._1)

          val dummySym = newSymbol(owner, "db", dummyRhs)
          val dummy = core.ValDef(dummySym, dummyRhs)
          skip(dummy)

          seen += (lhs -> Some(dummySym))
          dummy

        case Attr.inh(vr @ core.ValRef(sym), _) =>
          if (seen.keys.toList.contains(sym)) {
            val nvr = core.ValRef(seen(sym).get)
            skip(nvr)
            nvr
          } else {
            seen += (sym -> None)
            vr
          }

      }._tree(tree)

    // second traversal to correct block types
    // Background: scala does not change block types if expression type changes
    // (see internal/Trees.scala - Tree.copyAttrs)
    api.TopDown.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(lb @ core.Let(vals, defs, expr), owner :: _) if lb.tpe != expr.tpe =>
          val nlb = core.Let(vals, defs, expr)
          nlb
      }._tree(firstRun)
  })

  def prePrint(t: u.Tree) : Boolean = {
    print("\nprePrint: ")
    print(t)
    print("   type: ")
    println(t.tpe)
    true
  }

  def postPrint(t: u.Tree) : Unit = {
    print("postPrint: ")
    print(t)
    print("   type: ")
    println(t.tpe)
  }

  def refsKnown(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, Option[u.TermSymbol]]) : Boolean = {
    val refNames = t.collect{ case vr @ core.ValRef(_) => vr }.map(_.name)
    val seenNames = m.keys.toSeq.map(_.name)
    refNames.foldLeft(false)((a,b) => a || seenNames.contains(b))
  }

  private def isDatabag(tree: u.Tree): Boolean =  {
    if (tree.tpe == API.DataBag.tpe) return true
    false
  }

  private def newSymbol(own: u.Symbol, name: String, rhs: u.Tree): u.TermSymbol = {
    api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
  }

  private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
    val sbl = api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
    (core.Ref(sbl), core.ValDef(sbl, rhs))
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


