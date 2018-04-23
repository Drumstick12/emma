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
  import API._

  lazy val StreamExecutionEnvironment = api.Type[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment]

  val core = Core.Lang
  private val Seq(_1, _2) = {
    val tuple2 = api.Type[(Nothing, Nothing)]
    for (i <- 1 to 2) yield tuple2.member(api.TermName(s"_$i")).asTerm
  }

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
    // val refs = scala.collection.mutable.Map[u.TermSymbol, u.Ident]()
    // val defs = scala.collection.mutable.Map[u.Ident, u.ValDef]()
    val defs = scala.collection.mutable.Map[u.TermSymbol, u.ValDef]()

    println("___")
    println(LabyrinthCompiler.this)
    println("==0tree==")
    println(tree)
    println("==0tree==")

    val firstRun = api.TopDown.unsafe
      .withOwner
      .transformWith {

        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if prePrint(vd) && !meta(vd).all.all.contains(SkipTraversal)
            && refsSeen(rhs, seen) && !isFun(lhs) && !isFun(owner) =>
          rhs match {
            case core.ValRef(sym) if seen.keys.toList.contains (sym) =>
              val nvr = core.ValRef(seen(sym).get)
              val ns = newSymbol(owner, lhs.name.toString, nvr)
              val nvd = core.ValDef(ns, nvr)
              skip(nvd)
              seen += (lhs -> Some(ns))
              // refs += (ns -> nvr)
              nvd

            // first check if the rhs is a singleton bag due to anf and labyrinth transformation
            // {{{
            //      val a = 1;                          ==> Databag[Int]
            //      val anf$r1 = Seq.apply[Int](2);     ==> Databag[Seq[Int]]
            //      val b = DataBag.apply[Int](anf$r1); ==> Databag[Int] instead of Databag[Databag[Seq[Int]]]
            // }}}
            // val argReplRef = refs(seen(argsym).get)
            case dc @ core.DefCall(tgt, DataBag$.apply, targs, Seq(Seq(dcarg @ core.ValRef(argsym)))) =>
              val argSymRepl = seen(argsym).get
              val argReplRef = core.ValRef(argSymRepl)
              val argReplDef = defs(argSymRepl)
              val argReplIsSingBag = argReplDef.rhs match {
                //case core.DefCall(_, DB$.singSrc, _, Seq(Seq(vrarg2))) =>
                  //println(vrarg2)
                case let @ core.Let(_, _, core.ValRef(sym)) =>
                  println
                  defs(sym) match {
                  case core.ValDef(_, core.DefCall(_, DB$.singSrc, _, Seq(Seq(vrarg2)))) =>
                    println(vrarg2)
                    true
                  case _ => false
                }
                case _ => false
              }

              if (argReplIsSingBag) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromSingSrc,
                  Seq(argReplRef.tpe.widen.typeArgs.head.typeArgs.head),
                  Seq(Seq(argReplRef))
                )
                val dbSym = newSymbol(owner, "db", dbRhs)
                val dbRef = core.ValRef(dbSym)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                seen += (lhs -> Some(dbSym))
                // refs += (dbSym -> dbRef)
                // defs += (dbRef -> db)
                defs += (dbSym -> db)
                db
              } else {
                vd
              }

            case dc @ core.DefCall(tgt, ms, targs, Seq(Seq(dcarg @ core.ValRef(argsym)))) if !refSeen(tgt, seen) =>
              val argSymRepl = seen(argsym).get
              val argReplRef = core.ValRef(argSymRepl)
              val argReplDef = defs(argSymRepl)

              val lbdaSym = api.ParSym(owner, api.TermName.fresh("lmbda"), dcarg.tpe)
              val lmbdaRhsDC = core.DefCall(tgt, ms, targs, Seq(Seq(core.ParRef(lbdaSym))))
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(Seq(lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )

              val funSym = api.ValSym(owner, api.TermName.fresh("fun"), lmbda.tpe.widen)
              val funRefDef = valRefAndDef(funSym, lmbda)
              skip(funRefDef._2)

              val ndc = core.DefCall(Some(argReplRef), DataBag.map, Seq(dc.tpe), Seq(Seq(funRefDef._1)))
              val ns = newSymbol(owner, "dbMap", ndc)
              val ndcRefDef = valRefAndDef(ns, ndc)
              skip(ndcRefDef._2)

              // add lambda definition and new defcall to new letblock - eliminated by unnest
              val nlb = core.Let(Seq(funRefDef._2, ndcRefDef._2), Seq(), ndcRefDef._1)

              val nvdSym = api.ValSym(owner, api.TermName.fresh("map"), nlb.tpe.widen)
              val nvdRefDef = valRefAndDef(nvdSym, nlb)
              seen += (lhs -> Some(nvdSym))
              // refs += (nvdSym -> nvdRefDef._1)
              skip(nvdRefDef._2)

              nvdRefDef._2

            // TODO cross combination of two arguments
            case dc @ core.DefCall(tgt, ms, targs, args) =>
              println(tgt, ms, targs, args)
              // val tgtRepl = tgt match {
              //   case Some(core.ValRef(sym)) => {
              //     Some(core.ValRef(seen(sym).get))
              //  }
              //  case _ => None
              // }

              val argRepls = args.head.map(
                x => x match {
                  case core.ValRef(sym) if seen.keys.toList.contains(sym) => core.ValRef(seen(sym).get)
                  case _ => x
                }
              )
              val crossDc = core.DefCall(Some(Ops.ref), Ops.cross, targs, Seq(argRepls))
              println(crossDc)

              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), crossDc.tpe.typeArgs.head)
              val lbdaRef = core.ParRef(lbdaSym)
              // TODO:
              //   lambda = t -> {
              //     t1 = t._1
              //     t2 = t._2
              //     f(t1, t2)
              //   }

              //     t1 = t._1
              val t1 = core.DefCall(Some(lbdaRef), _1, Seq(), Seq(Seq()))
              val t1RefDef = valRefAndDef(owner, "t1", t1)

              //     t2 = t._2
              val t2 = core.DefCall(Some(lbdaRef), _2, Seq(), Seq(Seq()))
              val t2RefDef = valRefAndDef(owner, "t2", t2)

              //     f(t1, t2)
              val lmbdaRhsDC = core.DefCall(tgt, ms, targs, Seq(Seq(t1RefDef._1, t2RefDef._1)))
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(Seq(lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val crossSym = newSymbol(owner, "cross", crossDc)
              val crossRefDef = valRefAndDef(crossSym, crossDc)

              val ndc = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              crossRefDef._2

            case _ => {
              println
              vd
            }

          }

        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if prePrint(vd) && !meta(vd).all.all.contains(SkipTraversal)
            && !refsSeen(rhs, seen) && !isDatabag(rhs) && !isFun(lhs) && !isFun(owner) =>

          // create lambda () => rhs
          val rhsSym = newSymbol(owner, "lbda", rhs)
          val rhsRefDef = valRefAndDef(rhsSym, rhs)
          skip(rhsRefDef._2)
          val lRhs = core.Let(Seq(rhsRefDef._2), Seq(), rhsRefDef._1)
          val l = core.Lambda(Seq(), lRhs)
          val lSym = newSymbol(owner, "fun", l)
          val lRefDef = valRefAndDef(lSym, l)
          skip(lRefDef._2)

          val dbRhsDC = core.DefCall(Some(DB$.ref), DB$.singSrc, Seq(rhs.tpe), Seq(Seq(lRefDef._1)))
          val dbRhsDCSym = newSymbol(owner, "dbRhs", dbRhsDC)
          val dbRhsDCRefDef = valRefAndDef(dbRhsDCSym, dbRhsDC)
          skip(dbRhsDCRefDef._2)
          val dbRhs = core.Let(Seq(lRefDef._2, dbRhsDCRefDef._2), Seq(), dbRhsDCRefDef._1)
          val dbSym = newSymbol(owner, "db", dbRhsDC)
          val db = core.ValDef(dbSym, dbRhs)
          skip(db)

          // save mapping of refs -> defs
          val dbDefs = db.collect{ case dbvd @ core.ValDef(ld, _) => (ld, dbvd) }
          dbDefs.map(t => defs += (t._1 -> t._2))

          seen += (lhs -> Some(dbSym))
          defs += (dbSym -> db)
          postPrint(db)
          db

        case Attr.inh(vr @ core.ValRef(sym), _) =>
          if (prePrint(vr) && seen.keys.toList.contains(sym)) {
            val nvr = core.ValRef(seen(sym).get)
            skip(nvr)
            nvr
          } else {
            vr
          }

      }._tree(tree)

    // second traversal to correct block types
    // Background: scala does not change block types if expression type changes
    // (see internal/Trees.scala - Tree.copyAttrs)
    val secondRun = api.TopDown.unsafe
      .withOwner
      .transformWith {
        case Attr.inh(lb @ core.Let(valdefs, defdefs, expr), _) if lb.tpe != expr.tpe =>
          val nlb = core.Let(valdefs, defdefs, expr)
          nlb
      }._tree(firstRun)

    postPrint(secondRun)
    secondRun

  })

  def prePrint(t: u.Tree) : Boolean = {
    print("\nprePrint: ")
    print(t)
    print("   type: ")
    print(t.tpe)
    t match {
      case core.ValDef(lhs, rhs) =>
        print("   isFun: ")
        println(isFun(lhs))
      case _ => ()
    }
    true
  }

  def postPrint(t: u.Tree) : Unit = {
    print("postPrint: ")
    print(t)
    print("   type: ")
    println(t.tpe)
  }

  def refsSeen(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, Option[u.TermSymbol]]) : Boolean = {
    val refNames = t.collect{ case vr @ core.ValRef(_) => vr }.map(_.name)
    val seenNames = m.keys.toSeq.map(_.name)
    refNames.foldLeft(false)((a,b) => a || seenNames.contains(b))
  }

  def refSeen(t: Option[u.Tree], m: scala.collection.mutable.Map[u.TermSymbol, Option[u.TermSymbol]]): Boolean = {
    if (t.nonEmpty) {
      t.get match {
        case core.ValRef(sym) => {
          println
          m.keys.toList.contains(sym)
        }
        case _ => false
      }
    } else {
      false
    }
  }

  private def isDatabag(tree: u.Tree): Boolean = {
    tree.tpe.widen.typeConstructor =:= API.DataBag.tpe
  }

  def isFun(sym: u.TermSymbol) = api.Sym.funs(sym.info.dealias.widen.typeSymbol)
  def isFun(sym: u.Symbol) = api.Sym.funs(sym.info.dealias.widen.typeSymbol)

  private def newSymbol(own: u.Symbol, name: String, rhs: u.Tree): u.TermSymbol = {
    api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
  }

  private def valRefAndDef(own: u.Symbol, name: String, rhs: u.Tree): (u.Ident, u.ValDef) = {
    val sbl = api.ValSym(own, api.TermName.fresh(name), rhs.tpe.widen)
    (core.ValRef(sbl), core.ValDef(sbl, rhs))
  }

  private def valRefAndDef(sbl: u.TermSymbol, rhs: u.Tree): (u.Ident, u.ValDef) = {
    (core.ValRef(sbl), core.ValDef(sbl, rhs))
  }

  object Seq$ extends ModuleAPI {

    lazy val sym = api.Sym[Seq.type].asModule

    val apply = op("apply")

    override def ops = Set()
  }

  object DB$ extends ModuleAPI {

    lazy val sym = api.Sym[DB.type].asModule

    val singBag = op("singBag")
    val singSrc = op("singSrc")
    val fromSingSrc = op("fromSingSrc")

    override def ops = Set()

  }

  case class SkipTraversal()
  def skip(t: u.Tree): Unit = {
    meta(t).update(SkipTraversal)
  }
}

object DB {

  def singBag[A: org.emmalanguage.api.Meta](e: A): org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(Seq(e))
  }

  def singSrc[A: org.emmalanguage.api.Meta](l: () => A): org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(Seq(l()))
  }

  def fromSingBag[A: org.emmalanguage.api.Meta](db: org.emmalanguage.api.DataBag[Seq[A]]):
  org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(db.collect().head)
  }

  def fromSingSrc[A: org.emmalanguage.api.Meta](db: org.emmalanguage.api.DataBag[Seq[A]]):
  org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(db.collect().head)
  }

}


