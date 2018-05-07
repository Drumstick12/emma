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

import scala.collection.mutable.ListBuffer

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

  private val Seq(_3_1, _3_2, _3_3) = {
    val tuple3 = api.Type[(Nothing, Nothing, Nothing)]
    for (i <- 1 to 3) yield tuple3.member(api.TermName(s"_$i")).asTerm
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
    labyrinthNormalize

    // lowering
    //    Core.trampoline iff "emma.compiler.lower" is "trampoline"
    //
    //    // Core.dscfInv iff "emma.compiler.lower" is "dscfInv",
    //
    //    removeShadowedThis
  ) filterNot (_ == noop)

  // non-bag variables to DataBag
  val labyrinthNormalize = TreeTransform("nonbag2bag", (tree: u.Tree) => {
    val replacements = scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]()
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
            && refsSeen(rhs, replacements) && !isFun(lhs) && !isFun(owner) =>

          /*
            helper function for the second case of user databag creation (see below)
            {{{
                 val a = "path";                     ==> Databag[String]
                 val b = DataBag.readText(a)         ==> Databag[String]

                 val a' = singSrc[String]("path")
                 val b' = fromSingSrc[String](a')
            }}}
           */
          def defFromDatabagRead(s: u.TermSymbol) : u.ValDef = {
            val argSymRepl = replacements(s)
            val argReplRef = core.ValRef(argSymRepl)

            if (defcallargBecameSingSrc(s)) {
              val dbRhs = core.DefCall(
                Some(DB$.ref),
                DB$.fromSingSrcReadText,
                Seq(argReplRef.tpe.widen.typeArgs.head),
                Seq(Seq(argReplRef))
              )
              val dbSym = newSymbol(owner, "db", dbRhs)
              val db = core.ValDef(dbSym, dbRhs)
              skip(db)

              replacements += (lhs -> dbSym)
              defs += (dbSym -> db)
              db
            } else {
              vd
            }
          }

          def defcallargBecameSingSrc(s: u.TermSymbol) : Boolean = {
            val argSymRepl = replacements(s)
            val argReplDef = defs(argSymRepl)
            argReplDef.rhs match {
              case core.Let(_, _, core.ValRef(sym)) =>
                defs(sym) match {
                  case core.ValDef(_, core.DefCall(_, DB$.singSrc, _, _)) => true
                  case _ => false
                }
              case _ => false
            }
          }

          rhs match {
            case core.ValRef(sym) if replacements.keys.toList.contains (sym) =>
              val nvr = core.ValRef(replacements(sym))
              val ns = newSymbol(owner, lhs.name.toString, nvr)
              val nvd = core.ValDef(ns, nvr)
              skip(nvd)
              replacements += (lhs -> ns)
              nvd

            /*
            In the following we have to catch all the cases where the user manually creates Databags.

            first: check if the rhs is a singleton bag of Seq[A] due to anf and labyrinth transformation
            {{{
                 val a = Seq.apply[Int](2)
                 val b = DataBag.apply[Int](a)

                 val a' = singSrc[Seq[Int]](Seq(2)) ==> Databag[Seq[Int]]
                 val b' = fromSingSrc[Int](a')      ==> Databag[Int]
            }}}
             */
            case core.DefCall(_, DataBag$.apply, _, Seq(Seq(core.ValRef(argsym)))) =>
              val argSymRepl = replacements(argsym)
              val argReplRef = core.ValRef(argSymRepl)

              if (defcallargBecameSingSrc(argsym)) {
                val dbRhs = core.DefCall(
                  Some(DB$.ref),
                  DB$.fromSingSrcApply,
                  Seq(argReplRef.tpe.widen.typeArgs.head.typeArgs.head),
                  Seq(Seq(argReplRef))
                )
                val dbSym = newSymbol(owner, "db", dbRhs)
                val db = core.ValDef(dbSym, dbRhs)
                skip(db)

                replacements += (lhs -> dbSym)
                defs += (dbSym -> db)
                db
              } else {
                vd
              }

            /* TODO second: check if the user creates a databag using the .readText, .readCSV, or .readParqet methods.
             */
            case core.DefCall(_, DataBag$.readText, _, Seq(Seq(core.ValRef(argsym)))) => defFromDatabagRead(argsym)
            // TODO I very much expect these to not work because they are not single-paramater calls, although it
            // TODO is possible that they are caught by the cross-map case
            case core.DefCall(_, DataBag$.readCSV, _, Seq(Seq(core.ValRef(argsym)))) => defFromDatabagRead(argsym)
            case core.DefCall(_, DataBag$.readParquet, _, Seq(Seq(core.ValRef(argsym)))) => defFromDatabagRead(argsym)

            // TODO multi-argument case...
            // somthing like add1(a) after a became a databag db$a
            case dc @ core.DefCall(tgt, ms, targs, Seq(Seq(dcarg @ core.ValRef(argsym))))
              if !refSeen(tgt, replacements) =>

              val argSymRepl = replacements(argsym)
              val argReplRef = core.ValRef(argSymRepl)

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
              replacements += (lhs -> nvdSym)
              // refs += (nvdSym -> nvdRefDef._1)
              skip(nvdRefDef._2)

              nvdRefDef._2

            // if there are 2 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
            case dc @ core.DefCall(_, _, _, _) if prePrint(dc) && countSeenRefs(dc, replacements)==2 =>
              val refs = dc.collect{
                case vr @ core.ValRef(_) => vr
              }
              val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))

              val targsRepls = nonC.map(_.tpe)
              val crossDc = core.DefCall(Some(Ops.ref), Ops.cross, targsRepls, Seq(nonC))
              skip(crossDc)

              val x = nonC(0)
              val y = nonC(1)

              val xyTpe = api.Type.kind2[Tuple2](x.tpe, y.tpe)
              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyTpe)
              val lbdaRef = core.ParRef(lbdaSym)
              //   lambda = t -> {
              //     t1 = t._1
              //     t2 = t._2
              //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
              //   }

              //     t1 = t._1
              val t1 = core.DefCall(Some(lbdaRef), _1, Seq(), Seq())
              val t1RefDef = valRefAndDef(owner, "t1", t1)
              skip(t1RefDef._2)

              //     t2 = t._2
              val t2 = core.DefCall(Some(lbdaRef), _2, Seq(), Seq())
              val t2RefDef = valRefAndDef(owner, "t2", t2)
              skip(t2RefDef._2)

              val m = Map(x -> t1RefDef._1, y -> t2RefDef._1)

              val lmbdaRhsDC = api.TopDown.transform{
                case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
              }._tree(dc)
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(Seq(t1RefDef._2, t2RefDef._2, lmbdaRhsDCRefDef._2), Seq(), lmbdaRhsDCRefDef._1)
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val crossSym = newSymbol(owner, "cross", crossDc)
              val crossRefDef = valRefAndDef(crossSym, crossDc)
              skip(crossRefDef._2)

              val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
              skip(mapDCRefDef._2)

              val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
              val blockFinalSym = newSymbol(owner, "res", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)

              blockFinalRefDef._2

            // if there are 3 non-constant arguments inside the defcall, cross and apply the defcall method to the tuple
            case dc @ core.DefCall(_, _, _, _) if prePrint(dc) && countSeenRefs(dc, replacements)==3 =>
              val refs = dc.collect{
                case vr @ core.ValRef(_) => vr
              }
              val nonC = refs.filter(e => replacements.keys.toList.map(_.name).contains(e.name))

              val targsRepls = nonC.map(_.tpe)
              val crossDc = core.DefCall(Some(DB$.ref), DB$.cross3, targsRepls, Seq(nonC))
              skip(crossDc)

              val x = nonC(0)
              val y = nonC(1)
              val z = nonC(2)

              val xyzTpe = api.Type.kind3[Tuple3](x.tpe, y.tpe, z.tpe)
              val lbdaSym = api.ParSym(owner, api.TermName.fresh("t"), xyzTpe)
              val lbdaRef = core.ParRef(lbdaSym)
              //   lambda = t -> {
              //     t1 = t._1
              //     t2 = t._2
              //     t3 = t._3
              //     t1.f(c1, ..., cn, t2, cn+2, ..., ck)(impl ...)
              //   }

              //     t1 = t._1
              val t1 = core.DefCall(Some(lbdaRef), _3_1, Seq(), Seq())
              val t1RefDef = valRefAndDef(owner, "t1", t1)
              skip(t1RefDef._2)

              //     t2 = t._2
              val t2 = core.DefCall(Some(lbdaRef), _3_2, Seq(), Seq())
              val t2RefDef = valRefAndDef(owner, "t2", t2)
              skip(t2RefDef._2)

              //     t2 = t._3
              val t3 = core.DefCall(Some(lbdaRef), _3_3, Seq(), Seq())
              val t3RefDef = valRefAndDef(owner, "t3", t3)
              skip(t3RefDef._2)

              val m = Map(x -> t1RefDef._1, y -> t2RefDef._1, z -> t3RefDef._1)

              val lmbdaRhsDC = api.TopDown.transform{
                case v @ core.ValRef(_) => if (m.keys.toList.contains(v)) m(v) else v
              }._tree(dc)
              val lmbdaRhsDCRefDef = valRefAndDef(owner, "lbdaRhs", lmbdaRhsDC)
              skip(lmbdaRhsDCRefDef._2)
              val lmbdaRhs = core.Let(
                Seq(t1RefDef._2, t2RefDef._2, t3RefDef._2, lmbdaRhsDCRefDef._2),
                Seq(),
                lmbdaRhsDCRefDef._1
              )
              val lmbda = core.Lambda(
                Seq(lbdaSym),
                lmbdaRhs
              )
              val lambdaRefDef = valRefAndDef(owner, "lambda", lmbda)

              val crossSym = newSymbol(owner, "cross3", crossDc)
              val crossRefDef = valRefAndDef(crossSym, crossDc)
              skip(crossRefDef._2)

              val mapDC = core.DefCall(Some(crossRefDef._1), DataBag.map, Seq(dc.tpe), Seq(Seq(lambdaRefDef._1)))
              val mapDCRefDef = valRefAndDef(owner, "map", mapDC)
              skip(mapDCRefDef._2)

              val blockFinal = core.Let(Seq(crossRefDef._2, lambdaRefDef._2, mapDCRefDef._2), Seq(), mapDCRefDef._1)
              val blockFinalSym = newSymbol(owner, "res", blockFinal)
              val blockFinalRefDef = valRefAndDef(blockFinalSym, blockFinal)
              skip(blockFinalRefDef._2)

              replacements += (lhs -> blockFinalSym)

              blockFinalRefDef._2

            case _ => {
              println
              vd
            }

          }

        case Attr.inh(vd @ core.ValDef(lhs, rhs), owner :: _)
          if prePrint(vd) && !meta(vd).all.all.contains(SkipTraversal)
            && !refsSeen(rhs, replacements) && !isDatabag(rhs) && !isFun(lhs) && !isFun(owner) =>

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

          replacements += (lhs -> dbSym)
          defs += (dbSym -> db)
          postPrint(db)
          db

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

  def countSeenRefs(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]) : Int = {
    val refs = t.collect{ case vr @ core.ValRef(_) => vr.name }
    refs.foldLeft(0)((a,b) => a + (if (m.keys.toList.map(_.name).contains(b)) 1 else 0))
  }

  def refsSeen(t: u.Tree, m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]) : Boolean = {
    val refNames = t.collect{ case vr @ core.ValRef(_) => vr }.map(_.name)
    val seenNames = m.keys.toSeq.map(_.name)
    refNames.foldLeft(false)((a,b) => a || seenNames.contains(b))
  }

  def refSeen(t: Option[u.Tree], m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]): Boolean = {
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

  /**
   * returns a tuple indicating if the DefCall target is a non-constant argument (Boolean) and
   * all arguments as their potential replacements (or original if no replacement found in map)
   *
   * This solution actually requires some unnecessary work and should be refined later.
   */
  def analyzeArgs(tgt: Option[u.Tree], args: Seq[Seq[u.Tree]],
    m: scala.collection.mutable.Map[u.TermSymbol, u.TermSymbol]) :
  (Option[u.Tree], Seq[Seq[u.Tree]], Vector[(Int, Int)]) = {
    val tgtRepl = {
      if (tgt.nonEmpty) {
        tgt.get match {
          case core.ValRef(sym) => {
            if (m.keys.toList.contains(sym)) {
              Some(core.ValRef(m(sym)))
            }
            else tgt
          }
          case _ => tgt
        }
      } else {
        tgt
      }
    }

    val argPos = ListBuffer[(Int,Int)]()
    var posOuter = -1
    val argsRepls = args.map(
      s => {
        posOuter += 1
        var posInner = -1
        s.map{
          case vr @ core.ValRef(sym) => {
            posInner += 1
            if (m.keys.toList.contains(sym)) {
              argPos.append((posOuter, posInner))
              core.ValRef(m(sym))
            }
            else vr
          }
          case e => {
            posInner += 1
            e
          }
        }
      }
    )

    (tgtRepl, argsRepls, argPos.toList.toVector)
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

    val singSrc = op("singSrc")
    val fromSingSrcApply = op("fromSingSrcApply")
    val fromSingSrcReadText = op("fromSingSrcReadText")

    val cross3 = op("cross3")

    override def ops = Set()

  }

  case class SkipTraversal()
  def skip(t: u.Tree): Unit = {
    meta(t).update(SkipTraversal)
  }
}

object DB {

  def singSrc[A: org.emmalanguage.api.Meta](l: () => A): org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(Seq(l()))
  }

  def fromSingSrcApply[A: org.emmalanguage.api.Meta](db: org.emmalanguage.api.DataBag[Seq[A]]):
  org.emmalanguage.api.DataBag[A] = {
    org.emmalanguage.api.DataBag(db.collect().head)
  }

  def fromSingSrcReadText[A: org.emmalanguage.api.Meta](db: org.emmalanguage.api.DataBag[String]):
  org.emmalanguage.api.DataBag[String] = {
    org.emmalanguage.api.DataBag.readText(db.collect().head)
  }

  def cross3[A: org.emmalanguage.api.Meta, B: org.emmalanguage.api.Meta, C: org.emmalanguage.api.Meta](
    xs: org.emmalanguage.api.DataBag[A], ys: org.emmalanguage.api.DataBag[B], zs: org.emmalanguage.api.DataBag[C]
  )(implicit env: org.emmalanguage.api.LocalEnv): org.emmalanguage.api.DataBag[(A, B, C)] = for {
    x <- xs
    y <- ys
    z <- zs
  } yield (x, y, z)

}


