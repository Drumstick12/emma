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

import api.DataBag
import api.backend.LocalOps._
import api._

class TestInt(v: Int) {
  def addd(u: Int, w: Int, x: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
    this.v + u + w + x + m + n + s + t
}

class LabyrinthCompilerSpec extends BaseCompilerSpec
  with LabyrinthCompilerAware
  with LabyrinthAware {

  override val compiler = new RuntimeCompiler(codegenDir) with LabyrinthCompiler

  import compiler._
  import u.reify

  def withBackendContext[T](f: Env => T): T =
    withDefaultFlinkStreamEnv(f)


  val anfPipeline: u.Expr[Any] => u.Tree =
    pipeline(typeCheck = true)(
      Core.anf,
      Core.unnest
    ).compose(_.tree)

  def applyXfrm(xfrm: Xfrm): u.Expr[Any] => u.Tree = {

    pipeline(typeCheck = true)(
      Core.lnf,
      xfrm.timed
      ,
      Core.unnest
    ).compose(_.tree)
  }

  // ---------------------------------------------------------------------------
  // Spec tests
  // ---------------------------------------------------------------------------

  // helper
  def add1(x: Int) : Int = x + 1
  def str(x: Int) : String = x.toString
  def add(x: Int, y: Int) : Int = x + y
  def add(u: Int, v: Int, w: Int, x: Int, y: Int, z: Int)(m: Int, n: Int)(s: Int, t: Int) : Int =
    u + v + w + x + y + z + m + n + s + t

  // actual tests
  "normalization" - {
    "ValDef only" in {
      val inp = reify { val a = 1}
      val exp = reify { val a = DB.singSrc( () => { val tmp = 1; tmp } )}

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs on valdef rhs" in {
      val inp = reify { val a = 1; val b = a; val c = a; b}
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp });
        val b = a;
        val c = a;
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, SingSrc rhs" in {
      val inp = reify {
        val a = 1;
        val b = DB.singSrc(() => { val tmp = 2; tmp })
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp });
        val b = DB.singSrc(() => { val tmp = 2; tmp })
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs" in {
      val inp = reify
      {
        val a = 1
        val b = DataBag(Seq(2))
      }
      val exp = reify
      {
        val a = DB.singSrc(() => { val tmp = 1; tmp })
        val s = DB.singSrc(() => { val tmp = Seq(2); tmp })
        val sb = DB.fromSingSrcApply(s)
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, DataBag rhs 2" in {
      val inp = reify
      {
        val fun = add1(2)
        val s = Seq(fun)
        val b = DataBag(s)
      }
      val exp = reify
      {
        val lbdaFun = () => { val tmp = add1(2); tmp }
        val dbFun = DB.singSrc( lbdaFun )
        val dbs = dbFun.map( e => Seq(e) )
        val res = DB.fromSingSrcApply(dbs)
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs simple" in {
      val inp = reify { val a = 1; a}
      val exp = reify { val a = DB.singSrc(() => { val tmp = 1; tmp }); a}

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method one argument" in {
      val inp = reify {
        val a = 1;
        val b = add1(a);
        b
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp });
        val b = a.map(e => add1(e));
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    // TODO
    "TODO" in {
      //edges.withFilter(fun$r1)
    }

    "method one argument typechange" in {
      val inp = reify {
        val a = 1;
        val b = str(a);
        b
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp });
        val b = a.map(e => str(e));
        b
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments no constant" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(a,b)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = cross(a,b).map( (t: (Int, Int)) => add(t._1,t._2))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = add(3,4,a,5,6,7)(8,9)(10,b)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = cross(a,b).map( (t: (Int, Int)) => add(3,4,t._1,5,6,7)(8,9)(10,t._2))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = a.addd(1, b, 3)(4, 5)(6, 7)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = new TestInt(1); tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = cross(a,b).map( (t: (TestInt, Int)) => t._1.addd(1, t._2, 3)(4, 5)(6, 7))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments with constants" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = 3
        val d = add(3,4,a,5,6,7)(c,9)(10,b)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = DB.singSrc(() => { val tmp = 3; tmp })
        val d = DB.cross3(a,c,b).map( (t: (Int, Int, Int)) => add(3,4,t._1,5,6,7)(t._2,9)(10,t._3))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method three arguments 2" in {
      val inp = reify {
        val a = new TestInt(1)
        val b = 2
        val c = 3
        val d = a.addd(1, b, 3)(4, c)(6, 7)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = new TestInt(1); tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = DB.singSrc(() => { val tmp = 3; tmp })
        val d = DB.cross3(a,b,c).map( (t: (TestInt, Int, Int)) => t._1.addd(1, t._2, 3)(4, t._3)(6, 7))
      }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "triangles" in {
      val inp = reify {
//        val e = Edge(1, 2)
//        val edges = Seq(e)
//        val triangles = for {
//          Edge(x, u) <- edges
//          if x == u
//        } yield Edge(x, u)
//        // return
//        triangles

        val e = Edge.apply[Int](1, 2);
        val edges = Seq.apply[org.emmalanguage.compiler.Edge[Int]](e);
        val fun$r1 = ((check$ifrefutable$1: org.emmalanguage.compiler.Edge[Int]) => {
          val x$r2 = check$ifrefutable$1.src;
          val u$r2 = check$ifrefutable$1.dst;
          true
        });
        val anf$r5 = edges.withFilter(fun$r1);
        val fun$r2 = ((x$3: org.emmalanguage.compiler.Edge[Int]) => {
          val x$r1 = x$3.src;
          val u = x$3.dst;
          val anf$r8 = x$r1.==(u);
          anf$r8
        });
        val anf$r9 = anf$r5.withFilter(fun$r2);
        val fun$r3 = ((x$4: org.emmalanguage.compiler.Edge[Int]) => {
          val x = x$4.src;
          val u$r1 = x$4.dst;
          val anf$r12 = Edge.apply[Int](x, u$r1);
          anf$r12
        });
        val anf$r13 = Seq.canBuildFrom[org.emmalanguage.compiler.Edge[Int]];
        val triangles = anf$r9.map[org.emmalanguage.compiler.Edge[Int],
          Seq[org.emmalanguage.compiler.Edge[Int]]](fun$r3)(anf$r13);
        triangles
      }

      val exp = reify { 1 }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    // this is not yet suppported, if I'm not mistaken
    // "databag of lambdas" in {
    //   val inp = reify {
    //     val a = DataBag(Seq((a: Int) => add1(a)));
    //     val b = a.map(f => f(1));
    //    b
    //   }
    //   val exp = reify {
    //     1
    //   }
    //
    //   applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    // }
  }
}

case class Edge[V](src: V, dst: V)
case class LEdge[V, L](@emma.pk src: V, @emma.pk dst: V, label: L)
case class LVertex[V, L](@emma.pk id: V, label: L)
case class Triangle[V](x: V, y: V, z: V)
case class Message[K, V](@emma.pk tgt: K, payload: V)
