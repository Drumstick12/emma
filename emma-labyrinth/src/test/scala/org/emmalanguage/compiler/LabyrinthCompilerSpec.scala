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

  "all tests" - {
    "ValDef only" in {
      val inp = reify { val a = 1}
      val exp = reify { val a = DB.singSrc( () => { val tmp = 1; tmp } )}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs on valdef rhs" in {
      val inp = reify { val a = 1; val b = a; val c = a; b}
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp });
        val b = a;
        val c = a;
        b
      }

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
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

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
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
        val sb = DB.fromSingSrc(s)
      }

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs simple" in {
      val inp = reify { val a = 1; a}
      val exp = reify { val a = DB.singSrc(() => { val tmp = 1; tmp }); a}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
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

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
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

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments 1" in {
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

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "method two arguments 2" in {
      val inp = reify {
        val a = 1
        val b = 2
        val c = a.+(b)
      }
      val exp = reify {
        val a = DB.singSrc(() => { val tmp = 1; tmp })
        val b = DB.singSrc(() => { val tmp = 2; tmp })
        val c = cross(a,b).map( (t: (Int, Int)) => t._1 + t._2)
      }

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "test with some methods" in {
      val inp = reify {
        val a = DataBag(Seq((a: Int) => add1(a)));
        val b = a.map(f => f(1));
        b
      }
      val exp = reify {
        1
      }

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }
  }
}
