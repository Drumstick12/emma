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

  "all tests" - {
    "ValDef only" in {
      val inp = reify { val a = 1}
      val exp = reify { val a = DB.singSrc( () => 1)}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "ValDef only, SingBag rhs" in {
      val inp = reify { val a = 1; val b = DB.singBag(2)}
      val exp = reify { val a = DB.singBag(1); val b = DB.singBag(2)}

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
        val xa = 1
        val a = DB.singBag(xa)
        val s = Seq(2)
        val sb = DB.singBag(s)
        val b = DB.fromSingBag(sb)
      }

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs simple" in {
      val inp = reify { val a = 1; a}
      val exp = reify { val a = DB.singBag(1); a}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace refs on valdef rhs" in {
      val inp = reify { val a = 1; val b = a; b}
      val exp = reify { val a = DB.singBag(1); val b = a; b}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

    "replace defcalls on valdef rhs" in {
      val inp = reify { val a = 1; val b = add1(a); b}
      val exp = reify { val a = DB.singBag(1); val b = a.map(e => add1(e)); b}

      applyXfrm(nonbag2bag)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }
  }
}
