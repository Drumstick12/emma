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

//import shapeless.Generic
//import shapeless.Lazy
//import org.emmalanguage.api.DataBagSpec.CSVRecord
//import org.emmalanguage.test.schema.Literature.Book

//import scala.language.higherKinds
//import scala.language.implicitConversions

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
  "all tests" - {
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

    // ???
//    "DataBag.readXXXX" in {
//      val inp = reify {
//        val t = DataBag.readText("fooo")
//        val c = DataBag.readCSV[Book]("fooo", org.emmalanguage.api.CSV())
//        val p = DataBag.readParquet[Book]("fooo", org.emmalanguage.api.Parquet())
//      }
//
//      val exp = reify {
//        val a = 1
//      }
//
//      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
//    }

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

    "wordcount" in {
      val inp = reify {
        val docs = DataBag.readText(System.getProperty("java.io.tmpdir"))
        val words = for {
          line <- docs
          word <- DataBag[String](line.toLowerCase.split("\\W+"))
          if word != ""
        } yield word

        // group the words by their identity and count the occurrence of each word
        val counts = for {
          group <- words.groupBy(x => x)
        } yield (group.key, group.values.size)

        // counts.writeCSV("outputpath", org.emmalanguage.api.CSV())
        counts
      }

      // val xxxxxxx = {
      //   val docs = DataBag.readText(System.getProperty("java.io.tmpdir"))(LocalEnv.apply);
      //   val words = docs.flatMap(((line) => DataBag.apply[Predef.String](Predef.wrapRefArray(line.toLowerCase().split("\\W+")))(implicitly, LocalEnv.apply).withFilter(((word) => word.!=(""))).map(((word) => word))(implicitly)))(implicitly);
      //   val counts = words.groupBy(((x) => x))(implicitly).map(((group) => Tuple2.apply(group.key, group.values.size)))(implicitly);
      //   counts.writeCSV("outputpath",
      //     CSV.apply(CSV.apply$default$1, CSV.apply$default$2, CSV.apply$default$3, CSV.apply$default$4, CSV.apply$default$5, CSV.apply$default$6, CSV.apply$default$7, CSV.apply$default$8, CSV.apply$default$9))
      //   (CSVConverter.genericCSVConverter(Generic.materialize, Lazy.mkLazy))
      // }

      val exp = reify { val a = 1 }

      applyXfrm(labyrinthNormalize)(inp) shouldBe alphaEqTo(anfPipeline(exp))
    }

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
