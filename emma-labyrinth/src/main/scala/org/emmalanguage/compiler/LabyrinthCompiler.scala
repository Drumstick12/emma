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
    // TODO

//        SparkBackend.transform,
//        SparkOptimizations.specializeOps iff "emma.compiler.spark.native-ops" is true,
//
    // lowering
//    Core.trampoline iff "emma.compiler.lower" is "trampoline"
//
//    // Core.dscfInv iff "emma.compiler.lower" is "dscfInv",
//
//    removeShadowedThis
  ) filterNot (_ == noop)

  // non-bag variables to DataBag
  val nonbag2bag = TreeTransform("nonbag2bag",
    api.TopDown.transform {
      // case vd @ core.ValDef(lhs, rhs) => core.Inst(api.Type(DataBag.tpe, Seq()))
      case vd @ core.ValDef(lhs, rhs) => vd
      case v => v
    }._tree
  )
}


