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

import compiler.RuntimeCompiler
import compiler.LabyrinthCompiler

import com.typesafe.config.Config
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

trait LabyrinthCompilerAware extends RuntimeCompilerAware {

  type Env = StreamExecutionEnvironment

  val compiler = new RuntimeCompiler(codegenDir) with LabyrinthCompiler

  import compiler._

  lazy val api = compiler.api

  def Env: u.Type = compiler.StreamExecutionEnvironment

  def transformations(cfg: Config): Seq[TreeTransform] =
    compiler.transformations(cfg)

  override protected lazy val addContext = TreeTransform("LabyrinthCompilerAware.addContext", tree => {
    import u.Quasiquote
    import API._
    import UniverseImplicits._

    //q"(env: $Env) => { implicit val e: $Env = env; implicit val cfg: _root_.org.apache.flink.api.common.ExecutionConfig = e.getConfig; $tree }: ${tree.tpe}"

    val dummySym = api.TermSym.free(api.TermName.fresh("aaaa"), Env)

    tree match {
      case Core.Lang.Let(vals, defs, exp) =>

        //val lbdaSym = api.ParSym(null, api.TermName.fresh("t"), Env) //api.TermSym.free(api.TermName.fresh("t"), Env)
        val lbdaSym = api.ParSym(dummySym, api.TermName.fresh("t"), Env)
        val lbdaSymRef = core.ParRef(lbdaSym)

        // implicit val env
        val envSym = api.ValSym(
          dummySym,
          api.TermName.fresh("env"),
          lbdaSym.tpe.widen,
          u.Flag.IMPLICIT)
        val envSymRefDef = valRefAndDef(envSym, lbdaSymRef)

        // env.getConfig
        val cfgSym = api.ValSym(
          dummySym,
          api.TermName.fresh("cfg"),
          getTpe[org.apache.flink.api.common.ExecutionConfig],
          u.Flag.IMPLICIT
        )
        val getConf = core.DefCall(Some(lbdaSymRef), StreamExecutionEnvironment.getConfig)
        val cfgRefDef = valRefAndDef(cfgSym, getConf)

        val lbdaBlock = core.Let(
          Seq(envSymRefDef._2, cfgRefDef._2) ++ vals,
          defs,
          exp
        )

        val lbda = core.Lambda(Seq(lbdaSym), lbdaBlock)
        val lbdaRefDef = valRefAndDef(dummySym, "lbda", lbda)

//        val out = core.Let(
//          Seq(lbdaRefDef._2),
//          Seq(),
//          lbdaRefDef._1
//        )
//        out
        lbda
    }

  })

  object StreamExecutionEnvironment extends ClassAPI {
    lazy val sym = api.Sym[org.apache.flink.streaming.api.scala.StreamExecutionEnvironment].asClass

    val getConfig = op("getConfig")

    override def ops = Set()
  }
}
