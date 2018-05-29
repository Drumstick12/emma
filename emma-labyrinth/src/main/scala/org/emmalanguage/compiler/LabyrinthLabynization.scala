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

trait LabyrinthLabynization extends LabyrinthCompilerBase {

  import API._
  import UniverseImplicits._

  val labyrinthLabynize = TreeTransform("labyrinthLabynize", (tree: u.Tree) => {

    println("___")
    println("==0tree==")
    println(tree)
    println("==0tree==")

    // first traversal does the labyrinth normalization. second for block type correction.
    api.TopDown.unsafe
      .withOwner
      .transformWith {
        case t: u.Tree => t
      }._tree(tree)

  })
}