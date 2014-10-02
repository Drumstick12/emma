package eu.stratosphere.emma.macros.program


import eu.stratosphere.emma.api.Algorithm
import eu.stratosphere.emma.macros.program.controlflow.ControlFlow
import eu.stratosphere.emma.macros.program.comprehension.Comprehension
import eu.stratosphere.emma.macros.program.comprehension.rewrite.ComprehensionNormalization

import scala.language.existentials
import scala.language.experimental.macros
import scala.reflect.macros.blackbox

class WorkflowMacros(val c: blackbox.Context) {

  /**
   * Entry macro for emma algorithms.
   */
  def parallelize[T: c.WeakTypeTag](e: c.Expr[T]): c.Expr[Algorithm[T]] = {
    new LiftHelper[c.type](c).parallelize[T](e)
  }

  private class LiftHelper[C <: blackbox.Context](val c: C)
    extends ContextHolder[c.type]
    with ControlFlow[c.type]
    with Comprehension[c.type] {

    import c.universe._

    /**
     * Translates an Emma expression to an Algorithm.
     *
     * @return
     */
    def parallelize[T: c.WeakTypeTag](root: Expr[T]): Expr[Algorithm[T]] = {

      // Create a normalized version of the original tree
      val normalizedTree = normalize(root.tree)

      // ----------------------------------------------------------------------
      // Code analysis
      // ----------------------------------------------------------------------

      // 1. Create control flow graph
      val cfGraph = createControlFlowGraph(normalizedTree)

      // 2. Identify and isolate maximal comprehensions
      val comprehensionStore = createComprehensionStore(cfGraph)

      // 3. Analyze variable usage

      // ----------------------------------------------------------------------
      // Code optimizations
      // ----------------------------------------------------------------------

      // 1. Comprehension rewrite (TODO)

      // Sufficient conditions to rewrite folds using banana-split + fusion
      // 0) Let `f` be a fold expression
      // 1) Fold input should be a shared comprehended identifier (say `A`, or `A.values` where `A` is a group)
      // 2) Reaching defititions of A should be a singleton set at all folds (say, with definition `d`)
      // 3) There should be no terms that reference `A` which contain `d` in its reaching definitions

      // 2. Derive logical plans (TODO)

      // ----------------------------------------------------------------------
      // Final object assembly
      // ----------------------------------------------------------------------

      // construct algorithm object
      val algorithmCode =
        q"""
        object __emmaAlgorithm extends eu.stratosphere.emma.api.Algorithm[${c.weakTypeOf[T]}] {

           // required Emma imports
           import eu.stratosphere.emma.api._
           import eu.stratosphere.emma.ir
           // required Scala imports
           import scala.reflect.runtime.universe._

           def run(engine: runtime.Engine): ${c.weakTypeOf[T]} = engine match {
             case runtime.Native => runNative()
             case _ => runParallel(engine)
           }

           private def runNative(): ${c.weakTypeOf[T]} = ${c.untypecheck(root.tree)}

           private def runParallel(engine: runtime.Engine): ${c.weakTypeOf[T]} = ${compile(normalizedTree, cfGraph, comprehensionStore)}
        }
        """

      // construct and return a block that returns a Workflow using the above list of sinks
      val block = Block(List(algorithmCode), c.parse("__emmaAlgorithm"))
      c.Expr[Algorithm[T]](c.typecheck(block))
    }

    /**
     * Compiles a generic driver for a data-parallel runtime.
     *
     * @param tree The original program tree.
     * @param cfGraph The control flow graph representation of the tree.
     * @param comprehensionStore A store containing the comprehended terms in the tree.
     * @return A tree representing the compiled triver.
     */
    def compile(tree: Tree, cfGraph: CFGraph, comprehensionStore: ComprehensionStore): Tree = {

      object expandComprehendedTerms extends Transformer with (Tree => Tree) {
        override def transform(tree: Tree): Tree = comprehensionStore.getByTerm(tree) match {
          case Some(t) =>
            expand(tree, cfGraph, comprehensionStore)(t)
          case _ =>
            super.transform(tree)
        }

        override def apply(tree: Tree): Tree = transform(tree)
      }

      c.untypecheck(expandComprehendedTerms(tree))
    }

  }

}