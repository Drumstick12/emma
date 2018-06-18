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
package labyrinth.operators;

import labyrinth.BagOperatorOutputCollector
import api.DataBag
import api.alg.Alg
import compiler.Memo
import io.csv.CSV
import io.csv.CSVScalaSupport
import labyrinth.util.SerializedBuffer
import org.apache.flink.core.fs.FileInputSplit

import scala.util.Either

import java.util

object ScalaOps {

  def map[IN, OUT](f: IN => OUT): FlatMap[IN, OUT] = {

    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        out.collectElement(f(e))
      }
    }
  }

  def flatMap[IN, OUT](f: (IN, BagOperatorOutputCollector[OUT]) => Unit): FlatMap[IN, OUT] = {

    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        f(e, out)
      }
    }
  }

  def flatMapDataBagHelper[IN, OUT](f: IN => DataBag[OUT]): FlatMap[IN, OUT] = {

    val lbda = (e: IN, coll: BagOperatorOutputCollector[OUT]) =>
      for(elem <- f(e)) yield { coll.collectElement(elem) }

    new FlatMap[IN, OUT]() {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        lbda(e, out)
      }
    }

  }

  def fromNothing[OUT](f: () => OUT ): BagOperator[org.emmalanguage.labyrinth.util.Nothing,OUT] = {

    new BagOperator[org.emmalanguage.labyrinth.util.Nothing,OUT]() {
      override def openOutBag() : Unit = {
        super.openOutBag()
        out.collectElement(f())
        out.closeBag()
      }
    }
  }

  def fromSingSrcApply[IN](): SingletonBagOperator[Seq[IN], IN] = {

    new SingletonBagOperator[Seq[IN], IN] {
      override def pushInElement(e: Seq[IN], logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        e.foreach(x => out.collectElement(x))
      }
    }
  }

  def foldGroup[K,IN,OUT](keyExtractor: IN => K, i: IN => OUT, f: (OUT, OUT) => OUT): FoldGroup[K,IN,OUT] = {

    new FoldGroup[K, IN, OUT]() {

      override protected def keyExtr(e: IN): K = keyExtractor(e)

      override def openOutBag(): Unit = {
        super.openOutBag()
        hm = new util.HashMap[K, OUT]
      }

      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        val key = keyExtr(e)
        val g = hm.get(key)
        if (g == null) {
          hm.put(key, i(e))
        } else {
          hm.replace(key, f(g, i(e)))
        }
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)

        import scala.collection.JavaConversions._

        for (e <- hm.entrySet) {
          out.collectElement(e.getValue)
        }
        hm = null
        out.closeBag()
      }
    }
  }

  def reduceGroup[K,A](keyExtractor: A => K, f: (A, A) => A): FoldGroup[K, A, A] = {
    foldGroup(keyExtractor, (x:A) => x, f)
  }

  def fold[A,B](zero: B, init: A => B, plus: (B, B) => B): BagOperator[A,B] = {
    new Fold[A,B] {
      override def openInBag(logicalInputId: Int): Unit = {
        super.openInBag(logicalInputId)
        assert (host.subpartitionId == 0) // Parallelism should be 1.
        result = zero
      }

      override def pushInElement(e: A, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        result = plus(result, init(e))
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)
        out.collectElement(result)
        out.closeBag()
      }
    }
  }

  def foldAlgHelper[A,B](alg: Alg[A,B]): BagOperator[A,B] = {
    fold(alg.zero, alg.init, alg.plus)
  }

  def reduce[A](zero:A, plus: (A,A) => A): BagOperator[A,A] = {
    fold(zero, e => e, plus)
  }

  def joinGeneric[IN, K](keyExtractor: IN => K): JoinGeneric[IN, K] = {
    new JoinGeneric[IN, K] {
      override protected def keyExtr(e: IN): K = keyExtractor(e)
    }
  }

  def cross[A,B]: BagOperator[Either[A, B], org.apache.flink.api.java.tuple.Tuple2[A, B]] =
    new Cross[A,B] {}

  def singletonBagOperator[IN, OUT](f: IN => OUT): SingletonBagOperator[IN, OUT] = {
    new SingletonBagOperator[IN, OUT] {
      override def pushInElement(e: IN, logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        out.collectElement(f(e))
      }
    }
  }

  def union[T](): Union[T] = {
    new Union[T]
  }

  def textSource: BagOperator[String, InputFormatWithInputSplit[String, FileInputSplit]] = {
    new CFAwareTextSource
  }

  def textReader: BagOperator[InputFormatWithInputSplit[String, FileInputSplit], String] = {
    new CFAwareFileSourceParaReader[String, FileInputSplit](Memo.typeInfoForType[String])
  }

  def toCsvString[T](implicit converter: org.emmalanguage.io.csv.CSVConverter[T]): BagOperator[Either[T, CSV], String] =
  {
    new ToCsvString[T] {

      override def openOutBag(): Unit = {
        super.openOutBag()
        buff = new SerializedBuffer[Either[T, CSV]](inSer)
        csvConv = converter
      }

      override def pushInElement(e: Either[T, CSV], logicalInputId: Int): Unit = {
        super.pushInElement(e, logicalInputId)
        if (logicalInputId == 0) { // input data
          if (!csvWriterInitiated) {
            buff.add(e)
          }
          else { // generate string from input
            out.collectElement(generateString(e))
          }
        }
        else { // CSV info
          csvInfo = e.right.get
          csvInfoInitiated = true
          csvWriter = CSVScalaSupport[T](csvInfo).writer()
          csvWriterInitiated = true
          // generate strings from buffered input
          val it = buff.iterator()
          while (it.hasNext) { out.collectElement(generateString(it.next())) }
        }
      }

      override def closeInBag(inputId: Int): Unit = {
        super.closeInBag(inputId)
        csvWriter.close()
        out.closeBag()
      }

      def generateString(e: Either[T, CSV]): String = {
        val rec = Array.ofDim[String](csvConv.size)
        csvConv.write(e.left.get, rec, 0)(csvInfo)
        csvWriter.writeRowToString(rec)
      }
    }
  }

  def writeString: BagOperator[String, scala.Unit] = {
    new FileSinkString
  }
}
