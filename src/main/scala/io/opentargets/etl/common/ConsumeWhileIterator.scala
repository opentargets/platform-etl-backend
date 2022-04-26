package io.opentargets.etl.common

import scala.annotation.tailrec

/** ConsumeWhileIterator is a function to consume a whole iterator by applying a fn each time and
  * the result of each fn convert into a resulting Seq of objects B. An example could be
  * a lines iterator from a file and then grouping list of strings and each grouped list of strings
  * represent a single entity so getting a list of entities after the file is processed.
  * @param iter an iterator
  * @tparam A Source type of the iterator
  * @tparam B the resulted type of the object to be transformed
  */
class ConsumeWhileIterator[A, B](iter: Iterator[A]) {

  /** Method to partition an iterator based on function `fn` and then convert those partitions into a `Seq[B]`
    * @param fn is a method to collect a subset of the iterator, eg. `_.takeWhile(_.startsWith("hello"))`
    * @param convert takes the output of `fn` and reduces it to B.
    * @return all `B` created by `convert`
    */
  def consumeWhile(fn: Iterator[A] => Iterator[A])(convert: Seq[A] => B): Seq[B] = {

    @tailrec
    def doConsumeWhile(iter: Iterator[A], items: Seq[B])(
        fn: Iterator[A] => Iterator[A]
    )(convert: Seq[A] => B): Seq[B] = {
      if (iter.hasNext) {
        val newIter = fn(iter)
        val b = convert(newIter.toVector)
        doConsumeWhile(iter, items :+ b)(fn)(convert)
      } else items
    }

    doConsumeWhile(iter, Vector.empty[B])(fn)(convert)
  }
}

/** An implicit class to sugar the use of consumeWhile then an iterator object is on the left like
  * it.consumeWhile(...)(...)
  */
object ConsumeWhileIterator {
  implicit def toConsumeWhileIterator[A, B](itr: Iterator[A]): ConsumeWhileIterator[A, B] =
    new ConsumeWhileIterator(itr)
}
