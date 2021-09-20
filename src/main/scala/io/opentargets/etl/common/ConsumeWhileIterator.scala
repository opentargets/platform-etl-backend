package io.opentargets.etl.common

import scala.annotation.tailrec

class ConsumeWhileIterator[A, B](iter: Iterator[A]) {
    /**
    * Method to partition an iterator based on function `fn` and then convert those partitions into a `Seq[B]`
    * @param fn is a method to collect a subset of the iterator, eg. `_.takeWhile(_.startsWith("hello"))`
    * @param convert takes the output of `fn` and reduces it to B.
    * @return all `B` created by `convert`
    */
  def consumeWhile(fn: Iterator[A] => Iterator[A])(convert: Seq[A] => B): Seq[B] = {

    @tailrec
    def doConsumeWhile(iter: Iterator[A], items: Seq[B])(fn: Iterator[A] => Iterator[A])(
        convert: Seq[A] => B): Seq[B] = {
      if (iter.hasNext) {
        val newIter = fn(iter)
        val b = convert(newIter.toVector)
        doConsumeWhile(iter, items :+ b)(fn)(convert)
      } else items
    }

    doConsumeWhile(iter, Vector.empty[B])(fn)(convert)
  }
}

object ConsumeWhileIterator {
  implicit def toConsumeWhileIterator[A, B](itr: Iterator[A]): ConsumeWhileIterator[A, B] =
    new ConsumeWhileIterator(itr)
}
