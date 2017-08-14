package com.jrgv89.sparkApp.utils

import scala.collection.JavaConversions._

object Utils {
  /**
    * transform any object in seq[T]
    *
    * @param o {Any} scala.collection.Seq[T] | java.util.ArrayList[T]
    * @tparam T {T} String
    * @return Seq[T]
    */
  def toSeq[T](o: Any): Seq[T] = {
    o match {
      case _: java.util.ArrayList[T] => o.asInstanceOf[java.util.ArrayList[T]].toSeq
      case _: scala.collection.Seq[T] => o.asInstanceOf[Seq[T]]
    }
  }

  /**
    * transform any object in seq[T,T]
    *
    * @param o {Any} java.util.Map[T, T]
    * @tparam T {T} String
    * @return Map[T,T]
    */
  def toMap[T](o: Any): Map[T, T] = {
    o match {
      case _: java.util.Map[T, T] => o.asInstanceOf[java.util.Map[T, T]].toMap
      case _ => o.asInstanceOf[Map[T, T]]
    }
  }

  /**
    * method that measure time in execute f
    *
    * @param f {T} method to measure
    * @tparam T {T} generic
    * @return {(Double, Any)} time lapsed in execute f and result
    */
  def time[T](f: => T): (Double, Any) = {
    val start = System.nanoTime
    val result = f
    //TODO review this comment
    //    println(s"$s time: " + ((System.nanoTime - start) * 1e-9) + " s")
    ((System.nanoTime - start) * 1e-9, result)
  }
}
