package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 3:47 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._

import Numeric._
import collection.mutable

object MapUtils {

  def main(args:Array[String]){
    var map = new mutable.HashMap[String, Int]()
    updateCounts(map, "a", 1)
    println("Map: " + map.mkString(","))

  }
  def updateCounts[A, B](counts:scala.collection.mutable.Map[A, B], key:A, count:B)(implicit numeric: Numeric[B]){
    val updatedCount = counts.get(key) match {
      case Some(prev:B) => numeric.plus(prev, count)
      case None => count
    }
    counts += key -> updatedCount
  }
}
