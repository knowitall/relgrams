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
import collection.{Map, mutable}

object MapUtils {

  def IntIntMutableMapfromCountsString(countsString: String, sep:String=":") = {
    val mutableMap = new mutable.HashMap[Int, Int]()
    mutableMap ++ IntIntMapfromCountsString(countsString, sep)
    mutableMap
  }

  def StringIntMutableMapfromCountsString(countsString: String, sep:String=":") = {
    val mutableMap = new mutable.HashMap[String, Int]()
    mutableMap ++= StringIntMapfromCountsString(countsString, sep)
    mutableMap
  }



  def IntIntMapfromCountsString(countsString:String, sep:String=":"):Map[Int, Int] = countsString.split(",").map(x => {
    val kv = x.split(sep)
    (kv(0).toInt -> kv(1).toInt)
  }).toMap

  def StringIntMapfromCountsString(countsString:String, sep:String=":"):Map[String, Int] = countsString.split(",").flatMap(x => {
    val kv = x.split(sep)
    if (kv.size > 1){
      Some((kv(0) -> kv(1).toInt))
    }else{
      None
    }
  }).toMap


  def toCountsString[A](counts:Map[A, Int], sep:String=":")(implicit ordering:Ordering[A]) = counts.toSeq.sortBy(x => x._1).map(wc => wc._1 + sep + wc._2).mkString(",")
  //def toCountsString[A](counts:Map[A, Int])(implicit ordering:Ordering[A]) = counts.toSeq.sortBy(x => x._1).map(wc => wc._1 + ":" + wc._2).mkString(",")

  def addTo[A, B](addWith: mutable.Map[A, B], toAdd: mutable.Map[A, B])(implicit numeric: Numeric[B]){
    toAdd.map(kv => updateCounts(addWith, kv._1, kv._2))
  }

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
