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
import collection.{mutable, Map}

object MapUtils {


  val esep = "_ESEP_"
  val csep = "_CSEP_"


  def IntIntMutableMapfromCountsString(countsString: String) = {
    val mutableMap = new mutable.HashMap[Int, Int]()
    mutableMap ++= IntIntMapfromCountsString(countsString)
    mutableMap
  }

  def StringIntMutableMapfromSafeCountsString(countsString: String) = {
    val mutableMap = new mutable.HashMap[String, Int]()
    mutableMap ++= StringIntMapfromSafeCountsString(countsString)
    mutableMap
  }

  def IntIntMapfromCountsString(countsString:String):Map[Int, Int] = IntIntMapfromCountsString(countsString, ",", ":")
  def toIntIntCountsString(counts: Map[Int, Int]) = toCountsString(counts, ",", ":")

  def IntIntMapfromCountsString(countsString:String, esep:String, csep:String):Map[Int, Int] = countsString.split(esep).map(x => {
    val kv = x.split(csep)
    (kv(0).toInt -> kv(1).toInt)
  }).toMap


  def StringIntMapfromSafeCountsString(countsString:String):Map[String, Int] = StringIntMapfromCountsString(countsString, esep, csep)

  def StringIntMapfromCountsString(countsString:String, esep:String, csep:String):Map[String, Int] = countsString.split(esep).flatMap(x => {
    val kv = x.split(csep)
    if (kv.size > 1){
      Some((kv(0) -> kv(1).toInt))
    }else{
      None
    }
  }).toMap

  def toSafeCountsString[A](counts:Map[A, Int])(implicit ordering:Ordering[A]):String = toCountsString(counts, esep, csep)

  def toCountsString[A](counts:Map[A, Int], esep:String, csep:String)(implicit ordering:Ordering[A]):String = counts.toSeq.sortBy(x => x._1).map(wc => wc._1 + csep + wc._2).mkString(esep)

  def addToUntilSize[A,B](addWith: mutable.Map[A, B], toAdd: mutable.Map[A, B], maxSize: Int)(implicit numeric: Numeric[B]) = {
    if (addWith.keys.size < maxSize) addTo[A,B](addWith, toAdd)
  }

  def addTo[A, B](addWith: mutable.Map[A, B], toAdd: mutable.Map[A, B])(implicit numeric: Numeric[B]){
    toAdd.map(kv => updateCounts(addWith, kv._1, kv._2))
  }



  def main(args:Array[String]){
    var map = new mutable.HashMap[String, Int]()
    updateCounts(map, "a", 1)
    println("Map:%s".format(toSafeCountsString(map)))
    updateCounts(map, "a", 2)
    println("Map:%s".format(toSafeCountsString(map)))
    updateCounts(map, "b", 2)
    addTo(map, map)
    println("Map:%s".format(toCountsString(map, ",", ":")))

  }
  def updateCounts[A, B](counts:scala.collection.mutable.Map[A, B], key:A, count:B)(implicit numeric: Numeric[B]){
    val updatedCount = counts.get(key) match {
      case Some(prev:B) => numeric.plus(prev, count)
      case None => count
    }
    counts += key -> updatedCount
  }
}
