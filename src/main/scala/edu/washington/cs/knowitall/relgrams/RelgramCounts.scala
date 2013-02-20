package edu.washington.cs.knowitall.relgrams

import collection.{mutable, Map, Set}
import com.nicta.scoobi.core.WireFormat
import java.io.{DataInput, DataOutput}
import utils.MapUtils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 9:10 PM
 * To change this template use File | Settings | File Templates.
 */



import edu.washington.cs.knowitall.relgrams.utils.MapUtils._

object RelationTuple{
  val sep = "\t"
  def fromSerializedString(string:String):Option[RelationTuple] = {
    val splits = string.split("\t")
    if (splits.size > 3){
      val arg1 = splits(0)
      val rel = splits(1)
      val arg2 = splits(2)
      var hashes = splits(3).split(",").map(x => x.toInt).toSet
      var arg1HeadCounts = new mutable.HashMap[String, Int]()
      if (splits.size > 4) arg1HeadCounts ++= MapUtils.StringIntMapfromCountsString(splits(4))
      var arg2HeadCounts = new mutable.HashMap[String, Int]()
      if (splits.size > 5) arg2HeadCounts ++= MapUtils.StringIntMapfromCountsString(splits(5))
      Some(new RelationTuple(arg1, rel, arg2, hashes, arg1HeadCounts, arg2HeadCounts))
    }else{
      None
    }
  }
}
case class RelationTuple(arg1:String, rel:String, arg2:String,
                         var hashes:Set[Int],
                         var arg1HeadCounts:scala.collection.mutable.Map[String,Int],
                         var arg2HeadCounts:scala.collection.mutable.Map[String,Int]){

  def serialize:String = toString
  private def countsString(counts:Map[String, Int]):String = toCountsString(counts)//counts.toSeq.sortBy(ac => ac._2).map(ac => "%s(%d)".format(ac._1, ac._2)).mkString(",")
  def prettyString:String = "%s\t%s\t%s".format(arg1, rel, arg2)
  override def toString:String = "%s\t%s\t%s\t%s\t%s\t%s".format(arg1, rel, arg2,
                                                                 hashes.mkString(","),
                                                                 countsString(arg1HeadCounts.toMap), countsString(arg2HeadCounts.toMap))
}

object Relgram {
  val sep = "_RG_SEP_"
  def fromSerializedString(string:String):Option[Relgram] = {
    val splits = string.split(sep)
    if (splits.size > 1){
      (RelationTuple.fromSerializedString(splits(0)), RelationTuple.fromSerializedString(splits(1))) match {
        case (Some(first:RelationTuple), Some(second:RelationTuple)) => Some(new Relgram(first, second))
        case _ => None
      }
    }else{
      None
    }
  }

}
case class Relgram(first:RelationTuple, second:RelationTuple){

  import Relgram._
  def serialize: String = "%s%s%s".format(first.serialize, sep, second.serialize)
  def prettyString:String = "%s\t%s".format(first.prettyString, second.prettyString)
  override def toString:String = "%s\t%s".format(first, second)
}

object ArgCounts {
  def newMap = new mutable.HashMap[String, Int]
  def newInstance = new ArgCounts(newMap, newMap, newMap, newMap)

  def fromSerializedString(string:String):Option[ArgCounts] = {
    val splits = string.split("\t")
    if (splits.size >= 3){
      val firstArg1Counts = MapUtils.StringIntMutableMapfromCountsString(splits(0))
      val firstArg2Counts = MapUtils.StringIntMutableMapfromCountsString(splits(1))
      val secondArg1Counts = MapUtils.StringIntMutableMapfromCountsString(splits(2))
      val secondArg2Counts = MapUtils.StringIntMutableMapfromCountsString(splits(3))
      Some(new ArgCounts(firstArg1Counts, firstArg2Counts, secondArg1Counts, secondArg2Counts))
    } else {
      None
    }
  }
}
case class ArgCounts(firstArg1Counts:scala.collection.mutable.Map[String,Int],
                     firstArg2Counts:scala.collection.mutable.Map[String,Int],
                     secondArg1Counts:scala.collection.mutable.Map[String,Int],
                     secondArg2Counts:scala.collection.mutable.Map[String,Int]){



  def serialize:String = toString
  override def toString:String = "%s\t%s\t%s\t%s".format(toCountsString(firstArg1Counts.toMap), toCountsString(firstArg2Counts.toMap),
    toCountsString(secondArg1Counts.toMap), toCountsString(secondArg2Counts.toMap))
}



object RelgramCounts{
  def isDummy(rgc: RelgramCounts):Boolean = {
    rgc.relgram.first.arg1.equals("NA") &&  rgc.relgram.second.arg1.equals("NA")
  }


  val dummyTuple = new RelationTuple("NA", "NA", "NA", (0::Nil).toSet,
                                     new scala.collection.mutable.HashMap[String,Int](), new scala.collection.mutable.HashMap[String,Int]())
  val DummyRelgramCounts:RelgramCounts = new RelgramCounts(new Relgram(dummyTuple, dummyTuple), new mutable.HashMap[Int, Int](), ArgCounts.newInstance)
  implicit def RelgramCountsFmt = new WireFormat[RelgramCounts]{
    def toWire(x: RelgramCounts, out: DataOutput) {out.writeBytes(x.serialize + "\n")}
    def fromWire(in: DataInput): RelgramCounts = RelgramCounts.fromSerializedString(in.readLine()).getOrElse(DummyRelgramCounts)//.getOrElse(RelgramCounts.DummyRelgramCounts)
  }
  val sep = "_RGC_SEP_"
  def fromSerializedString(serializedString:String):Option[RelgramCounts] = {
    val splits = serializedString.split(sep)
    if (splits.size > 2){
      val relgramOption = Relgram.fromSerializedString(splits(0))
      val counts = deserializeCounts(splits(1))
      val argCountsOption = ArgCounts.fromSerializedString(splits(2))
      (relgramOption, argCountsOption) match {
        case (Some(relgram:Relgram), Some(argCounts:ArgCounts)) => Some(new RelgramCounts(relgram, counts , argCounts))
        case _ => {
          println("Relgram: " + relgramOption + " and ArgCountOption: " + argCountsOption)
          None
        }
      }
    }else{
      println("Failed to serialize from string: " + serializedString)
      None
    }
  }
  def serialize(counts:Map[Int, Int]) = MapUtils.toCountsString(counts)
  def deserializeCounts(countsString:String) = MapUtils.IntIntMutableMapfromCountsString(countsString)
}
case class RelgramCounts(relgram:Relgram, counts:scala.collection.mutable.Map[Int, Int], argCounts:ArgCounts){


  def prettyString:String = "%s\t%s\t%s".format(relgram.prettyString,
                                                 toCountsString(counts.toMap),
                                                 argCounts.toString)

  import RelgramCounts._
  def serialize:String = "%s%s%s%s%s".format(relgram.serialize, sep, toCountsString(counts.toMap), sep, argCounts.serialize)
  override def toString:String = "%s\t%s\t%s".format(relgram.toString,
                                                     toCountsString(counts.toMap),
                                                     argCounts.toString)

}
