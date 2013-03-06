package edu.washington.cs.knowitall.relgrams

import collection.{mutable, Map, Set}
import com.nicta.scoobi.core.WireFormat
import java.io.{DataInput, DataOutput}
import utils.MapUtils
import util.matching.Regex
import util.matching.Regex.Match

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 9:10 PM
 * To change this template use File | Settings | File Templates.
 */


import edu.washington.cs.knowitall.relgrams.utils.MapUtils



object RelationTupleCounts{
  val sep = "\t"
  val lastTabRe = """(.*)\t([0-9]+)""".r
  def fromSerializedString(string:String):Option[RelationTupleCounts] = {
    lastTabRe.findFirstMatchIn(string) match {
      case Some(m:Regex.Match) => {
        val tupleString = m.group(1)
        val count = m.group(2).toInt
        RelationTuple.fromSerializedString(tupleString) match {
          case Some(tuple:RelationTuple) => {
            Some(new RelationTupleCounts(tuple, count))
          }
          case None => None
        }
      }
      case None => None
    }
  }

  val dummyTupleCount = new RelationTupleCounts(RelationTuple.dummyTuple, 0)
  implicit def RelationTupleCountsFmt = new WireFormat[RelationTupleCounts]{
    def toWire(x: RelationTupleCounts, out: DataOutput) {out.writeBytes(x.toString + "\n")}
    def fromWire(in: DataInput): RelationTupleCounts = RelationTupleCounts.fromSerializedString(in.readLine()).getOrElse(dummyTupleCount)//.getOrElse(RelgramCounts.DummyRelgramCounts)
  }
}
case class RelationTupleCounts(tuple:RelationTuple, var count:Int){
  override def toString():String = "%s\t%d".format(tuple.serialize, count)
}

object RelationTuple{
  val sep = "\t"
  val SENT_SEP="_SENT_SEP_"
  val ID_SEP = ","
  def fromSerializedString(string:String):Option[RelationTuple] = {
    val splits = string.split("\t")
    if (splits.size > 5){
      val iterator = splits.iterator
      val arg1 = iterator.next
      val rel = iterator.next
      val arg2 = iterator.next
      var hashes = iterator.next.split(ID_SEP).map(x => x.toInt).toSet
      var sentences = iterator.next.split(SENT_SEP).toSet
      var ids = iterator.next.split(ID_SEP).toSet
      var arg1HeadCounts = new mutable.HashMap[String, Int]()
      if (iterator.hasNext) arg1HeadCounts ++= MapUtils.StringIntMapfromSafeCountsString(iterator.next)
      var arg2HeadCounts = new mutable.HashMap[String, Int]()
      if (iterator.hasNext) arg2HeadCounts ++= MapUtils.StringIntMapfromSafeCountsString(iterator.next)
      Some(new RelationTuple(arg1, rel, arg2, hashes, sentences, ids, arg1HeadCounts, arg2HeadCounts))
    }else{
      println("Splits size for relation tuple: " + splits.size + " = " + splits.mkString("_RT_"))
      None
    }
  }

  val dummyTuple = new RelationTuple("NA", "NA", "NA", Set(0), Set("NA"), Set("NA"),
                                    new scala.collection.mutable.HashMap[String,Int](), new scala.collection.mutable.HashMap[String,Int]())

  implicit def RelationTupleFmt = new WireFormat[RelationTuple]{
    def toWire(x: RelationTuple, out: DataOutput) {out.writeBytes(x.serialize + "\n")}
    def fromWire(in: DataInput): RelationTuple = RelationTuple.fromSerializedString(in.readLine()).getOrElse(dummyTuple)
  }



  //This is probably extreme!
  //val beVerbPPRemoveRe = """be (.*?) (.+$)""".r
  val beRemoveRe = """be (.*)""".r

  def cleanRelString(rel:String): String = rel.replaceAll("""^be """, "")/**beRemoveRe.findFirstMatchIn(rel) match {
    case Some(m:Match) => m.group(1)
    case None => rel.replaceAll(be """, "")
  }*/

  def setSubsumption(awords: Array[String], bwords: Array[String]): Boolean = {
    awords.filter(a => !a.equals("be")).toSet.subsetOf(bwords.filter(b => !b.equals("be")).toSet)
  }


}

case class RelationTuple(arg1:String, rel:String, arg2:String,
                         var hashes:Set[Int],
                         var sentences:Set[String],
                         var ids:Set[String],
                         var arg1HeadCounts:scala.collection.mutable.Map[String,Int],
                         var arg2HeadCounts:scala.collection.mutable.Map[String,Int]){

  import RelationTuple._
  def serialize:String = toString
  private def countsString(counts:Map[String, Int]):String = MapUtils.toSafeCountsString(counts)//, COUNTS_SEP)
  def prettyString:String = "%s\t%s\t%s".format(arg1, rel, arg2)
  override def toString:String = "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(arg1, rel, arg2,
                                                                 hashes.mkString(ID_SEP),
                                                                 sentences.mkString(SENT_SEP),
                                                                 ids.mkString(ID_SEP),
                                                                 countsString(arg1HeadCounts.toMap),
                                                                 countsString(arg2HeadCounts.toMap))

  def normTupleString(): String = {
    arg1 + " " + cleanRelString(rel) + " " + arg2
  }

  def subsumesOrSubsumedBy(that:RelationTuple) = {
    val thisString = this.normTupleString()
    val thatString = that.normTupleString()

    thisString.contains(thatString) ||
    thatString.contains(thisString) ||
    setSubsumption(thisString.split(" "), thatString.split(" "))
  }
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
  def prettyString:String = "%s\t%s\t%s\t%s".format(first.prettyString, second.prettyString, first.ids.mkString(","), second.ids.mkString(","))//, first.sentences.take(1).mkString(","), second.sentences.take(1).mkString(","))
  override def toString:String = "%s\t%s".format(first, second)
}

object ArgCounts {
  def newMap = new mutable.HashMap[String, Int]
  def newInstance = new ArgCounts(newMap, newMap, newMap, newMap)

  def fromSerializedString(string:String):Option[ArgCounts] = {
    val splits = string.split("\t")
    if (splits.size >= 3){
      val firstArg1Counts = MapUtils.StringIntMutableMapfromSafeCountsString(splits(0))
      val firstArg2Counts = MapUtils.StringIntMutableMapfromSafeCountsString(splits(1))
      val secondArg1Counts = MapUtils.StringIntMutableMapfromSafeCountsString(splits(2))
      val secondArg2Counts = MapUtils.StringIntMutableMapfromSafeCountsString(splits(3))
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
  override def toString:String = "%s\t%s\t%s\t%s".format(MapUtils.toSafeCountsString(firstArg1Counts.toMap),
                                                         MapUtils.toSafeCountsString(firstArg2Counts.toMap),
                                                         MapUtils.toSafeCountsString(secondArg1Counts.toMap),
                                                         MapUtils.toSafeCountsString(secondArg2Counts.toMap))
}



object RelgramCounts{

  def isDummy(rgc: RelgramCounts):Boolean = {
    rgc.relgram.first.arg1.equals("NA") &&  rgc.relgram.second.arg1.equals("NA")
  }
  import RelationTuple._
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
  def serializeCounts(counts:Map[Int, Int]) = MapUtils.toIntIntCountsString(counts)
  def deserializeCounts(countsString:String) = MapUtils.IntIntMutableMapfromCountsString(countsString)
}

case class RelgramCounts(relgram:Relgram, counts:scala.collection.mutable.Map[Int, Int], argCounts:ArgCounts){


  import RelgramCounts._

  def prettyString:String = "%s\t%s\t%s".format(relgram.prettyString,
                                                MapUtils.toIntIntCountsString(counts.toMap),
                                                argCounts.toString)

  def serialize:String = "%s%s%s%s%s".format(relgram.serialize, sep, serializeCounts(counts.toMap), sep, argCounts.serialize)

  override def toString:String = "%s\t%s\t%s".format(relgram.toString,
                                                     MapUtils.toIntIntCountsString(counts.toMap),
                                                     argCounts.toString)

}
