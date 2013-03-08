package edu.washington.cs.knowitall.relgrams

import com.nicta.scoobi.core.WireFormat
import java.io.{DataInput, DataOutput}
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.collection.immutable.Interval
import util.matching.Regex.Match
import io.Source

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 10:12 PM
 * To change this template use File | Settings | File Templates.
 */


object TypedTuplesRecord{

  def main(args:Array[String]){
    val testFile = args(0)
    Source.fromFile(testFile).getLines.flatMap(line => {
      val recordOption = fromString(line)
      recordOption match {
        case Some(record:TypedTuplesRecord) => {
          println("Successful record: " + record)

        }
        case None => {
          println("Failed to read record from line: " + line)
        }
      }
      recordOption
    }).foreach(record => {
      val string = record.toString
      TypedTuplesRecord.fromString(string) match {
        case Some(x:TypedTuplesRecord) => println("Success.")
        case None => println("Failed to deserialize from serialized string: " + string)
      }
    })

  }
  val logger = LoggerFactory.getLogger(this.getClass)
  val span_sep = "_SPAN_"
  val textSpanRe = """(.*?)%s(.*?)%s(.*?)""".format(span_sep, "-").r
  def textSpan(text:String, interval:Interval) = "%s%s%s-%s".format(text, span_sep, interval.start, interval.end)
  def fromTextSpan(textSpan:String):Option[(String, Interval)] = {
    if (textSpanRe.findFirstMatchIn(textSpan) != None){
      val textSpanRe(x:String, s:String, e:String) = textSpan
      Some((x, Interval.open(s.toInt, e.toInt)))
    }else{
      println("Failed to extract from textspan: " + textSpan)
      None
    }
  }
  //docid sentid sentence extrid origtuple headtuple arg1types arg2types
  def fromString(string: String): Option[TypedTuplesRecord] = {
    val splits = string.split("\t")
    if (splits.size > 9){
      var i = 0
      def nextString = {
        val out = if (splits.size > i) splits(i) else ""
        i = i + 1
        out
      }
      val docid = nextString
      val sentid = nextString.toInt
      val sentence = nextString
      val extrid = nextString.toInt
      val hashes = nextString.split(",").map(x => x.toInt).toSet
      val (arg1:String, arg1Interval:Interval) = fromTextSpan(nextString).get
      val (rel:String, relInterval:Interval) = fromTextSpan(nextString).get
      val (arg2:String, arg2Interval:Interval) = fromTextSpan(nextString).get
      val (arg1Head:String, arg1HeadInterval:Interval) = fromTextSpan(nextString).get
      val (relHead:String, relHeadInterval:Interval) = fromTextSpan(nextString).get
      val (arg2Head:String, arg2HeadInterval:Interval) = fromTextSpan(nextString).get
      val arg1Types = nextString.split(",")
      val arg2Types = nextString.split(",")

      Some(new TypedTuplesRecord(docid, sentid, sentence, extrid, hashes,
        arg1, arg1Interval, rel, relInterval, arg2, arg2Interval,
        arg1Head, arg1HeadInterval, relHead, relHeadInterval, arg2Head, arg2HeadInterval,
        arg1Types, arg2Types))
    }else{
      println("Failed to read TypedTuplesRecord from string: " + string)
      println("String has only %d splits. Expected at least %d".format(splits.size, 10))
      logger.error("Failed to read TypedTuplesRecord from string: " + string)
      logger.error("String has only %d splits. Expected at least %d".format(splits.size, 10))
      None
    }
  }

  implicit def TypedTuplesRecordFmt = new WireFormat[TypedTuplesRecord]{
    def toWire(x: TypedTuplesRecord, out: DataOutput) {out.writeUTF(x.toString)}
    def fromWire(in: DataInput): TypedTuplesRecord = {TypedTuplesRecord.fromString(in.readUTF()).get}
  }

}

case class TypedTuplesRecord(docid:String, sentid:Int, sentence:String, extrid:Int, hashes:Set[Int],
                             arg1:String, arg1Interval:Interval, rel:String, relInterval:Interval, arg2:String, arg2Interval:Interval,
                             arg1Head:String, arg1HeadInterval:Interval, var relHead:String, var relHeadInterval:Interval, arg2Head:String, arg2HeadInterval:Interval,
                             arg1Types:Seq[String], arg2Types:Seq[String]){


  import TypedTuplesRecord._
  override def toString:String = "%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(docid, sentid, sentence, extrid, hashes.mkString(","),
  textSpan(arg1, arg1Interval), textSpan(rel, relInterval), textSpan(arg2, arg2Interval),
  textSpan(arg1Head, arg1HeadInterval), textSpan(relHead, relHeadInterval), textSpan(arg2Head, arg2HeadInterval),
  arg1Types.mkString(","), arg2Types.mkString(","))

  //This is probably extreme!
  val beVerbPPRemoveRe = """be (.*?) (.+$)""".r
  val beRemoveRe = """be (.*)""".r

  def cleanRelString(rel:String): String = beVerbPPRemoveRe.findFirstMatchIn(rel) match {
    case Some(m:Match) => m.group(1)
    case None => rel.replaceAll("""^be """, "")
  }

  def normTupleString(): String = {
    arg1Head + " " + cleanRelString(relHead) + " " + arg2Head
  }

  def setSubsumption(awords: Array[String], bwords: Array[String]): Boolean = {
    awords.toSet.subsetOf(bwords.toSet)
  }
  def subsumesOrSubsumedBy(that:TypedTuplesRecord) = {
    val thisString = this.normTupleString()
    val thatString = that.normTupleString()
    thisString.contains(thatString) ||
    thatString.contains(thisString) ||
    setSubsumption(thisString.split(" "), thatString.split(" "))
  }
}
