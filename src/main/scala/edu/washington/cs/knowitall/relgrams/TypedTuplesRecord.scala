package edu.washington.cs.knowitall.relgrams

import com.nicta.scoobi.core.WireFormat
import java.io.{DataInput, DataOutput}
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 10:12 PM
 * To change this template use File | Settings | File Templates.
 */


object TypedTuplesRecord{

  val logger = LoggerFactory.getLogger(this.getClass)

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
      val arg1 = nextString
      val rel = nextString
      val arg2 = nextString
      val arg1Head = nextString
      val relHead = nextString
      val arg2Head = nextString
      val arg1Types = nextString.split(",")
      val arg2Types = nextString.split(",")

      Some(new TypedTuplesRecord(docid, sentid, sentence, extrid, hashes,
        arg1, rel, arg2,
        arg1Head, relHead, arg2Head,
        arg1Types, arg2Types))
    }else{
      logger.error("Failed to read TypedTuplesRecord from string: " + string)
      logger.error("String has only %d splits. Expected at least %d".format(splits.size, 10))
      None
    }
  }

  implicit def TypedTuplesRecordFmt = new WireFormat[TypedTuplesRecord]{
    def toWire(x: TypedTuplesRecord, out: DataOutput) {out.writeBytes(x.toString + "\n")}
    def fromWire(in: DataInput): TypedTuplesRecord = TypedTuplesRecord.fromString(in.readLine()).get//.getOrElse(RelgramCounts.DummyRelgramCounts)
  }

}

case class TypedTuplesRecord(docid:String, sentid:Int, sentence:String, extrid:Int, hashes:Set[Int],
                             arg1:String, rel:String, arg2:String,
                             arg1Head:String, var relHead:String, arg2Head:String,
                             arg1Types:Seq[String], arg2Types:Seq[String]){


  override def toString:String = "%s\t%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s".format(docid, sentid, sentence, extrid, hashes.mkString(","),
  arg1, rel, arg2,
  arg1Head, relHead, arg2Head,
  arg1Types.mkString(","), arg2Types.mkString(","))
  //check if 'this' current record is within a window distance from 'that'
  def isWithinWindow(that:TypedTuplesRecord, window:Int): Boolean = {
    (this.extrid > that.extrid) && ((this.extrid - that.extrid) <= window)
    //(inner.eid > outer.eid) && ((inner.sentenceid - outer.sentenceid) < eid)
  }
  //This is probably extreme!
  val beVerbPPRemoveRe = """be (.*?) (.+$)""".r
  val beRemoveRe = """be (.*)""".r
  def cleanRelString(rel:String): String = {
    var m = beVerbPPRemoveRe.findFirstMatchIn(rel)
    if ( m != None) {
      m.get.group(1)
    } else {
      rel.replaceAll("""^be """, "")
    }
  }
  def normTupleString(): String = {
    arg1Head + " " + cleanRelString(relHead) + " " + arg2Head
  }

  def setSubsumption(awords: Array[String], bwords: Array[String]): Boolean = {
    awords.toSet.subsetOf(bwords.toSet)
  }
  def subsumes(that: TypedTuplesRecord):Boolean = {
    val thisString = this.normTupleString()
    val thatString = that.normTupleString()
    val subsumesVal = thisString.contains(thatString) ||
      thatString.contains(thisString) ||
      setSubsumption(thisString.split(" "), thatString.split(" "))
    return subsumesVal

  }
}
