package edu.washington.cs.knowitall.relgrams

import edu.washington.cs.knowitall.tool.coref.{StanfordCoreferenceResolver, Mention}

import io.Source
import collection.mutable.{HashMap, ArrayBuffer}
import collection.mutable
import org.slf4j.LoggerFactory
import com.nicta.scoobi.core.WireFormat
import java.io.{DataInput, DataOutput}

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/25/13
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */


object TuplesDocumentGenerator{

  val resolveWithTimeout = runWithTimeout(1000 * 100)
  val resolver = new StanfordCoreferenceResolver()
  def getPrunedDocument(docid: String, records: Seq[TypedTuplesRecord]): TuplesDocument = {
    val prunedSortedRecords = pruneRecordsAndIndex(records).sortBy(x => x._2).map(x => x._1)
    new TuplesDocument(docid, prunedSortedRecords)
  }

  def getPrunedTuplesDocumentWithCorefMentions(docid: String, records: Seq[TypedTuplesRecord]) = {
    val prunedSortedRecords = pruneRecordsAndIndex(records).sortBy(x => x._2).map(x => x._1)
    getTuplesDocumentWithCorefMentions(new TuplesDocument(docid, prunedSortedRecords))
  }
  def getTuplesDocumentWithCorefMentions(document:TuplesDocument):Option[TuplesDocumentWithCorefMentions] = {
    var offset = 0
    val sentencesWithOffsets = document.tupleRecords.iterator.map(record => {
      val curOffset = offset
      offset = offset + record.sentence.length + 1
      (record.sentence, curOffset)
    }).toList
   resolveWithTimeout(resolver.clusters(sentencesWithOffsets.map(x => x._1).mkString("\n"))) match {
     case Some(mentions:Map[Mention, List[Mention]]) => Some(new TuplesDocumentWithCorefMentions(document, sentencesWithOffsets.map(x => x._2), mentions))
     case None => {
       println("Timing out document: " + document.docid + ". No mentions added.")
       Some(new TuplesDocumentWithCorefMentions(document, sentencesWithOffsets.map(x => x._2), Map[Mention, List[Mention]]()))
       None
     }
   }

  }

  import scala.actors.Futures._
  def runWithTimeout(timeoutMs: Long)(f: => Map[Mention, List[Mention]]) : Option[Map[Mention, List[Mention]]] = {
    awaitAll(timeoutMs, future(f)).head.asInstanceOf[Option[Map[Mention, List[Mention]]]]
  }



  //Two relations are different as long as they are between different arguments.
  def areDifferentRelations(outer: TypedTuplesRecord, inner: TypedTuplesRecord): Boolean = {
    def sameArgs(x:TypedTuplesRecord, y:TypedTuplesRecord)     = x.arg1.equals(y.arg1) && x.arg2.equals(y.arg2)
    def switchedArgs(x:TypedTuplesRecord, y:TypedTuplesRecord) = x.arg1.equals(y.arg2) && x.arg2.equals(y.arg1)
    def subsumedArgs(x:TypedTuplesRecord, y:TypedTuplesRecord) = x.subsumesOrSubsumedBy(y)

    !sameArgs(outer, inner) && !switchedArgs(outer, inner) && !subsumedArgs(outer, inner)
  }


  //Filter typed tuples and sort them in the order of occurrence in text.
  def removeNonAlphaNumericRecords(sRecords:Seq[TypedTuplesRecord]):Seq[TypedTuplesRecord] = {
    val hasAlphabetOrDigitRe = """[a-zA-Z0-9]""".r
    def specialCharsOnly(string:String) = hasAlphabetOrDigitRe.findFirstIn(string) == None
    def isGoodExtraction(record: TypedTuplesRecord):Boolean = {
      !specialCharsOnly(record.arg1Head) &&
      !specialCharsOnly(record.relHead) &&
      !specialCharsOnly(record.arg2Head)
    }
    sRecords.filter(record => isGoodExtraction(record))
  }
  def sortValue(record: TypedTuplesRecord): Int = {
    record.sentid * 1000 + record.extrid
  }

  val beVerbs = ("be"::"is"::"was"::"are"::"were"::Nil).toSet
  def rewriteBeVerbs(text: String): String = text.split(" ").map(word => if(beVerbs.contains(word)) "be" else word).mkString(" ")

  val hasVerbs = ("has"::"have"::"had"::"having"::Nil).toSet
  def rewriteBeAndHasVerbs(text: String): String = {
    text.split(" ").map(word => if(hasVerbs.contains(word)) "had" else if(beVerbs.contains(word)) "be" else word).mkString(" ")
  }


  def isReportingVerbRelation(rel:String) = !reportingVerbs.intersect(rel.split(" ").toSet).isEmpty
  val reportingVerbs = ("say"::"said"::"saying"::"tell"::"told"::"speak"::"spoke"::Nil).toSet
  def isBeRelation(rel:String) = rel.equals("be")
  def isHadRelation(rel:String) = rel.equals("had") || rel.equals("had had")

  def removeReportingAndBeHadRelations(records: Seq[TypedTuplesRecord]): Seq[TypedTuplesRecord] = {
    records.filterNot(record => isReportingVerbRelation(record.relHead) || isBeRelation(record.relHead) || isHadRelation(record.relHead))
  }

  def removeMoreThanXWordRelations(records: Seq[TypedTuplesRecord], x:Int): Seq[TypedTuplesRecord] = records.filter(record => record.relHead.split(" ").size <= x)

  def findAndRemoveRedundantRecords(records: Seq[TypedTuplesRecord]):Seq[TypedTuplesRecord] = {

    val outRecords = new ArrayBuffer[TypedTuplesRecord]()
    records.iterator.copyToBuffer(outRecords)
    records.iterator.foreach(outer => {
      records.iterator.filter(inner => (inner.extrid != outer.extrid)).foreach(inner => {
        if(!areDifferentRelations(outer, inner)){
          val removeRecord = if((outer.arg1Head + outer.relHead + outer.arg2Head).size > (inner.arg1Head + inner.relHead + inner.arg2Head).size) {
            inner
          }else{
            outer
          }
          outRecords -= removeRecord
        }
      })
    })
    if(outRecords.size == records.size) return outRecords else findAndRemoveRedundantRecords(outRecords)
    return outRecords
  }

  def removeRedundantRecords(records: Seq[TypedTuplesRecord]): Seq[TypedTuplesRecord] = {
    val groupedRecords = new HashMap[Int, ArrayBuffer[TypedTuplesRecord]]()

    records.foreach(record => groupedRecords.getOrElseUpdate(record.sentid, new ArrayBuffer[TypedTuplesRecord]) += record)
    var outRecords = mutable.Seq[TypedTuplesRecord]()
    groupedRecords.keys.foreach(key => {
      val grecords = groupedRecords(key)
      if(grecords.size > 1){
        val prunedRecords = findAndRemoveRedundantRecords(grecords)
        outRecords ++= prunedRecords
      }else{
        outRecords ++= grecords
      }

    })
    return outRecords
  }

  def pruneRecordsAndIndex(sRecords:Seq[TypedTuplesRecord]):Seq[(TypedTuplesRecord, Int)] = {
    var ssRecords = removeNonAlphaNumericRecords(sRecords)
    ssRecords = removeMoreThanXWordRelations(ssRecords, 7)
    ssRecords = removeRedundantRecords(ssRecords)
    ssRecords.foreach(record => record.relHead = rewriteBeAndHasVerbs(record.relHead))
    return removeReportingAndBeHadRelations(ssRecords).sortBy(record => sortValue(record)).zipWithIndex
  }


}


object TuplesDocumentWithCorefMentions{
  val logger = LoggerFactory.getLogger(this.getClass)
  def fromString(tdmString: String): Option[TuplesDocumentWithCorefMentions] = {
    val splits = tdmString.split(tdocsep)
    if (splits.size == 3){
      TuplesDocument.fromString(splits(0)) match {
        case Some(tuplesDocument:TuplesDocument) => {
          val sentenceOffsets = splits(1).split(",").map(x => x.toInt).toList
          val mentions = MentionIO.fromMentionsMapString(splits(2))
          Some(new TuplesDocumentWithCorefMentions(tuplesDocument, sentenceOffsets, mentions))
        }
        case None => {
          println("Failed to construct TuplesDocument from string: " + splits(0))
          None
        }
      }
    }else{
      println("Splits size != 3, actual = %d. String:\n%s".format(splits.size, tdmString))
      None
    }
  }

  val tdocsep = "_TDOC_SEP_"

  implicit def TuplesDocumentWithCorefMentionsFmt = new WireFormat[TuplesDocumentWithCorefMentions]{
    def toWire(x: TuplesDocumentWithCorefMentions, out: DataOutput) {out.writeBytes(x.toString + "\n")}
    def fromWire(in: DataInput): TuplesDocumentWithCorefMentions = TuplesDocumentWithCorefMentions.fromString(in.readLine()).get
  }

}
object MentionIO{
  val msep = "_MSEP_"
  val valsep = "_VALSEP_"
  val keyvalsep = "_KEYVAL_SEP_"
  val mfieldsep = "_MFIELD_SEP_"

  def mentionString(mention:Mention) = "%s%s%s".format(mention.text, mfieldsep, mention.offset)
  def mentionsMapString(mentions: Map[Mention, List[Mention]]):String = mentions.map(mmlist => "%s%s%s".format(mentionString(mmlist._1), valsep, mmlist._2.map(m => mentionString(m)).mkString(msep))).mkString(keyvalsep)
  def fromMentionString(string:String) = {
    val splits = string.split(mfieldsep)
    if (splits.size == 2){
      Some( new Mention(splits(0), splits(1).toInt))
    }else{
      None
    }
  }
  def fromMentionsMapString(string:String) = {
    val keyvals = string.split(keyvalsep)
    keyvals.flatMap(keyval => {
      val splits = keyval.split(valsep)
      if (splits.size > 1){
        val keyMentionOption = fromMentionString(splits(0))
        keyMentionOption match {
          case Some(keyMention:Mention) => {
            val valueMentions = splits(1).split(msep).flatMap(mstring => fromMentionString(mstring)).toList
            Some(keyMention -> valueMentions)
          }
          case None => {
            None
          }
        }
      }else{
        None
      }

    }).toMap
  }
}
case class TuplesDocumentWithCorefMentions(tuplesDocument:TuplesDocument,
                                           sentenceOffsets:List[Int],
                                           mentions:Map[Mention, List[Mention]]){
  import TuplesDocumentWithCorefMentions._
  override def toString:String = {
    "%s%s%s%s%s".format(tuplesDocument.toString(), tdocsep, sentenceOffsets.mkString(","), tdocsep, MentionIO.mentionsMapString(mentions))
  }
}

object TuplesDocument{

  val logger = LoggerFactory.getLogger(this.getClass)

  val docidsep = "_DOCID_SEP_"
  val recsep = "_RECORD_SEP_"
  def fromString(string:String):Option[TuplesDocument] = {
    val splits = string.split(docidsep)
    if (splits.size == 2){
      val docid = splits(0)
      val recordsstring = splits(1)
      val records = recordsstring.split(recsep).flatMap(rec => TypedTuplesRecord.fromString(rec)).toSeq
      if(!records.isEmpty){
        Some(new TuplesDocument(docid, records))
      }else{
        logger.error("Failed to read document from line: " + string)
        println("Failed to read document from line: " + string)
        None
      }
    }else{
      logger.error("Failed to read document with splits size != 2. Actual = %d. Line:\n%s".format(splits.size, string))
      println("Failed to read document with splits size != 2. Actual = %d. Line:\n%s".format(splits.size, string))
      None
    }
  }
  implicit def TuplesDocumentFmt = new WireFormat[TuplesDocument]{
    def toWire(x: TuplesDocument, out: DataOutput) {out.writeBytes(x.toString + "\n")}
    def fromWire(in: DataInput): TuplesDocument = TuplesDocument.fromString(in.readLine()).get
  }
}

case class TuplesDocument (docid:String, tupleRecords:Seq[TypedTuplesRecord]) {
  import TuplesDocument._
  override def toString():String = "%s%s%s".format(docid, docidsep, tupleRecords.mkString(recsep))
}


object CorefDocumentTester{
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args:Array[String]){
    val tupleDocuments = Source.fromFile(args(0)).getLines()
                               .flatMap(line =>TypedTuplesRecord.fromString(line)).toSeq
                               .groupBy(record => record.docid)
                               .map(kv => TuplesDocumentGenerator.getPrunedDocument(kv._1, kv._2))

    println("Size of tupleDocuments: " + tupleDocuments.size)
    tupleDocuments.foreach(td => TuplesDocument.fromString(td.toString()) match {
      case Some(m:TuplesDocument) => println("Success.")
      case None => println("failure on tdm: " + td.docid)
    })
    val tdmSeq = tupleDocuments.flatMap(td => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(td))
    tdmSeq.foreach(tdm=> TuplesDocumentWithCorefMentions.fromString(tdm.toString()) match {
      case Some(tdm:TuplesDocumentWithCorefMentions) => println("Success 2.")
      case None => println("Failure on tdm2: " + tdm.tuplesDocument.docid)
    })

  }
}