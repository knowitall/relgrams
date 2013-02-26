package edu.washington.cs.knowitall.relgrams

import edu.washington.cs.knowitall.tool.coref.{StanfordCoreferenceResolver, Mention}

import io.Source
import collection.mutable.{HashMap, ArrayBuffer}
import collection.mutable
import org.slf4j.LoggerFactory

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/25/13
 * Time: 3:57 PM
 * To change this template use File | Settings | File Templates.
 */


object TuplesDocumentGenerator{

  val resolver = new StanfordCoreferenceResolver()
  def getPrunedDocument(docid: String, records: Seq[TypedTuplesRecord]): TuplesDocument = {
    val prunedSortedRecords = pruneRecordsAndIndex(records).sortBy(x => x._2).map(x => x._1)
    new TuplesDocument(docid, prunedSortedRecords)
  }

  def getTuplesDocumentWithCorefMentions(document:TuplesDocument) = {
    var offset = 0
    val sentencesWithOffsets = document.tupleRecords.iterator.map(record => {
      val curOffset = offset
      offset = offset + record.sentence.length + 1
      (record.sentence, curOffset)
    }).toSeq
    val mentions:Map[Mention, List[Mention]] = resolver.clusters(sentencesWithOffsets.map(x => x._1).mkString("\n"))
    new TuplesDocumentWithCorefMentions(document, mentions)
  }


  //Two relations are different as long as they are between different arguments.
  def areDifferentRelations(outer: TypedTuplesRecord, inner: TypedTuplesRecord): Boolean = {
    if (outer.arg1.equals(inner.arg1) &&
      outer.arg2.equals(inner.arg2)){
      //println("args are exactly the same: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    if (outer.arg1.equals(inner.arg2) &&
      outer.arg2.equals(inner.arg1)){// && relationsAreMostlySame(inner.relHead, outer.relHead)){
      //println("args are inverted but relations are same: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    if(inner.subsumes(outer) || outer.subsumes(inner)){
      println("subsumes: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    return true
  }


  //Filter typed tuples and sort them in the order of occurrence in text.
  def removeNonAlphaNumericRecords(sRecords:Seq[TypedTuplesRecord]):Seq[TypedTuplesRecord] = {

    val hasAlphabetOrDigitRe = """[a-zA-Z0-9]""".r
    def specialCharsOnly(string:String) = hasAlphabetOrDigitRe.findFirstIn(string) == None
    def isGoodExtraction(record: TypedTuplesRecord):Boolean = {

      if(specialCharsOnly(record.arg1Head)) return false
      if(specialCharsOnly(record.rel)) return false
      if(specialCharsOnly(record.arg2Head)) return false

      return true
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
        if(areDifferentRelations(outer, inner) == false){

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
  val tdocsep = "_TDOC_SEP_"
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
            val valueMentions = splits(1).split(msep).flatMap(mstring => fromMentionString(mstring))
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
case class TuplesDocumentWithCorefMentions(tuplesDocument:TuplesDocument, mentions:Map[Mention, List[Mention]]){
  import TuplesDocumentWithCorefMentions._


  override def toString():String = "%s%s%s".format(tuplesDocument.toString(), tdocsep, MentionIO.mentionsMapString(mentions))
}

object TuplesDocument{
  val recsep = "_RECORD_SEP_"
  def fromString(string:String):Seq[TypedTuplesRecord] = string.split(recsep).flatMap(rec => TypedTuplesRecord.fromString(rec))
}

case class TuplesDocument (docid:String, tupleRecords:Seq[TypedTuplesRecord]) {
  import TuplesDocument._
  override def toString():String = tupleRecords.mkString(recsep)
}


object CorefDocumentTester{
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args:Array[String]){
    val tupleDocuments = Source.fromFile(args(0)).getLines()
                               .flatMap(line =>TypedTuplesRecord.fromString(line)).toSeq
                               .groupBy(record => record.docid)
                               .map(kv => TuplesDocumentGenerator.getPrunedDocument(kv._1, kv._2))

    //val tdmSeq = tupleDocuments.map(td => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(td))
    //tdmSeq.foreach(tdm=> logger.info(tdm.toString()))
    tupleDocuments.foreach(td => TuplesDocument.fromString(td.toString()) match {
      case Some(m:TuplesDocument) => logger.info("Success.")
      case None => logger.info("failure on tdm: " + td.docid)
    })
  }
}