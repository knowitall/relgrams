package edu.washington.cs.knowitall.relgrams

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 9:06 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._

import collection.mutable.{ArrayBuffer, HashMap}
import collection.mutable
import io.Source
import java.io.PrintWriter
import org.slf4j.LoggerFactory
import scala.Predef._
import scala.{collection, Some}
import edu.washington.cs.knowitall.tool.coref.{Mention, StanfordCoreferenceResolver}
import scala.collection


class RelgramsExtractor(maxWindow:Int, equality:Boolean, noequality:Boolean) {


  val logger = LoggerFactory.getLogger(this.getClass)

  //val resolver = new StanfordCoreferenceResolver()



  def relationTupleKey(arg1: String, rel: String, arg2: String): String = "%s\t%s\t%s".format(arg1, rel, arg2)
  def relationTupleKey(relationTuple:RelationTuple) = "%s\t%s\t%s".format(relationTuple.arg1, relationTuple.rel, relationTuple.arg2)
  def relgramKey(first: RelationTuple, second: RelationTuple): String = "%s\t%s".format(relationTupleKey(first), relationTupleKey(second))

  def updateArgCounts(argCounts: ArgCounts,
                      firstArg1Head: String, firstArg2Head: String,
                      secondArg1Head: String, secondArg2Head: String){

    import edu.washington.cs.knowitall.relgrams.utils.MapUtils._
    updateCounts(argCounts.firstArg1Counts, firstArg1Head, 1)
    updateCounts(argCounts.firstArg2Counts, firstArg2Head, 1)
    updateCounts(argCounts.secondArg1Counts, secondArg1Head, 1)
    updateCounts(argCounts.secondArg2Counts, secondArg2Head, 1)
  }


  def addToSentences(relationTuple: RelationTuple, sentence: String) {
    relationTuple.sentences += sentence
    if (relationTuple.sentences.size > 5){
      relationTuple.sentences = relationTuple.sentences.drop(1)
    }
  }
  import edu.washington.cs.knowitall.relgrams.utils.MapUtils._
  def extractRelgramsFromDocument(document:TuplesDocumentWithCorefMentions)={
    val docid = document.tuplesDocument.docid
    val prunedRecords = document.tuplesDocument.tupleRecords.zipWithIndex
    val mentions = document.mentions
    val sentencesWithOffsets = document.sentenceOffsets
    var relgramCountsMap = new mutable.HashMap[String, RelgramCounts]()
    var relationTuplesMap = new mutable.HashMap[String, RelationTuple]()
    prunedRecords.iterator.foreach(outerIndex => {
      val outer = outerIndex._1
      val oindex = outerIndex._2
      val outerStartOffset = sentencesWithOffsets(oindex)
      val outerArg1s = (outer.arg1Head::Nil ++ outer.arg1Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val outerArg2s = (outer.arg2Head::Nil ++ outer.arg2Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val orel = outer.relHead
      var outerSentences = new mutable.HashSet[String]()
      prunedRecords.iterator.filter(innerIndex => (innerIndex._2 > oindex) && (innerIndex._2 <= oindex+maxWindow)).foreach(innerIndex => {
        val iindex = innerIndex._2
        val countWindow = iindex-oindex
        val inner = innerIndex._1
        val innerStartOffset = sentencesWithOffsets(iindex)
        val innerArg1s = (inner.arg1Head::Nil ++ inner.arg1Types).filter(oa1 => !oa1.trim.isEmpty).toSet
        val innerArg2s = (inner.arg2Head::Nil ++ inner.arg2Types).filter(oa1 => !oa1.trim.isEmpty).toSet
        var innerSentences = new mutable.HashSet[String]()
        outerArg1s.foreach(oa1 => {
          outerArg2s.foreach(oa2 => {
            val first = relationTuplesMap.getOrElseUpdate(relationTupleKey(oa1, orel, oa2), new RelationTuple(oa1, orel, oa2, outer.hashes, outerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
            addToSentences(first, outer.sentence)
            addToArgCounts(first.arg1HeadCounts, outer.arg1Head)
            addToArgCounts(first.arg2HeadCounts, outer.arg2Head)
          })
        })

        val irel = inner.relHead
        val corefArgs:Option[(String, String, String, String)] = equality match {
          case true => CoreferringArguments.coreferringArgs(outer, outerStartOffset, inner, innerStartOffset, mentions)
          case false => None
        }
        outerArg1s.foreach(outerArg1 => {
          outerArg2s.foreach(outerArg2 => {
            val first = relationTuplesMap.getOrElseUpdate(relationTupleKey(outerArg1, orel, outerArg2), new RelationTuple(outerArg1, orel, outerArg2, outer.hashes, outerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
            innerArg1s.foreach(innerArg1 => {
              innerArg2s.foreach(innerArg2 => {
                val second = relationTuplesMap.getOrElseUpdate(relationTupleKey(innerArg1, irel, innerArg2), new RelationTuple(innerArg1, irel, innerArg2, inner.hashes, innerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
                noequality match {
                  case true => addToRelgramCounts(second, inner, relgramCountsMap, first, countWindow, outer)
                  case false =>
                }
                corefArgs match {
                  case Some(resolvedArgs:(String, String, String, String)) => {
                    extractEqualityRelgrams(resolvedArgs,
                                            outerArg1, outerArg2, innerArg1, innerArg2,
                                            relationTuplesMap,
                                            orel, outer, outerSentences, irel, inner, innerSentences,
                                            relgramCountsMap, countWindow)
                  }
                  case None =>
                }
              })
            })
          })
        })


      })
    })
    relgramCountsMap.toMap

  }


  def addToRelgramCounts(second: RelationTuple, inner: TypedTuplesRecord, relgramCountsMap: HashMap[String, RelgramCounts], first: RelationTuple, countWindow: Int, outer: TypedTuplesRecord) {
    addToArgCounts(second.arg1HeadCounts, inner.arg1Head)
    addToArgCounts(second.arg2HeadCounts, inner.arg2Head)
    addToSentences(second, inner.sentence)
    val rgc = relgramCountsMap.getOrElseUpdate(relgramKey(first, second),
      new RelgramCounts(new Relgram(first, second),
        new scala.collection.mutable.HashMap[Int, Int],
        ArgCounts.newInstance))
    updateCounts(rgc.counts, countWindow, 1)
    updateArgCounts(rgc.argCounts, outer.arg1Head, outer.arg2Head, inner.arg1Head, inner.arg2Head)
  }

  def extractEqualityRelgrams(resolvedArgs: (String, String, String, String),
                              outerArg1: String, outerArg2: String, innerArg1: String, innerArg2: String,
                              relationTuplesMap: HashMap[String, RelationTuple],
                              orel: String, outer: TypedTuplesRecord, outerSentences: mutable.HashSet[String],
                              irel: String, inner: TypedTuplesRecord, innerSentences: mutable.HashSet[String],
                              relgramCountsMap: HashMap[String, RelgramCounts], countWindow: Int) {
    val infa1 = resolvedArgs._1
    val infa2 = resolvedArgs._2
    val insa1 = resolvedArgs._3
    val insa2 = resolvedArgs._4
    def isType(string: String) = string.startsWith("Type:")
    def isVar(string: String) = string.equals(CoreferringArguments.XVAR)
    val fa1 = if (isVar(infa1) && isType(outerArg1)) infa1 + ':' + outerArg1 else infa1
    val fa2 = if (isVar(infa2) && isType(outerArg2)) infa2 + ':' + outerArg2 else infa2
    val sa1 = if (isVar(insa1) && isType(innerArg1)) insa1 + ':' + innerArg1 else insa1
    val sa2 = if (isVar(insa2) && isType(innerArg2)) insa2 + ':' + innerArg2 else insa2


    val firstCoref = relationTuplesMap.getOrElseUpdate(relationTupleKey(fa1, orel, fa2),
      new RelationTuple(fa1, orel, fa2, outer.hashes, outerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]())) //new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts))
    addToArgCounts(firstCoref.arg1HeadCounts, outer.arg1Head)
    addToArgCounts(firstCoref.arg2HeadCounts, outer.arg2Head)
    addToSentences(firstCoref, outer.sentence)

    val secondCoref = relationTuplesMap.getOrElseUpdate(relationTupleKey(sa1, irel, sa2),
      new RelationTuple(sa1, irel, sa2, inner.hashes, innerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]())) //new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts))

    addToArgCounts(secondCoref.arg1HeadCounts, inner.arg1Head)
    addToArgCounts(secondCoref.arg2HeadCounts, inner.arg2Head)
    addToSentences(secondCoref, inner.sentence)
    val corefRgc = relgramCountsMap.getOrElseUpdate(relgramKey(firstCoref, secondCoref),
      new RelgramCounts(new Relgram(firstCoref, secondCoref),
        new scala.collection.mutable.HashMap[Int, Int],
        ArgCounts.newInstance))

    import edu.washington.cs.knowitall.relgrams.utils.MapUtils._
    updateCounts(corefRgc.counts, countWindow, 1)
    updateArgCounts(corefRgc.argCounts, outer.arg1Head, outer.arg2Head, inner.arg1Head, inner.arg2Head)
  }

  def addToArgCounts(counts: mutable.Map[String, Int], headValue: String) {
    val count = counts.getOrElseUpdate(headValue, 0)
    counts += headValue -> (count + 1)
  }



}
object RelgramsExtractorTest{

  def main(args:Array[String]){

    var i = 0
    def nextArg:String = {
      val arg = args(i)
      i = i + 1
      arg
    }
    val typeRecordsFile = nextArg
    val window = nextArg.toInt
    val outputFile = nextArg
    val tupleDocumentsWithCorefs = Source.fromFile(typeRecordsFile)
                             .getLines()
                             .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
                             /**.flatMap(line => TypedTuplesRecord.fromString(line)).toSeq.groupBy(record => record.docid)
                             .map(x => TuplesDocumentGenerator.getPrunedDocument(x._1, x._2))
                             .map(x => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(x))*/
    val relgramsExtractor = new RelgramsExtractor(window, equality=true, noequality=false)
    val writer = new PrintWriter(outputFile, "utf-8")
    tupleDocumentsWithCorefs.foreach(tdm => {
      val relgramCounts = relgramsExtractor.extractRelgramsFromDocument(tdm)
      relgramCounts.map(rgcKV => writer.println(rgcKV._2.prettyString))
    })

  }
}
