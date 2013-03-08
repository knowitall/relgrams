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
import scala.Some


import edu.washington.cs.knowitall.relgrams.utils.MapUtils._
import utils.MapUtils
import edu.washington.cs.knowitall.tool.coref.Mention

class RelgramsExtractor(maxWindow:Int, equality:Boolean, noequality:Boolean) {


  val logger = LoggerFactory.getLogger(this.getClass)

  def relationTupleKey(arg1: String, rel: String, arg2: String): String = "%s\t%s\t%s".format(arg1, rel, arg2)
  def relationTupleKey(relationTuple:RelationTuple) = "%s\t%s\t%s".format(relationTuple.arg1, relationTuple.rel, relationTuple.arg2)
  def relgramKey(first: RelationTuple, second: RelationTuple): String = "%s\t%s".format(relationTupleKey(first), relationTupleKey(second))

  def updateArgCounts(argCounts: ArgCounts,
                      firstArg1Head: String, firstArg2Head: String,
                      secondArg1Head: String, secondArg2Head: String){
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

  def addToIds(tuple: RelationTuple, id: String){
    tuple.ids += id
    if(tuple.ids.size > 5){
      tuple.ids = tuple.ids.drop(1)
    }
  }


  def createOrGetRelationTuple(arg1:String, rel:String, arg2:String, record:TypedTuplesRecord, sentences:mutable.Set[String], ids:mutable.Set[String], relationTuplesMap:mutable.HashMap[String, RelationTuple]) = {
    val relKey = relationTupleKey(arg1, rel, arg2)
    def newCountsMap = new mutable.HashMap[String, Int]
    relationTuplesMap.get(relKey) match {
      case Some(relationTuple:RelationTuple) => relationTuple
      case None => {
        val relationTuple = new RelationTuple(arg1, rel, arg2, record.hashes, sentences, ids, newCountsMap,newCountsMap)
        addToArgCounts(relationTuple.arg1HeadCounts, record.arg1Head)
        addToArgCounts(relationTuple.arg2HeadCounts, record.arg2Head)
        addToSentences(relationTuple, record.sentence)
        addToIds(relationTuple, record.docid + "-" + record.sentid + "-" + record.extrid)
        relationTuplesMap += relKey -> relationTuple
        relationTuple
      }
    }
  }


  def subsumes(first: RelationTuple, second: RelationTuple) = {
      def sameArgs(x:RelationTuple, y:RelationTuple)     = x.arg1.equalsIgnoreCase(y.arg1) && x.arg2.equalsIgnoreCase(y.arg2)
      def switchedArgs(x:RelationTuple, y:RelationTuple) = x.arg1.equalsIgnoreCase(y.arg2) && x.arg2.equalsIgnoreCase(y.arg1)
      def subsumedArgs(x:RelationTuple, y:RelationTuple) = x.subsumesOrSubsumedBy(y)
      val same = sameArgs(first, second)
      val switched = switchedArgs(first, second)
      val subsumed = subsumedArgs(first, second)
      same || switched || subsumed
  }



  def extractRelgramsFromDocument(document:TuplesDocumentWithCorefMentions): (Map[String, RelgramCounts], Map[String, RelationTuple]) = {
    //val docid = document.tuplesDocument.docid

    var relgramCountsMap = new mutable.HashMap[String, RelgramCounts]()
    var relationTuplesMap = new mutable.HashMap[String, RelationTuple]()




    val prunedRecords:Seq[(TypedTuplesRecord, Int)] = document.tuplesDocument.tupleRecords.zipWithIndex
    //println("Pruned records: " + prunedRecords.size)



    val mentions = document.mentions
    val trimdocument = TuplesDocumentGenerator.trimDocument(document.tuplesDocument)

    val sentencesWithOffsets = TuplesDocumentGenerator.sentenceIdsWithOffsets(trimdocument)._2
    //println("Sentences with offsets: " + sentencesWithOffsets.size)

    def getRecordsIterator = prunedRecords.iterator//.filter(index => sentencesWithOffsets.keys.contains(index._2))



    val argRepCache = new mutable.HashMap[(Int, Int), (Set[String], Set[String])]()
    def argRepresentations(record:TypedTuplesRecord) = {
      argRepCache.getOrElseUpdate((record.sentid, record.extrid), argRepresentationsNoCache(record))
    }
    def argRepresentationsNoCache(record:TypedTuplesRecord) = {
      ((record.arg1Head::Nil ++ record.arg1Types).filter(arg => !arg.trim.isEmpty).toSet,
       (record.arg2Head::Nil ++ record.arg2Types).filter(arg => !arg.trim.isEmpty).toSet)
    }
    def startingOffset(record:TypedTuplesRecord) = sentencesWithOffsets.getOrElse(record.sentid, -1000000)

    def pruneMentions(mentions: Map[Mention, List[Mention]]): Map[Mention, List[Mention]] = {
      mentions.map(kv => kv._1 -> kv._2.filter(mention => mention.text.split(" ").size <= 5))
    }





    val prunedMentions = mentions//pruneMentions(mentions)
    //println("Mentions size: " + mentions.size)
    val recordRelationTuples = new mutable.HashMap[Int, ArrayBuffer[RelationTuple]]()
    getRecordsIterator.foreach(index => {
      val record = index._1
      val (arg1s, arg2s) = argRepresentations(record)
      var sentences = new mutable.HashSet[String]()
      var ids = new mutable.HashSet[String]()
      val rel = record.relHead
      arg1s.foreach(arg1 => {
        arg2s.foreach(arg2 => {
          recordRelationTuples.getOrElseUpdate(record.sentid, new ArrayBuffer[RelationTuple]()) += createOrGetRelationTuple(arg1, rel, arg2, record, sentences, ids, relationTuplesMap)
        })
      })
    })
    //println("Relation tuples: " + recordRelationTuples.size)

    getRecordsIterator.foreach(outerIndex => {

      val outer = outerIndex._1
      val oindex = outerIndex._2
      val outerStartOffset = startingOffset(outer)
      val outerRelationTuples = recordRelationTuples.get(outer.sentid).get
      val addedSeconds = new mutable.HashSet[String]()
      getRecordsIterator.filter(innerIndex => (innerIndex._2 > oindex) && (innerIndex._2 <= oindex+maxWindow))
                        .foreach(innerIndex => {
        val iindex = innerIndex._2
        val countWindow = iindex-oindex
        val inner = innerIndex._1
        val innerStartOffset = startingOffset(inner)
        val corefArgs:Option[(String, String, String, String)] = equality match {
          case true => {
            CoreferringArguments.coreferringArgs(outer, outerStartOffset, inner, innerStartOffset, prunedMentions)
          }
          case false => None
        }
        val innerRelationTuples = recordRelationTuples.get(inner.sentid).get
        outerRelationTuples.foreach(first => {
          innerRelationTuples.filter(second => outer.sentid != inner.sentid || !subsumes(first, second))
                              .foreach(second => {
            noequality match {
              case true => {
                val relKey = relationTupleKey(second)
                if (!addedSeconds.contains(relKey)) addToRelgramCounts(outer, inner, first, second, countWindow, relgramCountsMap)
                addedSeconds += relKey
              }
              case false =>
            }
            corefArgs match {
              case Some(resolvedArgs:(String, String, String, String)) => {
                extractEqualityRelgrams(resolvedArgs, outer, inner, first, second, relationTuplesMap, relgramCountsMap, countWindow, addedSeconds)
              }
              case None =>
            }
          })
        })
      })
    })
    return (relgramCountsMap.toMap, relationTuplesMap.toMap)

  }

  def distribute(rgcMap: Map[String, RelgramCounts]) = {
    def distributeCounts(counts:mutable.Map[Int, Int]) {
      val minOccurrenceWindow = counts.filter(kv => kv._2 > 0).map(kv => kv._1).min
      (1 until maxWindow).map(window => {
        if (window >= minOccurrenceWindow){
          counts += window -> 1
        }else {
          counts -= window
        }
      })
    }
    rgcMap.values.foreach(rgc => distributeCounts(rgc.counts))
    rgcMap
  }

  def addToRelgramCounts(outer:TypedTuplesRecord, inner:TypedTuplesRecord,
                         first:RelationTuple, second:RelationTuple,
                         countWindow:Int,
                         relgramCountsMap: HashMap[String, RelgramCounts]) {

    val rgc = relgramCountsMap.getOrElseUpdate(relgramKey(first, second),
      new RelgramCounts(new Relgram(first, second),
        new scala.collection.mutable.HashMap[Int, Int],
        ArgCounts.newInstance))
    updateCounts(rgc.counts, countWindow, 1)
    updateArgCounts(rgc.argCounts, outer.arg1Head, outer.arg2Head, inner.arg1Head, inner.arg2Head)
  }



  def extractEqualityRelgrams(resolvedArgs: (String, String, String, String),
                              outer: TypedTuplesRecord, inner: TypedTuplesRecord,
                              first: RelationTuple, second: RelationTuple,
                              relationTuplesMap:HashMap[String, RelationTuple], relgramCountsMap: HashMap[String, RelgramCounts],
                              countWindow: Int,
                              addedSeconds:mutable.HashSet[String]){



    val infa1 = resolvedArgs._1
    val infa2 = resolvedArgs._2
    val insa1 = resolvedArgs._3
    val insa2 = resolvedArgs._4
    def isType(string: String) = string.startsWith("Type:")
    def isVar(string: String) = string.startsWith(CoreferringArguments.XVAR)
    val fa1 = if (isVar(infa1) && isType(first.arg1)) infa1 + ':' + first.arg1 else infa1
    val fa2 = if (isVar(infa2) && isType(first.arg2)) infa2 + ':' + first.arg2 else infa2
    val sa1 = if (isVar(insa1) && isType(second.arg1)) insa1 + ':' + second.arg1 else insa1
    val sa2 = if (isVar(insa2) && isType(second.arg2)) insa2 + ':' + second.arg2 else insa2

    val secondKey = relationTupleKey(sa1, inner.relHead, sa2)
    if (addedSeconds.contains(secondKey)) return


    val firstSentences = new mutable.HashSet[String]
    firstSentences ++= first.sentences

    val firstids = new mutable.HashSet[String]
    firstids ++= first.ids
    val firstCoref = createOrGetRelationTuple(fa1, first.rel, fa2, outer, firstSentences, firstids, relationTuplesMap)

    val secondSentences = new mutable.HashSet[String]
    secondSentences ++= second.sentences

    val secondids = new mutable.HashSet[String]
    secondids ++= second.ids
    val secondCoref = createOrGetRelationTuple(sa1, second.rel, sa2, inner, secondSentences, secondids, relationTuplesMap)

    val corefRgc = relgramCountsMap.getOrElseUpdate(relgramKey(firstCoref, secondCoref),
      new RelgramCounts(new Relgram(firstCoref, secondCoref),
        new scala.collection.mutable.HashMap[Int, Int],
        ArgCounts.newInstance))
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
    val relgramsFile = nextArg
    val tuplesFile = nextArg
    val tupleDocumentsWithCorefs = Source.fromFile(typeRecordsFile)
                             .getLines()
                             .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
    val relgramsExtractor = new RelgramsExtractor(window, equality=true, noequality=true)
    def relgramsString (relgramCount:RelgramCounts):String = {
      "First\t%s\nSecond\t%s\nCounts\t%s\n\n".format(relgramCount.relgram.first.prettyString,
                                                     relgramCount.relgram.second.prettyString,
                                                     MapUtils.toCountsString(relgramCount.counts, ",", ":"))
    }

    val relgramsWriter = new PrintWriter(relgramsFile, "utf-8")
    val tuplesWriter = new PrintWriter(tuplesFile, "utf-8")
    tupleDocumentsWithCorefs.foreach(tdm => {
      val (relgramCounts, tuplesCounts) = relgramsExtractor.extractRelgramsFromDocument(tdm)
      println("RelgramCounts size: " + relgramCounts.keys.size)
      println("Tuples size: " + tuplesCounts.keys.size)
      relgramCounts.foreach(rgcKV => relgramsWriter.println(relgramsString(rgcKV._2)))//prettyString))
      tuplesCounts.foreach(tupleCount => tuplesWriter.println(tupleCount._2.prettyString))
    })
    relgramsWriter.close()
    tuplesWriter.close()
  }
}
