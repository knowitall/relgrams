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
  def relgramKey(farg1:String, frel:String, farg2:String,
                 sarg1:String, srel:String, sarg2:String):String =  "%s\t%s".format(relationTupleKey(farg1, frel, farg2), relationTupleKey(sarg1, srel, sarg2))
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


  def subsumes(first: TypedTuplesRecord, second: TypedTuplesRecord) = {
      def sameArgs(x:TypedTuplesRecord, y:TypedTuplesRecord)     = x.arg1.equalsIgnoreCase(y.arg1) && x.arg2.equalsIgnoreCase(y.arg2)
      def switchedArgs(x:TypedTuplesRecord, y:TypedTuplesRecord) = x.arg1.equalsIgnoreCase(y.arg2) && x.arg2.equalsIgnoreCase(y.arg1)
      def subsumedArgs(x:TypedTuplesRecord, y:TypedTuplesRecord) = x.subsumesOrSubsumedBy(y)
      val same = sameArgs(first, second)
      val switched = switchedArgs(first, second)
      val subsumed = subsumedArgs(first, second)

      same || switched || subsumed
  }


  def isTypedTuple(tuple:RelationTuple) = tuple.arg1.startsWith("Type:") || tuple.arg2.startsWith("Type:")

  def extractRelgramsFromDocument(document:TuplesDocumentWithCorefMentions): (Map[String, RelgramCounts], Map[String, RelationTuple]) = {
    //val docid = document.tuplesDocument.docid

    var relgramCountsMap = new mutable.HashMap[String, RelgramCounts]()
    var relationTuplesMap = new mutable.HashMap[String, RelationTuple]()


    val prunedRecords:Seq[(TypedTuplesRecord, Int)] = document.tuplesDocument
                                                              .tupleRecords
                                                              //.filter(record => notInferredPrepRelation(record))
                                                              .sortBy(r => (r.sentid, r.extrid))
                                                              .zipWithIndex

    val mentions = document.mentions
    val trimdocument = TuplesDocumentGenerator.trimDocument(document.tuplesDocument)

    val sentencesWithOffsets = TuplesDocumentGenerator.sentenceIdsWithOffsets(trimdocument)._2

    def getRecordsIterator = prunedRecords.iterator

    val argRepCache = new mutable.HashMap[(Int, Int), (Set[String], Set[String])]()
    def argRepresentations(record:TypedTuplesRecord) = {
      argRepCache.getOrElseUpdate((record.sentid, record.extrid), argRepresentationsNoCache(record))
    }
    def argRepresentationsNoCache(record:TypedTuplesRecord) = {
      def isNotQuantityPartOrGroupType(arg:String) = !arg.startsWith("type") || (!arg.contains("quantity") && !arg.contains("part") && !arg.contains("group"))
      def lowerCaseAndFilter(args:Iterable[String]) = args.map(arg => arg.toLowerCase)
                                                      .filter(arg => !arg.trim.isEmpty)
                                                      .filter(arg => isNotQuantityPartOrGroupType(arg))
                                                      .map(arg => arg.replaceAll("""number:number""", "number"))


      def ifTimeUnitOrPeriodRemoveNumber(args:Iterable[String]) = if(args.contains("type:time_unit") || args.contains("type:time_period")) args.filter(arg => !arg.equals("type:number")) else args
      (ifTimeUnitOrPeriodRemoveNumber(lowerCaseAndFilter(record.arg1Head::Nil ++ record.arg1Types)).toSet,
        ifTimeUnitOrPeriodRemoveNumber(lowerCaseAndFilter(record.arg2Head::Nil ++ record.arg2Types)).toSet)
    }
    def startingOffset(record:TypedTuplesRecord) = sentencesWithOffsets.getOrElse(record.sentid, -1000000)

    def pruneMentions(mentions: Map[Mention, List[Mention]]): Map[Mention, List[Mention]] = {
      mentions.map(kv => kv._1 -> kv._2.filter(mention => mention.text.split(" ").size <= 5))
    }

    val prunedMentions = pruneMentions(mentions)
    val recordRelationTuples = new mutable.HashMap[(Int, Int), mutable.HashSet[RelationTuple]]()
    getRecordsIterator.foreach(index => {
      val record = index._1
      val (arg1s, arg2s) = argRepresentations(record)
      var sentences = new mutable.HashSet[String]()
      var ids = new mutable.HashSet[String]()
      val rel = record.relHead
      arg1s.foreach(arg1 => {
        arg2s.foreach(arg2 => {
          recordRelationTuples.getOrElseUpdate((record.sentid, record.extrid), new mutable.HashSet[RelationTuple]()) += createOrGetRelationTuple(arg1, rel, arg2, record, sentences, ids, relationTuplesMap)
        })
      })
    })
    def areFromDifferentExtractions(one:TypedTuplesRecord, other:TypedTuplesRecord) = one.sentid != other.sentid || one.extrid != other.extrid
    getRecordsIterator.foreach(outerIndex => {

      val outer = outerIndex._1
      val oindex = outerIndex._2
      val outerStartOffset = startingOffset(outer)
      val outerRelationTuples = recordRelationTuples.get((outer.sentid, outer.extrid)).get
      val addedSeconds = new mutable.HashSet[String]()

      getRecordsIterator.filter(innerIndex => areFromDifferentExtractions(outer, innerIndex._1))
                        .filter(innerIndex => (innerIndex._2 > oindex) && (innerIndex._2 <= oindex+maxWindow))
                        .filter(innerIndex => outer.sentid != innerIndex._1.sentid || !subsumes(outer, innerIndex._1))
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
        val innerRelationTuples = recordRelationTuples.get((inner.sentid, inner.extrid)).get

        import edu.washington.cs.knowitall.relgrams.utils.Crossable._
        val firstSeconds = (outerRelationTuples x innerRelationTuples)
          firstSeconds.foreach(firstsecond => {
          val first = firstsecond._1
          val second = firstsecond._2
          noequality match {
            case true => {
              val relKey = relgramKey(first, second)
              //if(relKey.contains("be set leave")) println("Rel key: %s\tsecond: %s".format(relKey, second))
              if (!addedSeconds.contains(relKey)) {
                addToRelgramCounts(outer, inner, first, second, countWindow, relgramCountsMap)
              }
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

    def isType(string: String) = string.startsWith("Type:") || string.startsWith("type:")
    def isVar(string: String) = string.startsWith(CoreferringArguments.XVAR)

    def argRep(resolved:String, unresolved:String) = {
      (isVar(resolved), isType(unresolved)) match {
        case (true, true) => resolved + ':' + unresolved
        case (true, false) => resolved
        case _ => unresolved
      }
    }



    val fa1 = argRep(infa1, first.arg1)
    val fa2 = argRep(infa2, first.arg2)
    val sa1 = argRep(insa1, second.arg1)
    val sa2 = argRep(insa2, second.arg2)

    def isTypedVar(resolvedArg:String) = {
      val splits = resolvedArg.split(":")
      splits.size == 3 && splits(1).equalsIgnoreCase("Type")
    }
    val farg = if(isTypedVar(fa1)) fa1.split(":")(2) else if (isTypedVar(fa2)) fa2.split(":")(2) else "NA"
    val sarg = if(isTypedVar(sa1)) sa1.split(":")(2) else if (isTypedVar(sa2)) sa2.split(":")(2) else "NA"
    if (!(farg.equals("NA") || sarg.equals("NA")) && !(farg.equals(sarg))){
      return
    }

    val secondKey = relgramKey(fa1, outer.relHead, fa2, sa1, inner.relHead, sa2)//relationTupleKey(sa1, inner.relHead, sa2)

    if (addedSeconds.contains(secondKey)) {
      println("Ignoring: " + secondKey)
      return
    }
    addedSeconds += secondKey

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
