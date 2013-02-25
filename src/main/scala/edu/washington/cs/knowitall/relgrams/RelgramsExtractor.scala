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
import edu.washington.cs.knowitall.tool.coref.StanfordCoreferenceResolver
import scala.collection


class RelgramsExtractor(window:Int) {


  val logger = LoggerFactory.getLogger(this.getClass)

  val resolver = new StanfordCoreferenceResolver()

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

  def extractRelgrams(inrecords:Seq[TypedTuplesRecord]) = {
    val records = inrecords
    val prunedRecords = pruneRecordsAndIndex(records)
    var relgramCountsMap = new mutable.HashMap[String, RelgramCounts]()
    var relationTuplesMap = new mutable.HashMap[String, RelationTuple]()

    var offset = 0
    val sentencesWithOffsets = prunedRecords.iterator.map(record => {
      val curOffset = offset
      offset = offset + record._1.sentence.length + 1
      (record._1.sentence, curOffset)
    }).toSeq

    import edu.washington.cs.knowitall.relgrams.utils.Pairable._
    //val prunedRecordsWithStartOffsets = (prunedRecords pairElements sentencesWithOffsets)
    val mentions = resolver.clusters(sentencesWithOffsets.map(x => x._1).mkString("\n"))

    prunedRecords.iterator.foreach(outerIndex => {
      val outer = outerIndex._1
      val oindex = outerIndex._2
      val outerStartOffset = sentencesWithOffsets(oindex)._2
      val outerArg1s = (outer.arg1Head::Nil ++ outer.arg1Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val outerArg2s = (outer.arg2Head::Nil ++ outer.arg2Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val orel = outer.rel
      var outerSentences = new mutable.HashSet[String]()
      prunedRecords.iterator.filter(innerIndex => (innerIndex._2 > oindex) && (innerIndex._2 <= oindex+window)).foreach(innerIndex => {
        val iindex = innerIndex._2
        val countWindow = iindex-oindex
        val inner = innerIndex._1
        val innerStartOffset = sentencesWithOffsets(iindex)._2
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

        val irel = inner.rel

        val corefArgs:Option[(String, String, String, String)] = CoreferringArguments.coreferringArgs(outer, outerStartOffset, inner, innerStartOffset, mentions)
        /**corefArgs match {
          case Some(x:(String, String, String, String)) => println("corefs: " + x)
          case None => if (outer.arg2Head.contains("measure")) { println("Failed coref: " + outer + "\n" + inner )}
        }*/
        outerArg1s.foreach(oa1 => {
          outerArg2s.foreach(oa2 => {
            val first = relationTuplesMap.getOrElseUpdate(relationTupleKey(oa1, orel, oa2), new RelationTuple(oa1, orel, oa2, outer.hashes, outerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
            innerArg1s.foreach(ia1 => {
              innerArg2s.foreach(ia2 => {
                val second = relationTuplesMap.getOrElseUpdate(relationTupleKey(ia1, irel, ia2), new RelationTuple(ia1, irel, ia2, inner.hashes, innerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
                addToArgCounts(second.arg1HeadCounts, inner.arg1Head)
                addToArgCounts(second.arg2HeadCounts, inner.arg2Head)
                addToSentences(second, inner.sentence)
                val rgc = relgramCountsMap.getOrElseUpdate(relgramKey(first, second),
                  new RelgramCounts(new Relgram(first, second),
                                    new scala.collection.mutable.HashMap[Int, Int],
                                    ArgCounts.newInstance))

                import edu.washington.cs.knowitall.relgrams.utils.MapUtils._
                updateCounts(rgc.counts, countWindow, 1)
                updateArgCounts(rgc.argCounts, outer.arg1Head, outer.arg2Head, inner.arg1Head, inner.arg2Head)
                corefArgs match {
                  case Some(x:(String, String, String, String)) => {
                    val infa1 =x._1
                    val infa2 =x._2
                    val insa1 = x._3
                    val insa2 = x._4
                    def isType(string:String) = string.startsWith("Type:")
                    def isVar(string:String) = string.equals(CoreferringArguments.XVAR)
                    val fa1 = if (isVar(infa1) && isType(oa1)) infa1 + ':' + oa1 else infa1
                    val fa2 = if (isVar(infa2) && isType(oa2)) infa2 + ':' + oa2 else infa2
                    val sa1 = if (isVar(insa1) && isType(ia1)) insa1 + ':' + ia1 else insa1
                    val sa2 = if (isVar(insa2) && isType(ia2)) insa2 + ':' + ia2 else insa2


                    val firstCoref = relationTuplesMap.getOrElseUpdate(relationTupleKey(fa1, orel, fa2),
                      new RelationTuple(fa1, orel, fa2, outer.hashes, outerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts))
                    addToArgCounts(firstCoref.arg1HeadCounts, outer.arg1Head)
                    addToArgCounts(firstCoref.arg2HeadCounts, outer.arg2Head)
                    addToSentences(firstCoref, outer.sentence)

                    val secondCoref = relationTuplesMap.getOrElseUpdate(relationTupleKey(sa1, irel, sa2),
                       new RelationTuple(sa1, irel, sa2, inner.hashes, innerSentences, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts))

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
                  case None =>

                }

                //val count = rgc.counts.getOrElseUpdate(countWindow, 0)
                //rgc.counts += window -> (count + 1)
              })
            })
          })
        })


      })
    })
    relgramCountsMap
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
    val groupedRecords = Source.fromFile(typeRecordsFile)
                             .getLines()
                             .flatMap(line => TypedTuplesRecord.fromString(line)).toSeq.groupBy(record => record.docid)

    val relgramsExtractor = new RelgramsExtractor(window)
    val writer = new PrintWriter(outputFile, "utf-8")
    groupedRecords.foreach(kv => {
      val records = kv._2
      val relgramCounts = relgramsExtractor.extractRelgrams(records)
      relgramCounts.map(rgcKV => writer.println(rgcKV._2.prettyString))
    })

  }
}
