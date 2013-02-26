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


class RelgramsExtractor(maxWindow:Int) {


  val logger = LoggerFactory.getLogger(this.getClass)

  val resolver = new StanfordCoreferenceResolver()



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

  import TuplesDocumentGenerator._
  def extractRelgrams(inrecords:Seq[TypedTuplesRecord]):Map[String, RelgramCounts] = {
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
    val mentions:Map[Mention, List[Mention]] = resolver.clusters(sentencesWithOffsets.map(x => x._1).mkString("\n"))

    prunedRecords.iterator.foreach(outerIndex => {
      val outer = outerIndex._1
      val oindex = outerIndex._2
      val outerStartOffset = sentencesWithOffsets(oindex)._2
      val outerArg1s = (outer.arg1Head::Nil ++ outer.arg1Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val outerArg2s = (outer.arg2Head::Nil ++ outer.arg2Types).filter(oa1 => !oa1.trim.isEmpty).toSet
      val orel = outer.rel
      var outerSentences = new mutable.HashSet[String]()
      prunedRecords.iterator.filter(innerIndex => (innerIndex._2 > oindex) && (innerIndex._2 <= oindex+maxWindow)).foreach(innerIndex => {
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
    relgramCountsMap.toMap
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
