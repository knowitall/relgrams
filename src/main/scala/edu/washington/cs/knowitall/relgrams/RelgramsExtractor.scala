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



object TypedTuplesRecord{

}

case class TypedTuplesRecord(docid:String, sentid:Int, sentence:String, extrid:Int, hashes:Set[Int],
                             arg1:String, rel:String, arg2:String,
                             arg1Head:String, var relHead:String, arg2Head:String,
                             arg1Types:Seq[String], arg2Types:Seq[String]){



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

class RelgramsExtractor(window:Int) {




  //Two relations are different as long as they are between different arguments.
  def areDifferentRelations(outer: TypedTuplesRecord, inner: TypedTuplesRecord): Boolean = {
    if (outer.arg1.equals(inner.arg1) && outer.arg2.equals(inner.arg2)){
      //println("args are exactly the same: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    if (outer.arg1.equals(inner.arg2) && outer.arg2.equals(inner.arg1)){// && relationsAreMostlySame(inner.relHead, outer.relHead)){
      //println("args are inverted but relations are same: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    if(inner.subsumes(outer) || outer.subsumes(inner)){
      //println("subsumes: " + inner.arg1Head + "," + inner.relHead + "," + inner.arg2Head + "\t" + outer.arg1Head + "," + outer.relHead + "," + outer.arg2Head)
      return false
    }
    return true
  }


  def relationsAreMostlySame(arel: String, brel: String): Boolean = {
    val arelClean = cleanRelString(arel)
    val brelClean = cleanRelString(brel)
    arelClean.equals(brelClean)
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

  def pruneRecords(sRecords:Seq[TypedTuplesRecord]):Seq[TypedTuplesRecord] = {
  var ssRecords = removeNonAlphaNumericRecords(sRecords).sortBy(record => sortValue(record))
  ssRecords = removeMoreThanXWordRelations(ssRecords, 7)
  ssRecords = removeRedundantRecords(ssRecords)
  ssRecords.foreach(record => record.relHead = rewriteBeAndHasVerbs(record.relHead))
  return removeReportingAndBeHadRelations(ssRecords)
  }


  def relationTupleKey(arg1: String, rel: String, arg2: String): String = "%s\t%s\t%s".format(arg1, rel, arg2)
  def relationTupleKey(relationTuple:RelationTuple) = "%s\t%s\t%s".format(relationTuple.arg1, relationTuple.rel, relationTuple.arg2)
  def relgramKey(first: RelationTuple, second: RelationTuple): String = "%s\t%s".format(relationTupleKey(first), relationTupleKey(second))

  def extractRelgrams(records:Seq[TypedTuplesRecord]) = {
    val prunedRecords = pruneRecords(records)
    var relgramCountsMap = new mutable.HashMap[String, RelgramCounts]()
    var relationTuplesMap = new mutable.HashMap[String, RelationTuple]()
    prunedRecords.iterator.foreach(outer => {
      val outerArg1s = outer.arg1Head::Nil ++ outer.arg1Types
      val outerArg2s = outer.arg2Head::Nil ++ outer.arg2Types

      prunedRecords.iterator.filter(inner => inner.isWithinWindow(outer, window)).foreach(inner => {
        val innerArg1s = inner.arg1Head::Nil ++ inner.arg1Types
        val innerArg2s = inner.arg2Head::Nil ++ inner.arg2Types
        outerArg1s.foreach(oa1 => {
          outerArg2s.foreach(oa2 => {
            val first = relationTuplesMap.getOrElseUpdate(relationTupleKey(oa1, outer.relHead, oa2), new RelationTuple(oa1, rel, oa2, outer.hashes, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
            addToArgCounts(first.arg1HeadCounts, outer.arg1Head)
            addToArgCounts(first.arg2HeadCounts, outer.arg2Head)
            innerArg1s.foreach(ia1 => {
              innerArg2s.foreach(ia2 => {
                val second = relationTuplesMap.getOrElseUpdate(relationTupleKey(ia1, inner.relHead, ia2), new RelationTuple(ia1, rel, ia2, inner.hashes, new mutable.HashMap[String, Int], new mutable.HashMap[String, Int]()))//new RelationTuple(oa1, outer.relHead, oa2, outer.hashes, arg1HeadCounts, arg2HeadCounts)
                //addToArgCounts(second.arg1HeadCounts, inner.arg1Head)
                //addToArgCounts(second.arg2HeadCounts, inner.arg2Head)
                val rgc = relgramCountsMap.getOrElseUpdate(relgramKey(first, second),
                  new RelgramCounts(new Relgram(first, second), new scala.collection.mutable.HashMap[Int, Int]))
                val count = rgc.counts.getOrElseUpdate(window, 0)
                rgc.counts += window -> (count + 1)
              })
            })
          })
        })


      })
    })
    relgramCountsMap
  }

}


  def addToArgCounts(counts: mutable.Map[String, Int], headValue: String) {
    val count = counts.getOrElseUpdate(headValue, 0)
    counts += headValue -> (count + 1)
  }

  def main(args:Array[String]){

  }
}
