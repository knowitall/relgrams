package edu.washington.cs.knowitall.relgrams

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/17/13
 * Time: 10:23 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import scala.collection._

case class RelationTuple(arg1:String, rel:String, arg2:String,
                         var hashes:Set[Int],
                         var arg1HeadCounts:scala.collection.mutable.Map[String,Int],
                         var arg2HeadCounts:scala.collection.mutable.Map[String,Int]){

  private def countsString(counts:Map[String, Int]):String = RelgramCountingUtils.toCountsString(counts)//counts.toSeq.sortBy(ac => ac._2).map(ac => "%s(%d)".format(ac._1, ac._2)).mkString(",")
  def prettyString:String = "%s\t%s\t%s".format(arg1, rel, arg2)
  override def toString:String = "%s\t%s\t%s\t%s\t%s\t%s".format(arg1, rel, arg2, hashes.mkString(","), countsString(arg1HeadCounts.toMap), countsString(arg2HeadCounts.toMap))
}
case class Relgram(first:RelationTuple, second:RelationTuple){
  def prettyString:String = "%s\t%s".format(first.prettyString, second.prettyString)
  override def toString:String = "%s\t%s".format(first, second)
}

object ArgCounts {
  def newMap = new mutable.HashMap[String, Int]
  def newInstance = new ArgCounts(newMap, newMap, newMap, newMap)

}
case class ArgCounts(firstArg1Counts:scala.collection.mutable.Map[String,Int],
                     firstArg2Counts:scala.collection.mutable.Map[String,Int],
                     secondArg1Counts:scala.collection.mutable.Map[String,Int],
                     secondArg2Counts:scala.collection.mutable.Map[String,Int]){

  import RelgramCountingUtils._
  override def toString:String = "%s\t%s\t%s\t%s".format(toCountsString(firstArg1Counts.toMap), toCountsString(firstArg2Counts.toMap),
                                                        toCountsString(secondArg1Counts.toMap), toCountsString(secondArg2Counts.toMap))
}

case class RelgramCounts(relgram:Relgram, counts:scala.collection.mutable.Map[Int, Int], argCounts:ArgCounts){

  def prettyString:String = "%s\t%s\t%s".format(relgram.prettyString,
                                                 RelgramCountingUtils.toCountsString(counts.toMap),
                                                 argCounts.toString)

  override def toString:String = "%s\t%s\t%s".format(relgram.toString,
                                                     RelgramCountingUtils.toCountsString(counts.toMap),
                                                     argCounts.toString)

}


object RelgramCountingUtils {

def toCountsString[A](counts:Map[A, Int])(implicit ordering:Ordering[A]) = counts.toSeq.sortBy(x => x._1).map(wc => wc._1 + ":" + wc._2).mkString(",")
def updateHashes(rgc: RelgramCounts,
                 headFirst: String, headSecond: String,
                 firstHashes: Set[Int], secondHashes:Set[Int]) {
  val (fh:Set[Int], sh:Set[Int]) = lexicalOrder(headFirst, headSecond, firstHashes, secondHashes)
  rgc.relgram.first.hashes ++= fh
  rgc.relgram.second.hashes ++= sh
}



/**
 * Return a tuple in which the firstValue is the first element if the firstKey is lexically smaller than the second key.
 */
def lexicalOrder[T](firstKey: String, secondKey: String, firstValue:T, secondValue:T): (T, T) = {
  if (firstKey.compareTo(secondKey) > 0) (secondValue, firstValue) else (firstValue, secondValue)
}

def lexicallySortedTuple(first: String, second: String): (String, String) = lexicalOrder(first, second, first, second).asInstanceOf[(String, String)]



}

