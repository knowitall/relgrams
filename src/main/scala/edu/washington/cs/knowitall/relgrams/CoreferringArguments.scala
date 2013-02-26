package edu.washington.cs.knowitall.relgrams

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/19/13
 * Time: 11:17 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import edu.washington.cs.knowitall.tool.coref.Mention
import edu.washington.cs.knowitall.collection.immutable.Interval

object CoreferringArguments {

  def coreferringArgs(outer: TypedTuplesRecord, outerStartOffset:Int,
                      inner: TypedTuplesRecord, innerStartOffset:Int,
                      mentionsMap: Map[Mention, List[Mention]]): Option[(String, String, String, String)] = {


    def isPartOfMentionList(interval:Interval, list:List[Mention]) = list.exists(mention => interval.subset(mention.charInterval))//mention.charInterval.intersects(interval))
    def matchingMentions(interval:Interval) = mentionsMap.values
                                                       .filter(mentionList => isPartOfMentionList(interval, mentionList))
                                                       .flatMap(mentionList => mentionList.map(m => m))//.charInterval.toString()))

    def matchingMentionsStrings(interval:Interval) = matchingMentions(interval).map(m => m.text + ":" + m.charInterval.toString).toSet

    val fa1Mentions = matchingMentionsStrings(outer.arg1HeadInterval.shift(outerStartOffset))
    val fa2Mentions = matchingMentionsStrings(outer.arg2HeadInterval.shift(outerStartOffset))
    val sa1Mentions = matchingMentionsStrings(inner.arg1HeadInterval.shift(innerStartOffset))
    val sa2Mentions = matchingMentionsStrings(inner.arg2HeadInterval.shift(innerStartOffset))


    val fa1sa1 = fa1Mentions.intersect(sa1Mentions)
    if (!fa1sa1.isEmpty) {
      println("fa1sa1: " + fa1sa1)
      println("fa1:\n" + fa1Mentions.mkString("\n"))
      println("sa1:\n" + sa1Mentions.mkString("\n"))
      return Some((XVAR + ":" + outer.arg1, outer.arg2Head, XVAR + ":" + inner.arg1, inner.arg2Head))
    }
    val fa1sa2 = fa1Mentions.intersect(sa2Mentions)
    if (!fa1sa2.isEmpty) {
      return Some((XVAR + ":" + outer.arg1, outer.arg2Head, inner.arg1Head, XVAR + ":" + inner.arg2))
    }
    val fa2sa1 = fa2Mentions.intersect(sa1Mentions)
    if (!fa2sa1.isEmpty){
      return Some((outer.arg1Head, XVAR + ":" + outer.arg2, XVAR + ":" + inner.arg1, inner.arg2Head))
    }
    val fa2sa2 = fa2Mentions.intersect(sa2Mentions)
    if (!fa2sa2.isEmpty){
      return Some((outer.arg1Head, XVAR + ":" + outer.arg2, inner.arg1Head, XVAR + ":" + inner.arg2))
    }
    None
  }

  /**def coreferringArgs(outer: TypedTuplesRecord, outerStartOffset:Int,
                      inner: TypedTuplesRecord, innerStartOffset:Int,
                      mentions: Map[Mention, List[Mention]]): Option[(String, String, String, String)] = {


    None
    //val fa1MentionCluster = mentions.values.filter(cluster => cluster.find(mention => mention.charInterval ))
  }  */


  val XVAR = "XVAR"
  /**def coreferringArgs(outer: TypedTuplesRecord, inner: TypedTuplesRecord):Option[(String, String, String, String)] = {
    val fa1 = outer.arg1Head
    val fa2 = outer.arg2Head
    val sa1 = inner.arg1Head
    val sa2 = inner.arg2Head
    println("Checking: %s,%s,%s,%s".format(fa1, fa2, sa1, sa2))
    if (areCoreferrents(fa1, sa1)) return Some((XVAR, fa2, XVAR, sa2))
    if (areCoreferrents(fa1, sa2)) return Some((XVAR, fa2, sa1, XVAR))
    if (areCoreferrents(fa2, sa1)) return Some((fa1, XVAR, XVAR, sa2))
    if (areCoreferrents(fa2, sa2)) return Some((fa1, XVAR, sa1, XVAR))
    None
  }  */
  def areCoreferrents(firstArg:String, secondArg:String) = firstArg.equals(secondArg) || firstArg.contains(secondArg) || secondArg.contains(firstArg)

}
