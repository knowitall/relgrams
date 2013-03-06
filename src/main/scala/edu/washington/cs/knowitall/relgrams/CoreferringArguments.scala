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

  val XVAR = "XVAR"
  def coreferringArgs(outer: TypedTuplesRecord, outerStartOffset:Int,
                      inner: TypedTuplesRecord, innerStartOffset:Int,
                      mentionsMap: Map[Mention, List[Mention]]): Option[(String, String, String, String)] = {


    def isPartOf(interval:Interval, list:List[Mention]) = list.exists(mention => interval.subset(mention.charInterval))
    def matchingMentions(interval:Interval, text:String) = mentionsMap.values
                                                                      .filter(mentionList => isPartOf(interval, mentionList))
                                                                      .flatMap(mentionList => mentionList.map(m => m.text + ":" + m.charInterval.toString))
                                                                      .toSet

    val fa1Mentions = matchingMentions(outer.arg1HeadInterval.shift(outerStartOffset), outer.arg1Head)
    val fa2Mentions = matchingMentions(outer.arg2HeadInterval.shift(outerStartOffset), outer.arg2Head)

    val sa1Mentions = matchingMentions(inner.arg1HeadInterval.shift(innerStartOffset), inner.arg1Head)
    val sa2Mentions = matchingMentions(inner.arg2HeadInterval.shift(innerStartOffset), inner.arg2Head)

    if (fa1Mentions.exists(sa1Mentions)) return Some((XVAR, outer.arg2Head, XVAR, inner.arg2Head))
    if (fa1Mentions.exists(sa2Mentions)) return Some((XVAR, outer.arg2Head, inner.arg1Head, XVAR))
    if (fa2Mentions.exists(sa1Mentions)) return Some((outer.arg1Head, XVAR, XVAR, inner.arg2Head))
    if (fa2Mentions.exists(sa2Mentions)) return Some((outer.arg1Head, XVAR, inner.arg1Head, XVAR))
    None
  }



}


/**
 * println("fa1sa1: " + fa1sa1)
      println("fa1:\n" + fa1Mentions.mkString("\n"))
      println("sa1:\n" + sa1Mentions.mkString("\n"))
      println("outer.arg1: " + outer.arg1)
      println("inner.arg1: " + inner.arg1)
      println("outer: " + outer.toString)
      println("inner: " + inner.toString)
      println
      println
      println


 println("fa1sa2: " + fa1sa2)
      println("fa1:\n" + fa1Mentions.mkString("\n"))
      println("sa2:\n" + sa2Mentions.mkString("\n"))
      println("outer.arg1: " + outer.arg1)
      println("inner.arg2: " + inner.arg2)
      println("outer: " + outer.toString)
      println("inner: " + inner.toString)
      println
      println
      println


println("fa2sa1: " + fa2sa1)
      println("fa2:\n" + fa2Mentions.mkString("\n"))
      println("sa1:\n" + sa1Mentions.mkString("\n"))
      println("outer.arg2: " + outer.arg2)
      println("inner.arg1: " + inner.arg1)
      println("outer: " + outer.toString)
      println("inner: " + inner.toString)

      println("outer start offset: " + outerStartOffset)
      println("inner start offset: " + innerStartOffset)
      println("outer arg2HeadInterval: " + outer.arg2HeadInterval)
      println("inner arg1HeadInterval: " + inner.arg1HeadInterval)

      println("outer shifted arg2HeadInterval: " + outer.arg2HeadInterval.shift(outerStartOffset))
      println("inner shifted arg1HeadInterval: " + inner.arg1HeadInterval.shift(innerStartOffset))
      println
      println
      println


  println("fa2sa2: " + fa2sa2)
      println("fa2:\n" + fa2Mentions.mkString("\n"))
      println("sa2:\n" + sa2Mentions.mkString("\n"))
      println("outer.arg2: " + outer.arg2)
      println("inner.arg2: " + inner.arg2)
      println("outer: " + outer.toString)
      println("inner: " + inner.toString)
      println
      println
      println

 *
 *
 *
 */
