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
    def matchingMentions(interval:Interval, text:String) = (text.toLowerCase + ":" + interval.toString()::Nil ++ mentionsMap.values
                                                                      .filter(mentionList => isPartOf(interval, mentionList))
                                                                      .flatMap(mentionList => mentionList.map(m => m.text.toLowerCase + ":" + m.charInterval.toString))
                                                                      ).toSet

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

