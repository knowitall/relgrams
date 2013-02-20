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

object CoreferringArguments {
  def coreferringArgs(outer: TypedTuplesRecord, outerStartOffset:Int,
                      inner: TypedTuplesRecord, innerStartOffset:Int,
                      mentions: Map[Mention, List[Mention]]): Option[(String, String, String, String)] = {


    val fa1MentionCluster = mentions.values.filter(cluster => cluster.find(mention => mention.offset < ))
  }


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
