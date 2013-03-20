package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/20/13
 * Time: 2:02 PM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory

object CollectionUtils {
    val logger = LoggerFactory.getLogger(this.getClass)
  def maxOrElse[B](values:Iterable[B], orElse:B)(implicit numeric: Numeric[B]) = if (!values.isEmpty) values.max else orElse
}
