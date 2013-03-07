package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/7/13
 * Time: 10:00 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import java.io.PrintWriter
import io.Source

object ToUTF {

  def main(args:Array[String]){
    val inputFile = args(0)
    val outputFile = args(1)
    val writer = new PrintWriter(outputFile, "UTF-8")
    Source.fromFile(inputFile).getLines().foreach(line => writer.println(line))
    writer.close()
  }
}
