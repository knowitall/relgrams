package edu.washington.cs.knowitall.relgrams.utils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/17/13
 * Time: 9:48 AM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import java.io._
import edu.stanford.nlp.io.StringOutputStream
import scala.Some

object StringUtils {

  def split(string:String, blockSize:Int):Seq[String] = {
    val size = string.length
    val nparts = size/blockSize + 1
    if (nparts < 1) return Seq[String](string)
    var start = 0
    (0 until nparts).flatMap(i => {
      val end = math.min(string.length, start + blockSize)
      println("Start: " + start, " End: " + end)
      val substring = if (end <= string.length) Some(string.substring(start, end)) else None
      start = end
      substring
    })
  }

  def splitIntoNPlusOneParts(string:String, nparts:Int):Seq[String] = {
    def getBlockSize(size:Int, n:Int) = {
      math.round(size.toDouble/n.toDouble).toInt
    }
    if (nparts < 1) return Seq[String](string)
    val blockSize = getBlockSize(string.length, nparts)//string.length/nparts
    var start = 0
    (0 until nparts+1).map(i => {
      val end = math.min(string.length, start + blockSize)
      val substring = if (end <= string.length) string.substring(start, end) else ""
      start = end
      substring
    }).toSeq
  }

  def writeUTF(out:DataOutput, string:String, nparts:Int){
    val parts = splitIntoNPlusOneParts(string, nparts)
    println("Writing strings: " + parts.mkString("\n"))
    parts.foreach(out.writeUTF(_))
  }
  def readUTF(in:DataInput, nparts:Int) = {
    val strings = (0 until nparts+1).map(i => in.readUTF())
    println("Read strings: " + strings.mkString(","))
    strings
  }


  def main(args:Array[String]){
    val string = "123456789"
    val filename = StringUtils.getClass.getName + ".txt"
    (0 until 10).foreach(i => {
      val parts = splitIntoNPlusOneParts(string, i)
      assert(parts.size == i+1, "Actual Number of parts %d != expected number %d".format(parts.size, i+1))
      println("%d parts: [%s]".format(i, splitIntoNPlusOneParts(string, i)))
      val outStream = new DataOutputStream(new FileOutputStream(filename))
      writeUTF(outStream, string, i)
      outStream.close()
      val inStream = new DataInputStream(new FileInputStream(filename))
      val readStrings = readUTF(inStream, i)
      assert(readStrings.size == i+1, "Actual number of strings read %d != expected number %d".format(readStrings.size, i+1))

      println("%d read: [%s]".format(i, readStrings))
      val instring = readStrings.mkString("").trim
      println("Concat: " + instring.trim)
      inStream.close()
    })




  }


}
