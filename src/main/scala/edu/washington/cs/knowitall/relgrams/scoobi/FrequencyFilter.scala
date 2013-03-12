package edu.washington.cs.knowitall.relgrams.scoobi

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/11/13
 * Time: 9:25 AM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import com.nicta.scoobi.core.DList

import com.nicta.scoobi.Persist._

import java.io.{PrintWriter, File}
import io.Source
import edu.washington.cs.knowitall.relgrams.{RelationTuple, Relgram, RelgramCounts}
import collection.mutable

object FrequencyFilter extends ScoobiApp{

  val logger = LoggerFactory.getLogger(this.getClass)


  def distributeCounts(counts: Array[(Int, Int)], maxWindow:Int) = {

    var countsMap = counts.map(wc => wc._1 -> wc._2).toMap
    var sum = 0
    (0 until maxWindow).map(window => {
      sum = sum + countsMap.getOrElse(window, 0)
      sum
    })
  }
  def distributeCounts(counts: mutable.Map[Int, Int], maxWindow:Int) = {
    var sum = 0
    (0 until maxWindow).foreach(window => {
      sum = sum + counts.getOrElse(window, 0)
      counts += window -> sum
    })

  }

  def export(strings: DList[String], outputPath: String){
    try{
      val path = outputPath + File.separator + "tuples"
      persist(TextOutput.toTextFile(strings, path))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced tuples.")
        e.printStackTrace
      }

    }
  }

  def exportRelgramCounts(strings: DList[RelgramCounts], outputPath: String){
    try{
      val path = outputPath + File.separator + "tuples"
      persist(TextOutput.toTextFile(strings, path))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced tuples.")
        e.printStackTrace
      }

    }
  }



  def isGoodField(string:String) = {
    val alphaDigitRe = """^[a-zA-Z0-9]+.*""".r
    if(string == null)
      false
    else
      alphaDigitRe.findFirstMatchIn(string) != None
  }
  def isGoodTuple(tuple:RelationTuple) = if(tuple == null)  false else isGoodField(tuple.arg1) && isGoodField(tuple.rel) && isGoodField(tuple.arg2)
  def isGoodRelgram(relgram:Relgram) = isGoodTuple(relgram.first) && isGoodTuple(relgram.second)
  def isGoodRgc(rgc:RelgramCounts) = isGoodRelgram(rgc.relgram)

  def loadAndFilterRelgrams(inputPath:String, maxWindow:Int, minFreq:Int) = {
    TextInput.fromTextFile(inputPath)
             .flatMap(line => RelgramCounts.fromSerializedString(line))
             .filter(rgc => isGoodRgc(rgc))
             .map(rgc => {
                  distributeCounts(rgc.counts, maxWindow)
                  rgc
             })
             .filter(rgc => rgc.counts.values.max > minFreq)
  }

  def loadAndFilterRelgramsLocal(inputPath:String, maxWindow:Int, minFreq:Int) = {
    Source.fromFile(inputPath).getLines
          .flatMap(line => RelgramCounts.fromSerializedString(line))
          .filter(rgc => isGoodRgc(rgc))
          .map(rgc => {
              distributeCounts(rgc.counts, maxWindow)
              rgc
          })
          .filter(rgc => rgc.counts.values.max >= minFreq)
  }



  def run() {

    var inputPath, outputPath = ""
    var maxWindow = 10
    var minFreq = 3
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window size", {str => maxWindow = str.toInt})
      opt("minFreq", "minimum frequency.", {str => minFreq = str.toInt})
    }

    if (!parser.parse(args)) return
    val relgrams = loadAndFilterRelgrams(inputPath, maxWindow, minFreq)
    exportRelgramCounts(relgrams, outputPath)

    // 10,000
    // ask
    // National Mediation Board on	Feb.
    // Delta
    // be since
    // September
    // NYT_ENG_20010309.0354-9-1
    // NYT_ENG_20010309.0354-23-1
    // 14:1
    // 10,000_CSEP_1
    // Feb._CSEP_1
    // Delta_CSEP_1
    // September_CSEP_1
    /**val filterStrings = TextInput.fromTextFile(inputPath).flatMap(line => {
      distributeCountsAndFilter(line, maxWindow, minFreq)
    })
    export(filterStrings, outputPath)*/
  }

  def distributeCountsAndFilter(line: String, maxWindow: Int, minFreq: Int): Option[String] = {
    val splits = line.split("\t")
    val first = splits.take(3).mkString("\t")
    val second = splits.slice(3, 6).mkString("\t")
    val fids = splits(6)
    val sids = splits(7)
    val counts = splits(8).split(",").map(wc => {
      val csplits = wc.split(":")
      csplits(0).toInt -> csplits(1).toInt
    })
    val dcounts = distributeCounts(counts, maxWindow)
    if (dcounts.max >= minFreq) {
      Some(first + "\t" + second + "\t" + dcounts.mkString("\t") + "\t" + fids + "\t" + sids + "\t" + splits.slice(9, 13).mkString("\t").replaceAll( """_CSEP_""", ":").replaceAll("""_ESEP_""", ","))
    } else {
      None
    }
  }


}

object FrequencyFilterTest{

  def main(args:Array[String]){
    val inputPath = args(0)
    val outputPath = args(1)
    val writer = new PrintWriter(outputPath)
    val relgramCounts = FrequencyFilter.loadAndFilterRelgramsLocal(inputPath, 50, 1)
    relgramCounts.foreach(rgc => {
      writer.println(rgc.serialize)
    })
    /**Source.fromFile(inputPath).getLines().foreach(line => {
      FrequencyFilter.distributeCountsAndFilter(line, 50, 3) match {
        case Some(string:String) => writer.println(string)
        case None =>
      }
    })
      */
    writer.close
  }
}
