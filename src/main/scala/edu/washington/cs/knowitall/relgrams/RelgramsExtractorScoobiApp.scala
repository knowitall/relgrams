package edu.washington.cs.knowitall.relgrams

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 7:05 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import com.nicta.scoobi.core.DList
import utils.MapUtils
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser
import org.slf4j.LoggerFactory


object RelgramsExtractorScoobiApp extends ScoobiApp{


  val logger = LoggerFactory.getLogger(this.getClass)


  def reduceRelgramCounts(groupedRelgramCounts: DList[(String, Iterable[RelgramCounts])]):DList[RelgramCounts] = {
    groupedRelgramCounts.flatMap(grgc => reduceRelgramCounts(grgc._2))
  }

  def haveDisjointHashes(mergeWith: RelgramCounts, toMerge: RelgramCounts): Boolean = {
    val a1 = mergeWith.relgram.first.hashes
    val a2 = mergeWith.relgram.second.hashes
    val b1 = toMerge.relgram.first.hashes
    val b2 = toMerge.relgram.second.hashes
    !(a1 exists b1) && !(a2 exists b2)
  }

  def updateHashes(mergeWith: RelgramCounts, toMerge: RelgramCounts){
    mergeWith.relgram.first.hashes ++= toMerge.relgram.first.hashes
    mergeWith.relgram.second.hashes ++= toMerge.relgram.second.hashes
  }

  def mergeArgCounts(mergeWith: RelgramCounts, toMerge: RelgramCounts) {
    MapUtils.addTo(mergeWith.argCounts.firstArg1Counts, toMerge.argCounts.firstArg1Counts)
    MapUtils.addTo(mergeWith.argCounts.firstArg2Counts, toMerge.argCounts.firstArg2Counts)
    MapUtils.addTo(mergeWith.argCounts.secondArg1Counts, toMerge.argCounts.secondArg1Counts)
    MapUtils.addTo(mergeWith.argCounts.secondArg1Counts, toMerge.argCounts.secondArg2Counts)
  }

  def mergeCounts(mergeWith: RelgramCounts, toMerge: RelgramCounts){
    MapUtils.addTo(mergeWith.counts, toMerge.counts)
  }

  def merge(mergeWith: RelgramCounts, toMerge: RelgramCounts){

    if(haveDisjointHashes(mergeWith, toMerge)){
      updateHashes(mergeWith, toMerge)
      mergeArgCounts(mergeWith, toMerge)
      mergeCounts(mergeWith, toMerge)
    }else{
      println("Not merging1: " + mergeWith)
      println("Not merging2: " + toMerge)
    }


  }

  def reduceRelgramCounts(rgcs:Iterable[RelgramCounts]) = {
    var outRGC:RelgramCounts = null
    rgcs.filter(rgc => {
      val filterVal = !RelgramCounts.isDummy(rgc)
      if(filterVal == false){
        println("Ignoring dummy rgc: " + rgc)
      }
      filterVal
    }).foreach(rgc => {
      if (outRGC == null){
        outRGC = rgc
      }else{
        merge(outRGC, rgc)
      }
    })
    println("outrgc: " + outRGC.prettyString)
    if(outRGC != null) Some(outRGC) else None
  }


  val relgramsExtractor = new RelgramsExtractor(10)
  def groupDocsAndExtract(inputPath:String, outputPath:String) = {
    import TypedTuplesRecord._
    val lines = TextInput.fromTextFile(inputPath)

    val groupedRecords = lines.flatMap(line => {
                                    //println("line: " + line)
                                    val recordOption = TypedTuplesRecord.fromString(line)
                                    //println("Read record: " + recordOption)
                                    recordOption
                                  }).groupBy(record => record.docid)

    import RelgramCounts._
    val relgramCounts = groupedRecords.flatMap(kv => {
      val records = kv._2
      relgramsExtractor.extractRelgrams(records.toSeq).map(stringRGC => (stringRGC._1, stringRGC._2))
    }).groupByKey[String, RelgramCounts]

    reduceRelgramCounts(relgramCounts)
  }


  def export(relgramCounts: DList[RelgramCounts], outputPath: String){
    try{
      import RelgramCounts._
      import TypedTuplesRecord._
      persist(TextOutput.toTextFile(relgramCounts.map(x => x.prettyString), outputPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams for type and head norm types.")
        e.printStackTrace
      }

    }
  }

  def run() {

    var inputPath, outputPath = ""
    var maxWindow = 10
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window size", {str => maxWindow = str.toInt})
    }

    if (!parser.parse(args)) return
    println("InputPath: " + inputPath)
    val relgramCounts = groupDocsAndExtract(inputPath, outputPath)
    //println(relgramCounts.length.m.toString)
    export(relgramCounts, outputPath)

  }
}
