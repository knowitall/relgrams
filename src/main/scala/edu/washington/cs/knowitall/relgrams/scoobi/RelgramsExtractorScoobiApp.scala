package edu.washington.cs.knowitall.relgrams.scoobi

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/18/13
 * Time: 7:05 PM
 * To change this template use File | Settings | File Templates.
 */


import com.nicta.scoobi.application.ScoobiApp
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import com.nicta.scoobi.core.DList

import com.nicta.scoobi.Persist._
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.relgrams._
import scopt.mutable.OptionParser


object RelgramsExtractorScoobiApp extends ScoobiApp{

  import TypedTuplesRecord._
  import RelgramCounts._


  val logger = LoggerFactory.getLogger(this.getClass)
  val extractor = new RelgramsExtractor(maxWindow = 10)
  val counter = new RelgramsCounter


  def reduceRelgramCounts(groupedRelgramCounts: DList[(String, Iterable[RelgramCounts])]):DList[RelgramCounts] = {
    groupedRelgramCounts.flatMap(x => counter.reduceRelgramCounts(x._2))
  }


  def groupedTypedTuplesRecord(inputPath:String) = {
    //import TypedTuplesRecord._
    TextInput.fromTextFile(inputPath)
             .flatMap(line =>TypedTuplesRecord.fromString(line))
             .groupBy(record => record.docid)
  }
 /** def groupDocsAndExtract(inputPath:String, outputPath:String) = {

    val groupedRecords = groupedTypedTuplesRecord(inputPath)
    val relgramCounts = groupedRecords.flatMap(kv => {
      val docid = kv._1
      val records = kv._2
      try{
        extractor.extractRelgrams(records.toSeq)
                 .map(x => (x._1, x._2))
      }catch{
        case e:Exception => { logger.error("Failed to extract relgrams from docid %s with exception:\n%s".format(docid, e.toString)); None}
      }
    }).groupByKey[String, RelgramCounts]

    reduceRelgramCounts(relgramCounts)
  }   */


  def export(relgramCounts: DList[RelgramCounts], outputPath: String){
    try{
      persist(TextOutput.toTextFile(relgramCounts.map(x => x.prettyString), outputPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams for type and head norm types.")
        e.printStackTrace
      }

    }
  }

  def fromTuplesDocuments(inputPath: String) = {
    val tuplesDocuments = TextInput.fromTextFile(inputPath)
                                   .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
    val relgramCounts = tuplesDocuments.flatMap(document => {
      val docid = document.tuplesDocument.docid
     try{
        extractor.extractRelgramsFromDocument(document)
                 .map(x => (x._1, x._2))
      }catch{
        case e:Exception => { logger.error("Failed to extract relgrams from docid %s with exception:\n%s".format(docid, e.toString)); None}
      }
    }).groupByKey[String, RelgramCounts]

    reduceRelgramCounts(relgramCounts)
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
    val relgramCounts = fromTuplesDocuments(inputPath)//groupDocsAndExtract(inputPath, outputPath)
    export(relgramCounts, outputPath)
  }
}
