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
  var extractor:RelgramsExtractor = null
  var counter:RelgramsCounter = null

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


  def loadTupleDocuments(inputPath:String) = {
    TextInput.fromTextFile(inputPath)
      .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
  }


  def extractRelgramCounts(tuplesDocuments: DList[TuplesDocumentWithCorefMentions], equality:Boolean, noequality:Boolean)={
    tuplesDocuments.flatMap(document => {
      val docid = document.tuplesDocument.docid
      try{
        extractor.extractRelgramsFromDocument(document)
      }catch{
        case e:Exception => { logger.error("Failed to extract relgrams from docid %s with exception:\n%s".format(docid, e.toString)); None}
      }
    })
  }


  def reduceRelgramCounts(relgramCounts: DList[(String, RelgramCounts)]) = {
    relgramCounts.map(x => (x._1, x._2))
                 .groupByKey[String, RelgramCounts]
                 .flatMap(x => counter.reduceRelgramCounts(x._2))
  }

  def run() {

    var inputPath, outputPath = ""
    var maxWindow = 10
    var equality = false
    var noequality = false

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window size", {str => maxWindow = str.toInt})
      opt("equality", "Argument equality tuples.", {str => equality = str.toBoolean})
      opt("noequality", "Count tuples without equality.", {str => noequality = str.toBoolean})
    }

    if (!parser.parse(args)) return

    assert(equality || noequality, "Both equality or noequality flags are false. One of them must be set true.")

    extractor = new RelgramsExtractor(maxWindow, equality, noequality)
    counter = new RelgramsCounter
    val tupleDocuments = loadTupleDocuments(inputPath)
    val relgramCounts = extractRelgramCounts(tupleDocuments, equality, noequality)
    val reducedRelgramCounts = reduceRelgramCounts(relgramCounts)
    export(reducedRelgramCounts, outputPath)
  }
}
