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
import java.io.File


object RelgramsExtractorScoobiApp extends ScoobiApp{

  import TypedTuplesRecord._
  import RelgramCounts._


  val logger = LoggerFactory.getLogger(this.getClass)
  var extractor:RelgramsExtractor = null
  var counter:RelgramsCounter = null

  def exportRelgrams(relgramCounts: DList[RelgramCounts], outputPath: String){
    try{
      val relgramsPath = outputPath + File.separator + "relgrams"
      persist(TextOutput.toTextFile(relgramCounts.map(x => x.prettyString), relgramsPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams.")
        e.printStackTrace
      }

    }
  }

  def exportTuples(tupleCounts: DList[RelationTupleCounts], outputPath: String){
    try{
      val tuplesPath = outputPath + File.separator + "tuples"
      persist(TextOutput.toTextFile(tupleCounts.map(x => x.toString()), tuplesPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced tuples.")
        e.printStackTrace
      }

    }
  }

  def loadTupleDocuments(inputPath:String) = {
    TextInput.fromTextFile(inputPath)
      .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
  }


  def extractRelgramCountsAndTuples(tuplesDocuments: DList[TuplesDocumentWithCorefMentions], equality:Boolean, noequality:Boolean): DList[(Map[String, RelgramCounts], Map[String, RelationTuple])] ={
    tuplesDocuments.flatMap(document => {
      val docid = document.tuplesDocument.docid
      try{
        Some(extractor.extractRelgramsFromDocument(document))
      }catch{
        case e:Exception => {
          logger.error("Failed to extract relgrams from docid %s with exception:\n%s".format(docid, e.toString))
          None
        }
        case _ => None
      }
    })
  }


  /**def reduceRelgramCounts(relgramCounts: DList[(String, RelgramCounts)]) = {
    relgramCounts.map(x => (x._1, x._2))
                 .groupByKey[String, RelgramCounts]
                 .flatMap(x => counter.reduceRelgramCounts(x._2))
  }*/


  def reduceRelgramCounts(relgramCounts: DList[Map[String, RelgramCounts]]) = {
    relgramCounts.flatten
                 .groupByKey[String, RelgramCounts]
                 .flatMap(x => counter.reduceRelgramCounts(x._2))
  }

  def reduceTuplesCounts(tuplesCounts: DList[Map[String, RelationTuple]]) = {
    import RelationTupleCounts._
    tuplesCounts.flatten
      .groupByKey[String, RelationTuple]
      .flatMap(x => counter.reduceTuples(x._2))
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
    val extracts:DList[(Map[String, RelgramCounts], Map[String, RelationTuple])] = extractRelgramCountsAndTuples(tupleDocuments, equality, noequality)
    val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1))
    val reducedTupleCounts = reduceTuplesCounts(extracts.map(x => x._2))
    exportRelgrams(reducedRelgramCounts, outputPath)
    exportTuples(reducedTupleCounts, outputPath)


  }
}
