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
import io.Source


object RelgramsExtractorScoobiApp extends ScoobiApp{

  import TypedTuplesRecord._
  import RelgramCounts._


  val logger = LoggerFactory.getLogger(this.getClass)
  var extractor:RelgramsExtractor = null
  var counter:RelgramsCounter = null

  def exportTupleDocuments(documents: DList[TuplesDocumentWithCorefMentions], outputPath: String){
    try{
      val relgramsPath = outputPath + File.separator + "tdocs"
      persist(TextOutput.toTextFile(documents.map(doc => doc.toString), relgramsPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams.")
        e.printStackTrace
      }

    }
  }

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

  def extractRelgramCountsAndTuples(tuplesDocuments: DList[TuplesDocumentWithCorefMentions],
                                    maxWindow:Int,
                                    equality:Boolean,
                                    noequality:Boolean): Option[DList[(Map[String, RelgramCounts], Map[String, RelationTuple])]] ={

    import TuplesDocumentWithCorefMentions._
    import RelgramCounts._
    import RelationTuple._
    val relgrams: DList[(Map[String, RelgramCounts], Map[String, RelationTuple])] = tuplesDocuments.map(document => {
      val extractor = new RelgramsExtractor(maxWindow, equality, noequality)
      val rgs = extractor.extractRelgramsFromDocument(document)
      rgs
    })
    Some(relgrams)
  }
  def reduceRelgramCounts(relgramCounts: DList[Map[String, RelgramCounts]], maxSize:Int = 5): DList[RelgramCounts] = {
    val counter = new RelgramsCounter(maxSize)
    relgramCounts.flatten
                 .groupByKey[String, RelgramCounts]
                 .flatMap(x => counter.reduceRelgramCounts(x._2))
  }

  def reduceTuplesCounts(tuplesCounts: DList[Map[String, RelationTuple]], maxSize:Int = 5): DList[RelationTupleCounts] = {
    import RelationTupleCounts._
    val counter = new RelgramsCounter(maxSize)
    tuplesCounts.flatten
      .groupByKey[String, RelationTuple]
      .flatMap(x => counter.reduceTuples(x._2))
  }


  def loadTupleDocuments(inputPath: String) = {
    val input = TextInput.fromTextFile(inputPath)
    input.flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
  }

  def run() {

    var inputPath, outputPath = ""
    var maxWindow = 10
    var maxSize = 5
    var equality = false
    var noequality = false

    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window size", {str => maxWindow = str.toInt})
      opt("maxSize", "max size for id's arg head values etc.", {str => maxSize = str.toInt})
      opt("equality", "Argument equality tuples.", {str => equality = str.toBoolean})
      opt("noequality", "Count tuples without equality.", {str => noequality = str.toBoolean})
    }

    if (!parser.parse(args)) return

    assert(equality || noequality, "Both equality or noequality flags are false. One of them must be set true.")

    import TuplesDocumentWithCorefMentions._
    var tupleDocuments = loadTupleDocuments(inputPath)

    import RelgramCounts._
    extractRelgramCountsAndTuples(tupleDocuments, maxWindow, equality, noequality) match {
       case Some(extracts:DList[(Map[String, RelgramCounts], Map[String, RelationTuple])]) => {

         val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1), maxSize)
         val reducedTupleCounts   = reduceTuplesCounts(extracts.map(x => x._2), maxSize)

         exportRelgrams(reducedRelgramCounts, outputPath)
         exportTuples(reducedTupleCounts, outputPath)

       }
       case None => "Failed relgram extraction."
     }
  }
}
