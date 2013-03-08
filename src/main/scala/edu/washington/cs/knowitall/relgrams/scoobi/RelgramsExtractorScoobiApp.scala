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
      val relgramsPath = outputPath + "-tdocs"
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
      val relgramsPath = outputPath + "-relgrams"
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
      val tuplesPath = outputPath + "-tuples"
      persist(TextOutput.toTextFile(tupleCounts.map(x => x.toString()), tuplesPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced tuples.")
        e.printStackTrace
      }

    }
  }

  //val extractor = new RelgramsExtractor(maxWindow, equality, noequality)
  def extractRelgramCountsAndTuples(tuplesDocuments: DList[TuplesDocumentWithCorefMentions],
                                    maxWindow:Int,
                                    equality:Boolean,
                                    noequality:Boolean): Option[DList[(Map[String, RelgramCounts], Map[String, RelationTuple])]] ={

    import TuplesDocumentWithCorefMentions._
    import RelgramCounts._
    import RelationTuple._


    val relgrams: DList[(Map[String, RelgramCounts], Map[String, RelationTuple])] = tuplesDocuments.map(document => {
      val extractor = new RelgramsExtractor(maxWindow, equality, noequality)
      //println("Processing doc: " + document.tuplesDocument.docid)
      val rgs = extractor.extractRelgramsFromDocument(document)
      //println("relgrams size: " + rgs._1.keys.size)
      rgs
    })
    /**try{
      val relgrams = tuplesDocuments.flatMap(document => {
        val docid = document.tuplesDocument.docid
        if (docid != null) println("Docid: " + docid)
        try{
          Some(extractor.extractRelgramsFromDocument(document))
        }catch{
          case e:Exception => {
            val d = if (docid != null) docid else "No docid present."
            println("Failed to extract relgrams from docid %s with exception:\n%s".format(d, e.getStackTraceString))
            None            //val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1))
    //val reducedTupleCounts = reduceTuplesCounts(extracts.map(x => x._2))

    //exportRelgrams(reducedRelgramCounts, outputPath)
    //exportTuples(reducedTupleCounts, outputPath)

          }
        }
      })
      Some(relgrams)
    }catch{
      case e:Exception => {
        println("Failed to extract relgrams.")
        None
      }
    }  */
    Some(relgrams)
  }
  def reduceRelgramCounts(relgramCounts: DList[Map[String, RelgramCounts]]): DList[RelgramCounts] = {
    val counter = new RelgramsCounter
    relgramCounts.flatten
                 .groupByKey[String, RelgramCounts]
                 .flatMap(x => counter.reduceRelgramCounts(x._2))
  }

  def reduceTuplesCounts(tuplesCounts: DList[Map[String, RelationTuple]]): DList[RelationTupleCounts] = {
    import RelationTupleCounts._
    val counter = new RelgramsCounter
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

    import RelgramCounts._
    var tupleDocuments = loadTupleDocuments(inputPath)

    /**val c:DList[String] = tupleDocuments.map(td => {
      val docid = td.tuplesDocument.docid
      println("docid: "+ docid)
      docid
    })
    persist(TextOutput.toTextFile(c, outputPath + "-dummy"))
      */
    ////exportTupleDocuments(tupleDocuments, outputPath + "-tdocs")
    import TuplesDocumentWithCorefMentions._
     extractRelgramCountsAndTuples(tupleDocuments, maxWindow, equality, noequality) match {
       case Some(extracts:DList[(Map[String, RelgramCounts], Map[String, RelationTuple])]) => {
         //exportRelgrams(extracts.flatMap(e => e._1.values), outputPath)
         val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1))
         val reducedTupleCounts = reduceTuplesCounts(extracts.map(x => x._2))

         exportRelgrams(reducedRelgramCounts, outputPath)
         exportTuples(reducedTupleCounts, outputPath)

       }
       case None => "Failed relgram extraction."
     }


    //val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1))
    //val reducedTupleCounts = reduceTuplesCounts(extracts.map(x => x._2))

    //exportRelgrams(reducedRelgramCounts, outputPath)
    //exportTuples(reducedTupleCounts, outputPath)


  }
}
