package edu.washington.cs.knowitall.relgrams

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/6/13
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import scopt.mutable.OptionParser
import io.Source
import java.io.{PrintWriter, File}
import scopt.mutable.OptionParser
import scala.Some


object RelgramsLocalApp {

  def main(args:Array[String]){

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

    extractor = new RelgramsExtractor(maxWindow, equality, noequality)
    counter = new RelgramsCounter(maxSize)
    val tupleDocuments = loadTupleDocuments(inputPath)
    val extracts = extractRelgramCountsAndTuples(tupleDocuments, equality, noequality)
    val reducedRelgramCounts = reduceRelgramCounts(extracts.map(x => x._1))
    val reducedTupleCounts = reduceTuplesCounts(extracts.map(x => x._2))
    exportRelgrams(reducedRelgramCounts, outputPath)
    exportTuples(reducedTupleCounts, outputPath)
  }


  val logger = LoggerFactory.getLogger(this.getClass)
  var extractor:RelgramsExtractor = null
  var counter:RelgramsCounter = null


  def loadTupleDocuments(inputPath:String) = {
    Source.fromFile(inputPath).getLines().flatMap(x => TuplesDocumentWithCorefMentions.fromString(x)).toSeq
  }
  def extractRelgramCountsAndTuples(tuplesDocuments: Seq[TuplesDocumentWithCorefMentions], equality:Boolean, noequality:Boolean): Seq[(Map[String, RelgramCounts], Map[String, RelationTupleCounts])] ={
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

  def reduceRelgramCounts(relgramCounts: Iterable[Map[String, RelgramCounts]]) = {
    relgramCounts.flatten
                 .groupBy(x => x._1)
                 .flatMap(x => counter.reduceRelgramCounts(x._2.map(y => y._2)))
  }

  def reduceTuplesCounts(tuplesCounts: Iterable[Map[String, RelationTupleCounts]]) = {
    import RelationTupleCounts._
    tuplesCounts.flatten
                .groupBy(x => x._1)
                .flatMap(x => counter.reduceTupleCounts(x._2.map(y => y._2)))
  }

  def exportRelgrams(relgramCounts: Iterable[RelgramCounts], outputPath: String){
    try{
      val relgramsPath = outputPath + File.separator + "relgrams.txt"
      val writer = new PrintWriter(relgramsPath, "utf-8")
      writer.print(relgramCounts.map(x => x.prettyString).mkString("\n"))
      writer.close
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams.")
        e.printStackTrace
      }

    }
  }

  def exportTuples(tupleCounts: Iterable[RelationTupleCounts], outputPath: String){
    try{
      val tuplesPath = outputPath + File.separator + "tuples.txt"
      val writer = new PrintWriter(tuplesPath, "utf-8")
      writer.print(tupleCounts.map(x => x.toString()).mkString("\n"))
      writer.close
    }catch{
      case e:Exception => {
        println("Failed to persist reduced tuples.")
        e.printStackTrace
      }

    }
  }

}
