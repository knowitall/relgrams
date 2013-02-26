package edu.washington.cs.knowitall.relgrams.scoobi

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/25/13
 * Time: 10:06 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams.{TuplesDocumentWithCorefMentions, TuplesDocumentGenerator, TypedTuplesRecord}
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import com.nicta.scoobi.core.DList
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser

object TuplesDocumentsWithCorefScoobiApp extends ScoobiApp{

  def export(list: DList[TuplesDocumentWithCorefMentions], outputPath: String){
    import TuplesDocumentWithCorefMentions._
    try{
      persist(TextOutput.toTextFile(list.map(x => x.toString()), outputPath))
    }catch{
      case e:Exception => {
        println("Failed to persist reduced relgrams for type and head norm types.")
        e.printStackTrace
      }

    }
  }

  def run() {
    var inputPath, outputPath = ""
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    val tupleDocuments = TextInput.fromTextFile(inputPath)
                                 .flatMap(line =>TypedTuplesRecord.fromString(line))
                                 .groupBy(record => record.docid)
                                 .map(kv => TuplesDocumentGenerator.getPrunedTuplesDocumentWithCorefMentions(kv._1, kv._2.toSeq))
                                 //.map(td => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(td))

    export(tupleDocuments, outputPath)
  }
}
