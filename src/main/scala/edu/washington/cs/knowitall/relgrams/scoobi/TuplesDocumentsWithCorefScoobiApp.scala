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
import edu.washington.cs.knowitall.relgrams.{TuplesDocument, TuplesDocumentWithCorefMentions, TuplesDocumentGenerator, TypedTuplesRecord}
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import com.nicta.scoobi.core.DList
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser

object TuplesDocumentsWithCorefScoobiApp extends ScoobiApp{

  def exportWithCorefs(list: DList[TuplesDocumentWithCorefMentions], outputPath: String){
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

  def export(list: DList[TuplesDocument], outputPath: String){
    import TuplesDocument._
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
    var fromDocs = false
    var docorefs = false
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("fromDocs", "from serialized tuple documents?", { str => fromDocs = str.toBoolean})
      //opt("docorefs", "do coreference mentions?", { str => docorefs = str.toBoolean})
    }

    if (!parser.parse(args)) return

    import TuplesDocument._
    if (!fromDocs){

      val tupledocuments = TextInput.fromTextFile(inputPath)
                                              .flatMap(line =>TypedTuplesRecord.fromString(line))
                                              .groupBy(record => record.docid)
                                              .map(x => TuplesDocumentGenerator.getPrunedDocument(x._1, x._2.toSeq))
      export(tupledocuments, outputPath)

    }else if (fromDocs) {
      val tupleDocumentsWithCorefs = TextInput.fromTextFile(inputPath)
                                              .flatMap(line => TuplesDocument.fromString(line))
                                              .map(document => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(document))
      exportWithCorefs(tupleDocumentsWithCorefs, outputPath)

    }
                                 //.map(td => TuplesDocumentGenerator.getTuplesDocumentWithCorefMentions(td))
  }
}
