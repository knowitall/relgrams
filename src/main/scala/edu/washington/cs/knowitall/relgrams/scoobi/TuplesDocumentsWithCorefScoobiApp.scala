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
import org.slf4j.LoggerFactory

object TuplesDocumentsWithCorefScoobiApp extends ScoobiApp{

  val applogger = LoggerFactory.getLogger(this.getClass)
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
    var corefTimeoutMs = 1000 * 30 //30 seconds.
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("fromDocs", "from serialized tuple documents?", { str => fromDocs = str.toBoolean})
      opt("corefTimeoutMs", "coref time out in milli-seconds.", { str => corefTimeoutMs = str.toInt})
    }

    if (!parser.parse(args)) return
    val tgen = new TuplesDocumentGenerator(corefTimeoutMs)
    import TuplesDocument._
    import TuplesDocumentWithCorefMentions._
    if (!fromDocs){

      println("Building TupleDocuments.")
      applogger.info("Building TupleDocuments.")
      val tupledocuments = TextInput.fromTextFile(inputPath)
                                              .flatMap(line =>TypedTuplesRecord.fromString(line))
                                              .groupBy(record => record.docid)
                                              .map(x => tgen.getPrunedDocument(x._1, x._2.toSeq))
      export(tupledocuments, outputPath)
    }else if (fromDocs) {
      println("Building TupleDocumentsWithCorefMentions.")
      applogger.info("Building TupleDocumentsWithCorefMentions.")

      val tupleDocuments = TextInput.fromTextFile(inputPath)
                                              .flatMap(line => TuplesDocument.fromString(line) match {
                                                case Some(x:TuplesDocument) => if(x.tupleRecords.size <= 100) Some(x) else {
                                                  println("Ignoring document: %s with size %d".format(x.docid, x.tupleRecords.size))
                                                  None
                                                }
                                                case None => None
                                              })

      val tupleDocumentsWithCorefs = tupleDocuments.flatMap(document => tgen.getTuplesDocumentWithCorefMentionsBlocks(document))

      exportWithCorefs(tupleDocumentsWithCorefs, outputPath)

    }
  }
}
