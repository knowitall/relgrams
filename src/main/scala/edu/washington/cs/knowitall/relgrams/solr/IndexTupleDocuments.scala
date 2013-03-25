package edu.washington.cs.knowitall.relgrams.solr

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/24/13
 * Time: 8:43 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams.{TuplesDocument, TuplesDocumentWithCorefMentions}
import com.nicta.scoobi.core.DList
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}
import org.slf4j.LoggerFactory
import org.apache.solr.common.SolrInputDocument
import collection.mutable

import com.nicta.scoobi.Persist._

class ToSolrTuplesDocument(solrServer:HttpSolrServer) {
  val logger = LoggerFactory.getLogger(this.getClass)
  def addToIndex(id:Int, document:TuplesDocument) {
    document.tupleRecords.foreach(record => {
    val solrDoc = new SolrInputDocument
    solrDoc.addField("id", document.docid + "-" + record.sentid + "-" + record.extrid)
    solrDoc.addField("docid", document.docid)
    solrDoc.addField("serialize", record.toString)
    solrDoc.addField("sentence", record.sentence)
    solrDoc.addField("arg1", record.arg1)
    solrDoc.addField("arg1Head", record.arg1Head)
    solrDoc.addField("rel", record.rel)
    solrDoc.addField("relHead", record.relHead)
    solrDoc.addField("arg2", record.arg2)
    solrDoc.addField("arg2Head", record.arg2Head)
    solrDoc.addField("arg1Types", record.arg1Types.mkString(","))
    solrDoc.addField("arg2Types", record.arg2Types.mkString(","))
    solrServer.add(solrDoc)
    })
  }

}


object IndexTupleDocuments extends ScoobiApp{

  def run() {
    var inputPath, solrURL, outputPath = ""
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("solr", "solr server path", {str => solrURL = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
    }
    if (!parser.parse(args)) return
    import TuplesDocumentWithCorefMentions._
    println("This is run once....")
    def loadTupleDocuments(inputPath: String):DList[TuplesDocumentWithCorefMentions] = {
      TextInput.fromTextFile(inputPath)
        .flatMap(x => TuplesDocumentWithCorefMentions.fromString(x))
    }
    val tosolrs = new mutable.HashMap[Thread, ToSolrTuplesDocument] with mutable.SynchronizedMap[Thread, ToSolrTuplesDocument]
    def getToSolr(solrURL:String) = {
      val solrServer = new HttpSolrServer(solrURL)
      solrServer.setParser(new XMLResponseParser())
      new ToSolrTuplesDocument(solrServer)
    }

    val tupleDocuments = loadTupleDocuments(inputPath)
    val out:DList[String] = tupleDocuments.flatMap(td => {
      val tosolr = tosolrs.getOrElseUpdate(Thread.currentThread(), getToSolr(solrURL))
      tosolr.addToIndex(td.tuplesDocument.docid.hashCode, td.tuplesDocument)
      None
    })
    persist(TextOutput.toTextFile(out, outputPath))

  }
}
