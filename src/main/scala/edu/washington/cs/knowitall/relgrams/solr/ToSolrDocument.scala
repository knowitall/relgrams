package edu.washington.cs.knowitall.relgrams.solr

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/11/13
 * Time: 4:07 PM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory

import io.Source

import edu.washington.cs.knowitall.relgrams.{AffinityMeasures, Measures}
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}
import org.apache.solr.common.SolrInputDocument

class ToSolrDocument(solrServer:HttpSolrServer) {
   val logger = LoggerFactory.getLogger(this.getClass)
  def addToIndex(id:Int, measures:Measures, affinities:AffinityMeasures) = {
    val undirrgc = measures.urgc
    val rgc = undirrgc.rgc
    val farg1 = rgc.relgram.first.arg1
    val frel = rgc.relgram.first.rel
    val farg2 = rgc.relgram.first.arg2
    val sarg1 = rgc.relgram.second.arg1
    val srel = rgc.relgram.second.rel
    val sarg2 = rgc.relgram.second.arg2
    val solrDoc = new SolrInputDocument
    solrDoc.addField("id", id)
    solrDoc.addField("farg1", farg1)
    solrDoc.addField("frel", frel)
    solrDoc.addField("farg2", farg2)
    solrDoc.addField("sarg1", sarg1)
    solrDoc.addField("srel", srel)
    solrDoc.addField("sarg2", sarg2)
    solrDoc.addField("serialize", measures.serialize)
    solrDoc.addField("affinities", affinities.serialize)
    solrServer.add(solrDoc)
  }

}

object ToSolrDocument{

  def main(args:Array[String]){

    val inputPath = args(0)
    val solrPath = args(1)
    val windowAlpha = args(2).toDouble
    val smoothingDelta = args(3).toDouble

    val solrServer = new HttpSolrServer(solrPath)
    solrServer.setParser(new XMLResponseParser())
    val tosol = new ToSolrDocument(solrServer)
    var id = 0
    Source.fromFile(inputPath).getLines().foreach(line => {
      Measures.fromSerializedString(line) match {
        case Some(measures:Measures) => {
          AffinityMeasures.fromMeasures(measures, windowAlpha, smoothingDelta) match {
            case Some(affinities:AffinityMeasures) => {
              val response = tosol.addToIndex(id, measures, affinities)
              println("response: " + response)
              id = id + 1
            }
            case _ =>
          }
        }
        case _ =>
      }
    })
    solrServer.commit()
  }
}
