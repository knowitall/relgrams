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

import edu.washington.cs.knowitall.relgrams.{UndirRelgramCounts, RelgramCounts}
import xml.Elem
import dispatch.Http
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}
import org.apache.solr.common.SolrInputDocument
import org.apache.solr.client.solrj.SolrServer
import org.jets3t.service.impl.rest.XmlResponsesSaxParser

object ToSolrDocument {
   val logger = LoggerFactory.getLogger(this.getClass)

  /**
   *
  <add>
  <doc>
    <field name="id">SOLR1000</field>
    <field name="farg1">First arg1</field>
    <field name="frel">eats</field>
    <field name="farg2">second</field>
    <field name="sarg1">Second arg1</field>
    <field name="srel">also eats</field>
    <field name="sarg2">second arg2</field>
    <field name="count">10</field>
  </doc>
  </add>
   */
  var id = 0
  def toSolrXML(rgc: RelgramCounts) = {
    val farg1 = rgc.relgram.first.arg1
    val frel = rgc.relgram.first.rel
    val farg2 = rgc.relgram.first.arg2
    val sarg1 = rgc.relgram.second.arg1
    val srel = rgc.relgram.second.rel
    val sarg2 = rgc.relgram.second.arg2

    val countsString = rgc.counts.keys.toSeq.sortBy(w => w).map(w => rgc.counts.getOrElse(w, 0))
    <add>
      <doc>
        <field name="id">{id}</field>
        <field name="farg1">{farg1}</field>
        <field name="frel">{frel}</field>
        <field name="farg2">{farg2}</field>
        <field name="sarg1">{sarg1}</field>
        <field name="srel">{srel}</field>
        <field name="sarg2">{sarg2}</field>
        <field name="counts">{countsString}</field>
      </doc>
    </add>
  }

  val http = new Http
  var solrServer:HttpSolrServer = null
  import dispatch._
  def addToIndex(undirrgc:UndirRelgramCounts) = {
    val rgc = undirrgc.rgc

    val farg1 = rgc.relgram.first.arg1
    val frel = rgc.relgram.first.rel
    val farg2 = rgc.relgram.first.arg2
    val sarg1 = rgc.relgram.second.arg1
    val srel = rgc.relgram.second.rel
    val sarg2 = rgc.relgram.second.arg2
    //val countsString = rgc.counts.keys.toSeq.sortBy(w => w).map(w => rgc.counts.getOrElse(w, 0))
    //val countsString = rgc.counts.keys.toSeq.sortBy(w => w).map(w => rgc.counts.getOrElse(w, 0))
    val solrDoc = new SolrInputDocument
    solrDoc.addField("id", id)
    solrDoc.addField("farg1", farg1)
    solrDoc.addField("frel", frel)
    solrDoc.addField("farg2", farg2)
    solrDoc.addField("sarg1", sarg1)
    solrDoc.addField("srel", srel)
    solrDoc.addField("sarg2", sarg2)
    //solrDoc.addField("counts", countsString)
    solrDoc.addField("serialize", undirrgc.serialize)
    solrServer.add(solrDoc)

  }


  def main(args:Array[String]){

    val inputPath = args(0)
    val solrPath = args(1)

    solrServer = new HttpSolrServer(solrPath)
    solrServer.setParser(new XMLResponseParser())
    Source.fromFile(inputPath).getLines().foreach(line => {
      UndirRelgramCounts.fromSerializedString(line) match {
        case Some(rgc:UndirRelgramCounts) => {
          val response = addToIndex(rgc)
          println("response: " + response)
          id = id + 1
        }
        case None =>
      }
    })
    solrServer.commit()


  }
}
