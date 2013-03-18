package edu.washington.cs.knowitall.relgrams.solr

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/12/13
 * Time: 12:21 PM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import edu.washington.cs.knowitall.relgrams.web.{MeasureName, RelgramsQuery}
import org.apache.solr.client.solrj.{SolrQuery, SolrServer}
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}

import scala.collection.JavaConversions._
import edu.washington.cs.knowitall.relgrams.{AffinityMeasures, Measures, RelationTuple}

import javax.xml.stream.XMLResolver

class SolrSearchWrapper(solrBaseUrl:String) {

  val logger = LoggerFactory.getLogger(this.getClass)

  val server = new HttpSolrServer(solrBaseUrl)
  server.setParser(new XMLResponseParser)

  def toSolrQuery(query:RelgramsQuery):Option[SolrQuery] = {
    val solrQuery = new SolrQuery()
    solrQuery.setQuery("*:*")
    solrQuery.setRows(10000000)
    def isEmpty(string:String) = string.isEmpty

    if (!isEmpty(query.relationTuple.arg1)) solrQuery.addFilterQuery("farg1:" + query.relationTuple.arg1)
    if (!isEmpty(query.relationTuple.rel)) solrQuery.addFilterQuery("frel:" + query.relationTuple.rel)
    if (!isEmpty(query.relationTuple.arg2)) solrQuery.addFilterQuery("farg2:" + query.relationTuple.arg2)
    if (solrQuery.getFilterQueries.size > 0){

      Some(solrQuery)
    }else{
      None
    }

    //solrQuery.addSort("farg1", SolrQuery.ORDER.asc )
    //solrQuery.addSort("frel", SolrQuery.ORDER.asc )
    //solrQuery.addSort("farg2", SolrQuery.ORDER.asc )
  }

  import dispatch._
  def search(query:RelgramsQuery): Seq[(Measures, AffinityMeasures)] = {
    toSolrQuery(query) match {
      case Some(solrQuery:SolrQuery) => {
        logger.info("RelgramsQuery: " + query.toHTMLString)
        logger.info("SolrQuery: " + solrQuery)
        val results = server.query(solrQuery)
        logger.info("Query: %s returned %d solr documents.".format(solrQuery.toString, results.getResults.size))
        results.getResults.flatMap(result => {
          Measures.fromSerializedString(result.getFirstValue("serialize").toString) match {
            case Some(m:Measures) => {
              AffinityMeasures.fromSerializedString(result.getFirstValue("affinities").toString) match {
                case Some(affinities:AffinityMeasures) => {
                  Some((m, affinities))
                }
                case None => None
              }
            }

            case None => None
          }
        })
      }
      case None => Seq[(Measures, AffinityMeasures)]()
    }


  }

}

object SolrSearchWrapperTest{
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args:Array[String]){
    val url = args(0)
    val server = new SolrSearchWrapper(url)
    val query = new RelgramsQuery(RelationTuple.fromArg1RelArg2("", "pay", ""),MeasureName.bigram, 10, 0.5, 0.5, "ARG1RELARG2", "conditional", "both")
    logger.info("relgramsQuery: " + query.toHTMLString)
    val results = server.search(query)
    logger.info(results.mkString(","))
  }
}
