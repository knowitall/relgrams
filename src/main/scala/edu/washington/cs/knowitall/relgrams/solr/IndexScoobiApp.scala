package edu.washington.cs.knowitall.relgrams.solr

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/13/13
 * Time: 11:31 AM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import edu.washington.cs.knowitall.relgrams._
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import scopt.mutable.OptionParser
import scala.Some
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser
import scala.Some
import org.apache.solr.client.solrj.impl.{XMLResponseParser, HttpSolrServer}
import collection.mutable
import com.nicta.scoobi.core.DList


object IndexScoobiApp extends ScoobiApp {

  val logger = LoggerFactory.getLogger(this.getClass)

  def run() {

    var inputPath, outputPath = ""
    var windowAlpha = 0.9
    var smoothingDelta = 1000.0
    var minFreq = 5
    var solrURL = ""
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("solr", "solr server path", {str => solrURL = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str})
      opt("minFreq", "minimum frequency for relgram.", {str => minFreq = str.toInt})
      opt("windowAlpha", "alpha decay for combining measures from different windows.", {str => windowAlpha = str.toDouble})
      opt("smoothingDelta", "smoothing delta.", {str => smoothingDelta = str.toDouble})
    }

    if (!parser.parse(args)) return

    println("solrURL: " + solrURL)
    def aboveThreshold(measures:Measures) = measures.urgc.bitermCounts.values.max > minFreq

    val tosolrs = new mutable.HashMap[Thread, ToSolrDocument] with mutable.SynchronizedMap[Thread, ToSolrDocument]
    def getToSolr(solrURL:String) = {
      val solrServer = new HttpSolrServer(solrURL)
      solrServer.setParser(new XMLResponseParser())
      new ToSolrDocument(solrServer)
    }

    import Measures._
    val output:DList[Measures]  = TextInput.fromTextFile(inputPath).flatMap(line => Measures.fromSerializedString(line) match {
      case Some(measures:Measures) => {
        if (aboveThreshold(measures)){

          AffinityMeasures.fromMeasures(measures, windowAlpha, smoothingDelta) match {
            case Some(affinities:AffinityMeasures) => {
              val tosolr = tosolrs.getOrElseUpdate(Thread.currentThread(), getToSolr(solrURL))
              val id = measures.urgc.rgc.relgram.prettyString.hashCode + System.currentTimeMillis()
              tosolr.addToIndex(id.toInt, measures, affinities)
              None
              //Some(measures)
            }
            case _ => None
          }
        }else{
          None
        }
      }
      case _ => None
    })

    persist(TextOutput.toTextFile(output, outputPath))

  }

}
