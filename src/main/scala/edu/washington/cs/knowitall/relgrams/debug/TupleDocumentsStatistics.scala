package edu.washington.cs.knowitall.relgrams.debug

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/22/13
 * Time: 12:44 PM
 * To change this template use File | Settings | File Templates.
 */


import scala.collection.JavaConversions._
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import edu.washington.cs.knowitall.relgrams.TuplesDocumentWithCorefMentions
import com.nicta.scoobi.Persist._
import com.nicta.scoobi.core.{DObject, DList}

object TupleDocumentsStatistics extends ScoobiApp{

  def run() {

    var inputPath, outputPath = ""
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
    }

    if (!parser.parse(args)) return
    val tupleDocuments = TextInput.fromTextFile(inputPath).map(line => TuplesDocumentWithCorefMentions.fromString(line))
    val numDocs = persist(tupleDocuments.count(x => x.isDefined))
    val failedDocumentsDList: DObject[Int] = tupleDocuments.count(x => !x.isDefined)

    val failedDocs = try { persist(failedDocumentsDList) } catch { case e:Exception => 0}

    def bin(size:Int) = if (size < 11) 10 else if (size < 21) 20 else if (size < 31) 30 else if (size < 41) 40 else if (size < 51) 50 else if (size < 61) 60 else if (size < 71) 70 else if (size < 81) 80 else if (size < 91) 90 else 100
    val groupedRecords: DList[(Int, Iterable[Int])] = tupleDocuments.flatMap(x => x)
                                                                    .map(doc => (bin(doc.tuplesDocument.tupleRecords.size), 1))
                                                                    .groupByKey[Int, Int]
    def combineFunc(x:Int, y:Int) = x + y
    val docSizesDList: DList[(Int, Int)] = groupedRecords.combine[Int, Int](combineFunc)
                                       //.combine((a: Int, b: Int) => a + b)

    val docsizes  = persist(TextOutput.toTextFile(docSizesDList.map(x => x._1 + "\t" + x._2), outputPath))
    println("%s\t%s\t%s".format("Num docs", "Failed Docs"))
    println("%s\t%s\t%s".format(numDocs, failedDocs))
  }
}
