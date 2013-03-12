package edu.washington.cs.knowitall.relgrams.scoobi

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/12/13
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import com.nicta.scoobi.application.ScoobiApp
import scopt.mutable.OptionParser
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import edu.washington.cs.knowitall.relgrams.{UndirRelgramCounts, Relgram, RelgramCounts}
import com.nicta.scoobi.core.DList
import edu.washington.cs.knowitall.relgrams.utils.MapUtils
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser
import io.Source
import collection.immutable
import java.io.PrintWriter

object RelgramMeasuresScoobiApp extends ScoobiApp {



  val logger = LoggerFactory.getLogger(this.getClass)

  def loadRelgramCounts(relgramsPath:String) = {
    import RelgramCounts._
    TextInput.fromTextFile(relgramsPath)
             .flatMap(line => fromSerializedString(line))
  }

  def lexicallyOrderedKey(relgram: Relgram) = {
    val first = relgram.first.prettyString
    val second = relgram.second.prettyString
    if (first.compareTo(second) > 0){
      first + "\t" + second
    }else{
      second + "\t" + first
    }
  }
  def toUndirRelgramCounts(rgcs: Iterable[RelgramCounts]) = {
    val seq = rgcs.toSeq
    val ab = seq(0)
    if (seq.size == 2){
      val ba = seq(1)
      val bitermCounts = MapUtils.combine(ab.counts, ba.counts)
      new UndirRelgramCounts(ab, bitermCounts)::new UndirRelgramCounts(ba, bitermCounts)::Nil
    }else{
      new UndirRelgramCounts(ab, ab.counts)::Nil
    }

  }

  def toUndirRelgramCounts(relgramCounts: DList[RelgramCounts]):DList[UndirRelgramCounts] = {

    import RelgramCounts._
    import UndirRelgramCounts._
    relgramCounts.groupBy(rgc => lexicallyOrderedKey(rgc.relgram))
                 .flatMap(grp => toUndirRelgramCounts(grp._2))
  }

  def export(undirCounts: DList[UndirRelgramCounts], outputPath: String){
    import UndirRelgramCounts._
    try{
      persist(TextOutput.toTextFile(undirCounts.map(x => x.serialize), outputPath))
    }catch{
      case e:Exception => {
        println("Failed to persist undir counts to path: " + outputPath)
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

    import RelgramCounts._
    import UndirRelgramCounts._
    val relgramCounts = loadRelgramCounts(inputPath)
    val undirCounts = toUndirRelgramCounts(relgramCounts)
    export(undirCounts, outputPath)
  }
}

object RelgramMeasuresTest{

  def main(args:Array[String]){
    var inputPath, outputPath = ""
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
    }

    if (!parser.parse(args)) return

    def exportLocal(undirCounts: immutable.Iterable[UndirRelgramCounts], outputPath: String){
      val writer = new PrintWriter(outputPath)
      undirCounts.foreach(uc => writer.println(uc.toString))
      writer.close
    }

    import RelgramCounts._
    import UndirRelgramCounts._
    val undirCounts = Source.fromFile(inputPath)
                              .getLines
                              .flatMap(line => RelgramCounts.fromSerializedString(line))//RelgramMeasuresScoobiApp.loadRelgramCounts(inputPath)
                              .toSeq
                              .groupBy(rgc => RelgramMeasuresScoobiApp.lexicallyOrderedKey(rgc.relgram))
                              .flatMap(grp => RelgramMeasuresScoobiApp.toUndirRelgramCounts(grp._2))

    exportLocal(undirCounts, outputPath)

  }
}
