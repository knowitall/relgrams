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
import com.nicta.scoobi.io.text.{TextOutput, TextInput}
import edu.washington.cs.knowitall.relgrams._
import com.nicta.scoobi.core.DList
import edu.washington.cs.knowitall.relgrams.utils.MapUtils
import com.nicta.scoobi.Persist._
import scopt.mutable.OptionParser
import io.Source
import collection.{mutable, immutable}
import java.io.{File, PrintWriter}
import scopt.mutable.OptionParser
import scala.Some


object RelgramMeasuresScoobiApp extends ScoobiApp {



  val logger = LoggerFactory.getLogger(this.getClass)

  def loadRelgramCountsAndDistribute(relgramsPath:String, maxWindow:Int, minDirFreq:Int) = {
    loadRelgramCounts(relgramsPath, minDirFreq).map(rgc => {
      MapUtils.distributeCounts(rgc.counts, maxWindow)
      rgc
    })
  }

  def loadRelgramCounts(relgramsPath:String, minDirFreq:Int) = {
    def aboveDirThreshold(rgc:RelgramCounts) = rgc.counts.values.max > minDirFreq
    import RelgramCounts._
    TextInput.fromTextFile(relgramsPath)
             .flatMap(line => fromSerializedString(line))
             .filter(rgc => aboveDirThreshold(rgc))
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
  def toUndirRelgramCounts(rgcs: Iterable[RelgramCounts], minFreq:Int) = {
    val seq = rgcs.toSeq
    val ab = seq(0)

    def aboveThreshold(counts:Map[Int, Int]) = {
      val bc = counts.values
      !bc.isEmpty && bc.max >= minFreq
    }
    def flip(ab:RelgramCounts): RelgramCounts = {

      def flipRelgram(relgram:Relgram): Relgram = new Relgram(relgram.second, relgram.first)
      def flipArgCounts(argCounts:ArgCounts):ArgCounts = new ArgCounts(argCounts.secondArg1Counts, argCounts.secondArg2Counts,
                                                                       argCounts.firstArg1Counts, argCounts.firstArg2Counts)
      val orelgram = flipRelgram(ab.relgram)
      val oargCounts = flipArgCounts(ab.argCounts)
      new RelgramCounts(orelgram, new mutable.HashMap[Int, Int](), oargCounts)
    }
    if (seq.size >= 2){
      if (seq.size > 2) println("Duplicate relgrams: " + seq.mkString("\n"))
      val ba = seq(1)
      val bitermCounts = MapUtils.combine(ab.counts, ba.counts)
      if(aboveThreshold(bitermCounts.toMap)){
        new UndirRelgramCounts(ab, bitermCounts)::new UndirRelgramCounts(ba, bitermCounts)::Nil
      }else{
        Seq[UndirRelgramCounts]()
      }
    }else{
      //println("Seq size < 2: " + ab.toString)
      val ba = flip(ab)
      val bitermCounts = ab.counts.map(x => x).toMap
      if(aboveThreshold(bitermCounts)){
        new UndirRelgramCounts(ab, bitermCounts)::new UndirRelgramCounts(ba, bitermCounts)::Nil
      }else{
        Seq[UndirRelgramCounts]()
      }
    }
  }

  def toUndirRelgramCounts(relgramCounts: DList[RelgramCounts], minFreq:Int):DList[UndirRelgramCounts] = {

    import RelgramCounts._
    import UndirRelgramCounts._
    relgramCounts.groupBy(rgc => lexicallyOrderedKey(rgc.relgram))
                 .flatMap(grp => toUndirRelgramCounts(grp._2, minFreq))
  }

  def exportUndirCounts(undirCounts: DList[UndirRelgramCounts], outputPath: String){
    import UndirRelgramCounts._
    try{
      val undirPath = outputPath + File.separator + "undircounts"
      persist(TextOutput.toTextFile(undirCounts.map(x => x.serialize), undirPath))
    }catch{
      case e:Exception => {
        println("Failed to persist undir counts to path: " + outputPath)
        e.printStackTrace
      }

    }
  }

  def exportMeasures(measures: DList[Measures], outputPath: String){
    import Measures._
    try{
      val measuresPath = outputPath + File.separator + "measures"
      persist(TextOutput.toTextFile(measures.map(x => x.serialize), measuresPath))
    }catch{
      case e:Exception => {
        println("Failed to persist measures sto path: " + outputPath)
        e.printStackTrace
      }

    }
  }

  def loadRelationTupleCounts(tuplesPath:String) = TextInput.fromTextFile(tuplesPath)
                                                            .flatMap(line => RelationTupleCounts.fromSerializedString(line))


  def tupleKey(tuple:RelationTuple) = tuple.arg1 + "\t" + tuple.rel + "\t" + tuple.arg2

  def computeMeasures(relgramCounts:DList[UndirRelgramCounts], tupleCounts:DList[RelationTupleCounts]) = {
    import RelationTupleCounts._
    import RelgramCounts._
    import Measures._

    val groupedRGCs: DList[(String, UndirRelgramCounts)] =relgramCounts.map(urgc => (tupleKey(urgc.rgc.relgram.first), urgc))
    val groupedTCs: DList[(String, Int)] = tupleCounts.map(tc => (tupleKey(tc.tuple), tc.count))

    import com.nicta.scoobi.lib.Relational._
    val groups: DList[(String, (Iterable[UndirRelgramCounts], Iterable[Int]))] = coGroup(groupedRGCs, groupedTCs)

    val measures: DList[(String, Measures)] = groups.flatMap(group => {
      val firstCount = group._2._2.headOption.getOrElse(0)
      group._2._1.map(urgc => {
        (tupleKey(urgc.rgc.relgram.second), new Measures(urgc, firstCount, 0))
      })
    })

    val mgroups: DList[(String, (Iterable[Measures], Iterable[Int]))] = coGroup(measures,groupedTCs)
    mgroups.flatMap(group => {
      val secondCount = group._2._2.headOption.getOrElse(0)
      group._2._1.map(measure => {
        new Measures(measure.urgc, measure.firstCounts, secondCount)
      })
    })

  }

  override def run() {
    var inputPath, outputPath = ""
    var tuplesPath = ""
    var maxWindow = 50
    var minDirFreq, minFreq = 5
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("tuplesPath", "hdfs tuples path", {str => tuplesPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window length.", {str => maxWindow = str.toInt})
      opt("minFreq", "min freq for undirected cooccurrence", {str => minFreq = str.toInt})
      opt("minDirFreq", "min freq for undirected cooccurrence", {str => minDirFreq = str.toInt})
    }

    if (!parser.parse(args)) return

    import RelgramCounts._
    import UndirRelgramCounts._
    import Measures._
    import RelationTupleCounts._
    val relgramCounts = loadRelgramCountsAndDistribute(inputPath, maxWindow, minDirFreq)
    //NOTE filtering out relgrams that do not occur at least minFreq times with each other.
    def aboveThreshold(urgc:UndirRelgramCounts) = {
      val bc = urgc.bitermCounts.values
      !bc.isEmpty && bc.max >= minFreq
    }
    val undirCounts = toUndirRelgramCounts(relgramCounts, minFreq)
    val tupleCounts = loadRelationTupleCounts(tuplesPath)
    val measures = computeMeasures(undirCounts, tupleCounts)
    exportMeasures(measures, outputPath)
    //exportUndirCounts(undirCounts, outputPath)
  }
}

object RelgramMeasuresTest{

  def main(args:Array[String]){
    var inputPath, outputPath = ""
    var maxWindow = 50
    val parser = new OptionParser() {
      arg("inputPath", "hdfs input path", {str => inputPath = str})
      arg("outputPath", "hdfs output path", { str => outputPath = str })
      opt("maxWindow", "max window length.", {str => maxWindow = str.toInt})
    }

    if (!parser.parse(args)) return
    val minFreq = 0
    def exportLocal(undirCounts: immutable.Iterable[UndirRelgramCounts], outputPath: String){
      val writer = new PrintWriter(outputPath)
      undirCounts.foreach(uc => writer.println(uc.serialize))
      writer.close
    }

    import RelgramCounts._
    import UndirRelgramCounts._
    import Measures._
    val undirCounts = Source.fromFile(inputPath)
                              .getLines
                              .flatMap(line => RelgramCounts.fromSerializedString(line))//RelgramMeasuresScoobiApp.loadRelgramCounts(inputPath)
                              .toSeq
                              .map(rgc => {
                                MapUtils.distributeCounts(rgc.counts, maxWindow)
                                rgc
                              })
                              .groupBy(rgc => RelgramMeasuresScoobiApp.lexicallyOrderedKey(rgc.relgram))
                              .flatMap(grp => RelgramMeasuresScoobiApp.toUndirRelgramCounts(grp._2, minFreq))

    undirCounts.foreach(uc => UndirRelgramCounts.fromSerializedString(uc.serialize) match {
      case Some(c:UndirRelgramCounts) =>
      case None => "Failed to deserialize string: " + uc.serialize
    })

    exportLocal(undirCounts, outputPath)

  }
}
