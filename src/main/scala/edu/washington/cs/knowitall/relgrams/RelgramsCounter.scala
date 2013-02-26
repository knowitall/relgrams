package edu.washington.cs.knowitall.relgrams

import utils.MapUtils

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 2/25/13
 * Time: 3:31 PM
 * To change this template use File | Settings | File Templates.
 */
class RelgramsCounter {


  def haveDisjointHashes(mergeWith: RelgramCounts, toMerge: RelgramCounts): Boolean = {
    val a1 = mergeWith.relgram.first.hashes
    val a2 = mergeWith.relgram.second.hashes
    val b1 = toMerge.relgram.first.hashes
    val b2 = toMerge.relgram.second.hashes
    !(a1 exists b1) && !(a2 exists b2)
  }

  def updateHashes(mergeWith: RelgramCounts, toMerge: RelgramCounts){
    mergeWith.relgram.first.hashes ++= toMerge.relgram.first.hashes
    mergeWith.relgram.second.hashes ++= toMerge.relgram.second.hashes
  }

  def mergeArgCounts(mergeWith: RelgramCounts, toMerge: RelgramCounts) {
    MapUtils.addTo(mergeWith.argCounts.firstArg1Counts, toMerge.argCounts.firstArg1Counts)
    MapUtils.addTo(mergeWith.argCounts.firstArg2Counts, toMerge.argCounts.firstArg2Counts)
    MapUtils.addTo(mergeWith.argCounts.secondArg1Counts, toMerge.argCounts.secondArg1Counts)
    MapUtils.addTo(mergeWith.argCounts.secondArg1Counts, toMerge.argCounts.secondArg2Counts)
  }

  def mergeCounts(mergeWith: RelgramCounts, toMerge: RelgramCounts){
    MapUtils.addTo(mergeWith.counts, toMerge.counts)
  }

  def merge(mergeWith: RelgramCounts, toMerge: RelgramCounts){

    if(haveDisjointHashes(mergeWith, toMerge)){
      updateHashes(mergeWith, toMerge)
      mergeArgCounts(mergeWith, toMerge)
      mergeCounts(mergeWith, toMerge)
    }else{
      println("Not merging1: " + mergeWith)
      println("Not merging2: " + toMerge)
    }


  }

  def reduceRelgramCounts(rgcs:Iterable[RelgramCounts]) = {
    var outRGC:RelgramCounts = null
    rgcs.filter(rgc => {
      val filterVal = !RelgramCounts.isDummy(rgc)
      if(filterVal == false){
        println("Ignoring dummy rgc: " + rgc)
      }
      filterVal
    }).foreach(rgc => {
      if (outRGC == null){
        outRGC = rgc
      }else{
        merge(outRGC, rgc)
      }
    })
    println("outrgc: " + outRGC.prettyString)
    if(outRGC != null) Some(outRGC) else None
  }

}
