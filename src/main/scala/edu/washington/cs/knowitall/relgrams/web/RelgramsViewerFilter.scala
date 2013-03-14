package edu.washington.cs.knowitall.relgrams.web

/**
 * Created with IntelliJ IDEA.
 * User: niranjan
 * Date: 3/12/13
 * Time: 11:54 AM
 * To change this template use File | Settings | File Templates.
 */
import org.slf4j.LoggerFactory
import unfiltered.filter.Plan
import unfiltered.request.{HttpRequest, GET, Path, Method}

import unfiltered.response.ResponseString
import edu.washington.cs.knowitall.relgrams._
import javax.servlet.http.HttpServletRequest

import unfiltered.response.ResponseString
import edu.washington.cs.knowitall.relgrams.solr.SolrSearchWrapper
import unfiltered.response.ResponseString


object MeasureName extends Enumeration("bigram", "biterm", "psgf", "pfgs", "psgf_undir", "pmi", "npmi", "pmi_undir", "npmi_undir",
  "psgf_combined", "pfgs_combined",  "psgf_undir_combined", "pfgs_undir_combined",
  "pmi_combined", "npmi_combined", "pmi_undir_combined", "npmi_undir_combined",
  "pfgs_undir", "avg_psgf_pfgs",
  "scp", "scp_undir", "scp_combined", "scp_undir_combined"){


  type MeasureName = Value
  val bigram,
  biterm,
  psgf, pfgs,
  psgf_undir,
  pmi, npmi,
  pmi_undir, npmi_undir,
  psgf_combined, pfgs_combined, psgf_undir_combined, pfgs_undir_combined,
  pmi_combined, npmi_combined,
  pmi_undir_combined, npmi_undir_combined,
  pfgs_undir,
  avg_psgf_pfgs, scp, scp_undir, scp_combined, scp_undir_combined = Value

}


import MeasureName._
class RelgramsQuery(val relationTuple:RelationTuple,
                    val measure:MeasureName,
                    val measureIndex:Int,
                    val alpha:Double,
                    val smoothingDelta:Double,
                    val outputView:String,
                    val sortBy:String,
                    val equalityOption:String){

  override def toString:String = relationTuple.toString() + "\t" + measure + "\t" + measureIndex + "\t" + outputView + "\t" + sortBy + "\t" + equalityOption
  def toHTMLString(relationTuple:RelationTuple):String = "(%s;%s;%s)".format(relationTuple.arg1, relationTuple.rel, relationTuple.arg2)
  def toHTMLString:String = toHTMLString(relationTuple) + "\t" + measure + "\t" + measureIndex + "\t" + outputView + "\t" + sortBy + "\t" + equalityOption

}


object ReqHelper{

  def getParamValue(paramName:String, req:HttpRequest[HttpServletRequest]):String = req.parameterNames.contains(paramName) match {
    case true => req.parameterValues(paramName)(0).trim
    case false => ""
  }
  def getEqualityOption(req:HttpRequest[HttpServletRequest]) = getParamValue("equalityOption", req)
  def getSortBy(req:HttpRequest[HttpServletRequest]) = getParamValue("sortBy", req)
  def getArg1(req:HttpRequest[HttpServletRequest]) = getParamValue("arg1", req)
  def getRel(req:HttpRequest[HttpServletRequest]) = getParamValue("rel", req)
  def getArg2(req:HttpRequest[HttpServletRequest]) = getParamValue("arg2", req)
  def getMeasure(req:HttpRequest[HttpServletRequest]) = try { MeasureName.withName(getParamValue("measure", req)) } catch { case  e:Exception => MeasureName.psgf }
  def getAlpha(req:HttpRequest[HttpServletRequest]) = try {getParamValue("alpha", req).toDouble} catch { case e:Exception => -1.0}
  def getSmoothingDelta(req:HttpRequest[HttpServletRequest]) = try {getParamValue("delta", req).toDouble} catch { case e:Exception => -1.0}

  def getIndex(req:HttpRequest[HttpServletRequest]) = try { getParamValue("k", req).toInt } catch { case e:Exception => 1}

  def getOutputView(req:HttpRequest[HttpServletRequest]) = getParamValue("v", req)

  def getRelgramsQuery(req:HttpRequest[HttpServletRequest]) = {
    new RelgramsQuery(RelationTuple.fromArg1RelArg2(getArg1(req),getRel(req),getArg2(req)),
                          getMeasure(req),
                          getIndex(req),
                          getAlpha(req),
                          getSmoothingDelta(req),
                          getOutputView(req),
                          getSortBy(req),
                          getEqualityOption(req))
  }
}

object HtmlHelper{

  def viewOptions(query:RelgramsQuery) = {
    var anySelected, arg1relselected, relarg2selected, arg1relarg2selected = ""
    query.outputView match {
      case "Any" => anySelected = "selected"
      case "ARG1REL" => arg1relselected = "selected"
      case "RELARG2" => relarg2selected = "selected"
      case "ARG1RELARG2" => arg1relarg2selected = "selected"
      case _ => anySelected = "selected"

    }
    "<select name=\"v\">\n" +
      "<option value=\"Any\" " +  anySelected + " >Any</option>\n" +
      "<option value=\"ARG1REL\" " +  arg1relselected + " >ARG1REL</option>\n"  +
      "<option value=\"RELARG2\" " + relarg2selected + ">RELARG2</option>\n" +
      "<option value=\"ARG1RELARG2\" " + arg1relarg2selected + ">ARG1RELARG2</option>\n" +
      "</select><br/><br/><br/>\n"

  }

  def optionString(measureName:String, selectedString:String, displayName:String) = {
    "<option value=\"%s\" %s>%s</option>\n".format(measureName, selectedString, displayName)//
  }
  def measureOptions(query: RelgramsQuery): String = {

    var bigramSelected, bitermSelected, psgfSelected, psgfUndirSelected, pmiSelected, npmiSelected = ""
    var psgfCombinedSelected,psgfUndirCombinedSelected,pmiCombinedSelected, npmiCombinedSelected = ""
    query.measure match {
      case MeasureName.bigram => bigramSelected = "selected"
      case MeasureName.biterm => bitermSelected = "selected"
      case MeasureName.psgf => psgfSelected = "selected"
      case MeasureName.psgf_undir => psgfUndirSelected = "selected"
      case MeasureName.pmi => pmiSelected = "selected"
      case MeasureName.npmi => npmiSelected = "selected"
      case MeasureName.psgf_combined => psgfCombinedSelected = "selected"
      case MeasureName.psgf_undir_combined => psgfUndirCombinedSelected = "selected"
      case MeasureName.pmi_combined => pmiCombinedSelected = "selected"
      case MeasureName.npmi_combined => npmiCombinedSelected = "selected"

      case _ => bigramSelected = "selected"

    }


    "<select name=\"measure\">\n" +
      optionString(MeasureName.bigram.toString, bigramSelected, "#(F,S)") +
      optionString(MeasureName.biterm.toString, bitermSelected, "#(F,S) + #(S,F)") +
      optionString(MeasureName.psgf.toString, psgfSelected, "Dir: P(S|F)") +
      optionString(MeasureName.psgf_undir.toString, psgfUndirSelected, "UnDir: P(S|F)") +
      optionString(MeasureName.pmi.toString, pmiSelected, "PMI") +
      optionString(MeasureName.npmi.toString, npmiSelected, "NPMI") +
      optionString(MeasureName.psgf_combined.toString, psgfCombinedSelected, "Combined Dir: P(S|F)") +
      optionString(MeasureName.psgf_undir_combined.toString, psgfUndirCombinedSelected, "Combined UnDir: P(S|F)") +
      optionString(MeasureName.pmi_combined.toString, pmiCombinedSelected, "Combined PMI") +
      optionString(MeasureName.npmi_combined.toString, npmiCombinedSelected, "Combined NPMI") +
      "</select> Measure<br/><br/><br/>\n"
  }

  def equalityOptions(query:RelgramsQuery):String = {
    var equalitySelected = ""
    var noequalitySelected = ""
    var bothSelected = ""

    query.equalityOption match {
      case "equality" => equalitySelected = "selected"
      case "noequality" => noequalitySelected = "selected"
      case "both" => bothSelected = "selected"
      case _ => bothSelected = "selected"
    }

    "<select name=\"equalityOption\">"+
    optionString("equality", equalitySelected, "Equality Relgrams") +
    optionString("noequality", noequalitySelected, "No Equality Relgrams") +
    optionString("both", bothSelected, "All Relgrams") +
    "</select> Equality<br/><br/><br/>"
  }
  def sortByOptions(query:RelgramsQuery):String = {

    var conditionalSelected = ""
    var fsSelected = ""
    var sfSelected = ""
    var fSelected = ""
    var sSelected = ""

    query.sortBy match {
      case "conditional" => conditionalSelected = "selected"
      case "fs" => fsSelected = "selected"
      case "sf" => sfSelected = "selected"
      case "f" => fSelected = "selected"
      case "s" => sSelected = "selected"
      case _ => fsSelected = "selected"
    }
    "<select name =\"sortBy\">" +
    optionString("conditional", conditionalSelected, "P(S|F)") +
    optionString("fs", fsSelected, "#(F,S)") +
    optionString("sf", sfSelected, "#(S,F)") +
    optionString("f", fSelected, "#F") +
    optionString("s", sSelected, "#S") +
    "</select> Sort By<br/><br/><br/>\n"

  }
  def mesureIndexOptions(query: RelgramsQuery): String = {

    "<select name=\"k\">" +
      (0 until 10).map(i => {
        val selected = if (i == query.measureIndex) "selected" else ""
        """<option value="%d" %s>%d</option>\n""".format(i, selected, (i+1))
      }).mkString("\n") +
      "</select>Window<br/><br/><br/>\n"

  }

  def alphaBox(query:RelgramsQuery, alpha:Double) = {
    val value = if(query.alpha > 0) query.alpha else alpha
    """<input name=alpha value="%.2f"> Alpha (Used to combine measures from different windows) </input><br/>""".format(value)
  }

  def deltaBox(query:RelgramsQuery, delta:Double) = {
    val value = if(query.alpha > 0) query.smoothingDelta else delta
    """<input name=delta value="%.2f"> Delta (Smoothing factor hack: 10,100,1000 etc. Higher values discount more.)</input><br/>""".format(value)
  }

  def createForm(query:RelgramsQuery): String = {

    var loginForm:String = "<h3>Relgrams Search Interface:</h3><br/><br/>\n"
    loginForm += "<form action=\"relgrams\">\n"
    //loginForm += "<textarea name=original rows=10 cols=40>" + document + "</textarea><br/>"
    loginForm += "<input name=arg1 value=\"%s\"> Arg1</input><br/>\n".format(query.relationTuple.arg1)
    loginForm += "<input name=rel value=\"%s\"> Rel</input><br/>\n".format(query.relationTuple.rel)
    loginForm += "<input name=arg2 value=\"%s\"> Arg2</input><br/>\n".format(query.relationTuple.arg2)

    loginForm += sortByOptions(query)
    loginForm += equalityOptions(query)
    /**loginForm += viewOptions(query)
    loginForm += measureOptions(query)
    loginForm += mesureIndexOptions(query)

    loginForm += alphaBox(query, 0.5)
    loginForm += deltaBox(query, 10)
    */
    loginForm += "<input name=search type=\"submit\" value=\"search\"/>\n"//<span style="padding-left:300px"></span>
    loginForm += "</form>\n"
    //println(loginForm)
    return loginForm
  }



}

class RelgramsViewerFilter extends unfiltered.filter.Plan {
  val logger = LoggerFactory.getLogger(this.getClass)

  val solrManager = new SolrSearchWrapper("http://rv-n15.cs.washington.edu:10000/solr/relgrams")

  def wrapHtml(content:String) = "<html>" + content + "</html>"

  def search(query: RelgramsQuery):(String, Seq[(Measures, AffinityMeasures)]) = (query.toHTMLString, solrManager.search(query))

  val tableTags = "<table border=1>\n%s\n</table>\n"
  //def headerRow(measure:MeasureName.MeasureName) = "<tr><td><b>First (F)</b></td><td><b>Second (S)</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td></tr>".format("Measure", "#(F,S)", "#(S,F)", "#(F,*)", "#(S,*)")

  def headerRow(measure:MeasureName.MeasureName) = "<tr><td><b>First (F)</b></td><td><b>Second (S)</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td></tr>".format("P(S|F)", "#(F,S)", "#(S,F)", "#F", "#S")
  def wrapResultsTableTags(content:String) = {
    tableTags.format(content)
  }

  val rowTags = "<tr>%s<tr>\n"
  def toResultsRow(measureName:String, query:RelgramsQuery, measures:Measures, affinities:AffinityMeasures):String = rowTags.format(resultsRowContent(measureName, query, measures, affinities))

  def fromTabToColonFmt(text:String) = {
    "(%s)".format(text.replaceAll("\t", "; "))
  }


  val typeRe = """^(.*?):(.*?)(__.*)*$""".r
  def displayTypeText(text: String): Any = {
    val m = typeRe.findFirstMatchIn(text)
    if (m != None) {
      val source = m.get.group(1)
      val typeText = m.get.group(2)
      source + ":" + typeText
    }else{
      text
    }
  }
  def toDisplayText(relString: String): String = {
    val splits = relString.split("\t")
    displayTypeText(splits(0)) + "\t" + splits(1) + "\t" + displayTypeText(splits(2))
  }
  def relationWithRelArgs(first:RelationTuple):String = {
    relationWithRelArgs(first.prettyString, first.arg1HeadCounts.toArray.sortBy(x => -x._2).mkString(","), first.arg2HeadCounts.toArray.sortBy(x => -x._2).mkString(","))
  }
  def relationWithRelArgs(text:String, arg1TopArgs:String, arg2TopArgs:String):String = {
    val splits = toDisplayText(text).split("\t")
    """(<a href="#" TITLE="%s">%s</a>;%s;<a href="#" TITLE="%s">%s</a>)""".format(arg1TopArgs, splits(0), splits(1), arg2TopArgs, splits(2))
  }
  def resultsRowContent(measureName:String, query:RelgramsQuery, measures:Measures, affinities:AffinityMeasures):String = {

    val measureVal = getMeasure(measureName, measures, affinities)
    val undirRGC = measures.urgc
    val rgc = undirRGC.rgc
    val fscount = undirRGC.rgc.counts.values.max
    val bitermCount = undirRGC.bitermCounts.values.max
    val sfcount = bitermCount - fscount
    "<td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td>".format(relationWithRelArgs(rgc.relgram.first),
      relationWithRelArgs(rgc.relgram.second),
      measureVal,
      fscount,
      sfcount,
      measures.firstCounts,
      measures.secondCounts)

    //mrvg.relviewGrams.firstSecondCounts.get(9))
  }

  def agreesWithEqualityOption(equalityOption: String, tuple: (Measures, AffinityMeasures)): Boolean = equalityOption match {
    case "equality" => tuple._1.urgc.rgc.prettyString.contains("XVAR")
    case "noequality" => !tuple._1.urgc.rgc.prettyString.contains("XVAR")
    case "both" => true
    case _  => false
  }

  def hasTypeQuantity(tuple:RelationTuple):Boolean = tuple.arg1.contains("Type:quantity") || tuple.arg2.contains("Type:quantity")
  def hasTypeQuantity(relgram:Relgram):Boolean = hasTypeQuantity(relgram.first) || hasTypeQuantity(relgram.second)
  def renderSearchResults(measureName:String, query:RelgramsQuery, results:(String, Seq[(Measures, AffinityMeasures)])) = {
    wrapResultsTableTags(headerRow(query.measure) + "\n<br/>\n" +
                         results._2.filter(ma => agreesWithEqualityOption(query.equalityOption, ma))
                                   .filter(ma => hasTypeQuantity(ma._1.urgc.rgc.relgram))
                                   .map(ma => toResultsRow(measureName, query, ma._1, ma._2)).mkString("\n"))
  }



  def isNonEmptyQuery(query: RelgramsQuery): Boolean = {
    println("Relgrams Query: " + query.toHTMLString)
    val bool = query.relationTuple.arg1.size > 0 || query.relationTuple.rel.size > 0 || query.relationTuple.arg2.size > 0
    println("bool: " + bool)
    return bool
  }

  def sortByMeasure(tuple: (Measures, AffinityMeasures), sortBy:String) = {
    val measure = getMeasure(sortBy, tuple)
    0-measure
  }


  def getMeasure(measureName: String, tuple: (Measures, AffinityMeasures)): Double = {
    val (measures, affinities) = tuple
    getMeasure(measureName, measures, affinities)
  }
  def getMeasure(measureName:String, measures:Measures, affinities:AffinityMeasures):Double = measureName match {
    case "conditional" => affinities.firstUndir.conditional
    case "fs" => measures.urgc.rgc.counts.values.max.toDouble
    case "sf" => (measures.urgc.bitermCounts.values.max - measures.urgc.rgc.counts.values.max).toDouble
    case "f" => measures.firstCounts.toDouble
    case "s" => measures.secondCounts.toDouble
    case _ => affinities.firstUndir.conditional
  }

  def intent: Plan.Intent = {

    case req @ GET(Path("/relgrams")) => {
      val relgramsQuery = ReqHelper.getRelgramsQuery(req)
      val sortBy = ReqHelper.getSortBy(req)
      val results = if (isNonEmptyQuery(relgramsQuery)) search(relgramsQuery) else ("", Seq[(Measures, AffinityMeasures)]())
      val sortedResults = results._2.sortBy(ma => sortByMeasure(ma, sortBy))
      ResponseString(wrapHtml(HtmlHelper.createForm(relgramsQuery) + "<br/><br/>"
        + renderSearchResults("conditional", relgramsQuery, (results._1, sortedResults))))
        //"" + "Arg1: " + relgramsQuery.toHTMLString) )
    }
  }
}
