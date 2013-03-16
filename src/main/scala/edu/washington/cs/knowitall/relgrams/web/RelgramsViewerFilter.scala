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
import scopt.mutable.OptionParser
import java.net.{URLEncoder, URL}
import collection.mutable
import xml.NodeBuffer
import util.matching.Regex.Match


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

  def getParamValue(paramName:String, req:HttpRequest[Any]):String = req.parameterNames.contains(paramName) match {
    case true => req.parameterValues(paramName)(0).trim
    case false => ""
  }
  def getParamValues(paramName:String, req:HttpRequest[Any]):Seq[String] = req.parameterNames.contains(paramName) match {
    case true => req.parameterValues(paramName).map(x => x.trim)
    case false => Seq[String]()
  }
  def getEqualityOption(req:HttpRequest[Any]) = {
    val values = getParamValues("equalityOption", req)
    if (values.size > 1)
      "both"
    else if(values.size == 1)
      values(0)
    else
      ""

  }
  def getSortBy(req:HttpRequest[Any]) = getParamValue("sortBy", req)
  def getArg1(req:HttpRequest[Any]) = getParamValue("arg1", req)
  def getRel(req:HttpRequest[Any]) = getParamValue("rel", req)
  def getArg2(req:HttpRequest[Any]) = getParamValue("arg2", req)
  def getMeasure(req:HttpRequest[Any]) = try { MeasureName.withName(getParamValue("measure", req)) } catch { case  e:Exception => MeasureName.psgf }
  def getAlpha(req:HttpRequest[Any]) = try {getParamValue("alpha", req).toDouble} catch { case e:Exception => -1.0}
  def getSmoothingDelta(req:HttpRequest[Any]) = try {getParamValue("delta", req).toDouble} catch { case e:Exception => -1.0}

  def getIndex(req:HttpRequest[Any]) = try { getParamValue("k", req).toInt } catch { case e:Exception => 1}

  def getOutputView(req:HttpRequest[Any]) = getParamValue("v", req)

  def getRelgramsQuery(req:HttpRequest[Any]) = {
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

  def equalityCheckBoxes(query:RelgramsQuery):String = {

    var equalitySelected = ""
    var noequalitySelected = ""
    var typedEqualitySelected = ""
    var typedNoEqualitySelected = ""
    query.equalityOption match {
      case "typeequality" => typedEqualitySelected = "checked"
      case "equality" => equalitySelected = "checked"
      case "typednoequality" => typedNoEqualitySelected = "checked"
      case "noequality" => noequalitySelected = "checked"
      case "both" => {equalitySelected = "checked"; noequalitySelected = "checked"}
      case _ => {equalitySelected = "checked"; noequalitySelected = "checked"}
    }
    //<input type="checkbox" name="equalityOption" value="typedequality" checked={typedEqualitySelected}>Typed Equality [At least one arg must be typed]</input>
    //<input type="checkbox" name="equalityOption" value="noequality" checked={typedNoEqualitySelected}>Typed but no Equality [At least one arg must be typed]</input>
    //<br/>
    val inputs = <span>
                    <input type="checkbox" name="equalityOption" value="equality" checked={equalitySelected}>Equality [Display rel-grams with equality constraints.]</input>
                    <input type="checkbox" name="equalityOption" value="noequality" checked={noequalitySelected}>No Equality [Display rel-grams without equality constraints.]</input>
                    <br/>
                    <!--input type="checkbox" name="equalityOption" value="typedequality" checked={typedEqualitySelected}>Typed Equality [At least one arg must be typed]</input-->
                    <!--input type="checkbox" name="equalityOption" value="noequality" checked={typedNoEqualitySelected}>Typed but no Equality [At least one arg must be typed]</input-->
                    <br/>
                </span>
    inputs.toString
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


  def scrubHTML(string:String) = {
    import org.apache.commons.lang.StringEscapeUtils.escapeHtml
    escapeHtml(string)
  }

  def createXMLForm(query:RelgramsQuery): String = {

    //def scrubHTML(text:String) =

   /** val arg1 = query.relationTuple.arg1
    val rel = query.relationTuple.rel
    val arg2 = query.relationTuple.arg2
    val title = "Relgrams Search Interface:"
    val formName = "relgrams"

    <h3>{title}</h3>
    <form action={formName}>
      <input name="arg1" value={arg1}>"Arg1"</input>
      <input name="rel" value={rel}>"Rel"</input>
      <input name="arg1" value={arg2}>"Arg2"</input>
      sortByElem
    </form>*/

    var loginForm:String = "<h3>Relgrams Search Interface:</h3><br/><br/>\n"
    loginForm += "<form action=\"relgrams\">\n"
    //loginForm += "<textarea name=original rows=10 cols=40>" + document + "</textarea><br/>"
    loginForm += "<input name=arg1 value=\"%s\"> Arg1</input><br/>\n".format(query.relationTuple.arg1)
    loginForm += "<input name=rel value=\"%s\"> Rel</input><br/>\n".format(query.relationTuple.rel)
    loginForm += "<input name=arg2 value=\"%s\"> Arg2</input><br/>\n".format(query.relationTuple.arg2)
    loginForm += "<br/>"
    loginForm += sortByOptions(query)

    loginForm += equalityCheckBoxes(query).toString//equalityOptions(query)
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

  def usage(exampleURL1:String, exampleURL2:String) = <div>
              <b>Usage:</b> Can be used to find rel-grams whose first tuple matches the fields specified below.
              <br/>
              <ul>
              <li>By default, tuples that contain ANY of the words in the corresponding fields are returned.</li>
              <li>Use <b>AND</b> between words to find tuples that contain all the specified words.</li>
              <li>Quotes around words causes the input string to be treated as a phrase.</li>
              <li>Example 1: <a href={exampleURL1}>(X:[person], die in, [time_unit])</a></li>
              <li>Example 2: <a href={exampleURL2}>([organization], file, *)</a></li>
              </ul>
              <br/>
              </div>
  def createForm(query:RelgramsQuery, host:String, port:Int): String = {

    //val exampleURL = """http://%s:%s/relgrams?arg1="xvar+type+person"&rel="die+in"&arg2="type+time+unit"&sortBy=fs&equalityOption=equality&search=search""".format(host, port)
    val arg1 = scrubHTML(query.relationTuple.arg1)
    val rel = scrubHTML(query.relationTuple.rel)
    val arg2 = scrubHTML(query.relationTuple.arg2)

    val exampleURL1 = """http://rv-n15.cs.washington.edu:25000/relgrams?arg1=study&rel=published+in&arg2=&sortBy=fs&equalityOption=equality&equalityOption=noequality&search=search"""
    val exampleURL2 = """"""
    var loginForm:String = "<h3>Relgrams Search Interface:</h3><br/><br/>\n"
    loginForm += usage(exampleURL1, exampleURL2)
    loginForm += "<form action=\"relgrams\">\n"

    //loginForm += "<textarea name=original rows=10 cols=40>" + document + "</textarea><br/>"
    loginForm += "<input name=arg1 value=\"%s\"> Arg1</input><br/>\n".format(arg1)
    loginForm += "<input name=rel value=\"%s\"> Rel</input><br/>\n".format(rel)
    loginForm += "<input name=arg2 value=\"%s\"> Arg2</input><br/>\n".format(arg2)

    loginForm += sortByOptions(query)
    loginForm += equalityCheckBoxes(query).toString//equalityOptions(query)
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

object RelgramsViewerFilter extends unfiltered.filter.Plan {
  val logger = LoggerFactory.getLogger(this.getClass)

  var solrManager = new SolrSearchWrapper("http://rv-n15.cs.washington.edu:10000/solr/relgrams")

  def wrapHtml(content:String) = "<html>" + content + "</html>"

  def search(query: RelgramsQuery):(String, Seq[(Measures, AffinityMeasures)]) = (query.toHTMLString, solrManager.search(query))

  val cssold = "<script>table.soft {\n\tborder-spacing: 0px;}\n.soft th, .soft td {\n\tpadding: 5px 30px 5px 10px;\n\tborder-spacing: 0px;\n\tfont-size: 90%;\n\tmargin: 0px;}\n.soft th, .soft td {\n\ttext-align: left;\n\tbackground-color: #e0e9f0;\n\tborder-top: 1px solid #f1f8fe;\n\tborder-bottom: 1px solid #cbd2d8;\n\tborder-right: 1px solid #cbd2d8;}\n.soft tr.head th {\n\tcolor: #fff;\n\tbackground-color: #90b4d6;\n\tborder-bottom: 2px solid #547ca0;\n\tborder-right: 1px solid #749abe;\n\tborder-top: 1px solid #90b4d6;\n\ttext-align: center;\n\ttext-shadow: -1px -1px 1px #666666;\n\tletter-spacing: 0.15em;}\n.soft td {\n\ttext-shadow: 1px 1px 1px #ffffff;}\n.soft tr.even td, .soft tr.even th {\n\tbackground-color: #3E698E;}\n.soft tr.head th:first-child {\n\t-webkit-border-top-left-radius: 5px;\n\t-moz-border-radius-topleft: 5px;\n\tborder-top-left-radius: 5px;}\n.soft tr.head th:last-child {\n\t-webkit-border-top-right-radius: 5px;\n\t-moz-border-radius-topright: 5px;\n\tborder-top-right-radius: 5px;}</script>"
  val css ="<script type=\"text/css\">/* financial or timetable */\n\nbody {\n\tfont-family: Arial, Verdana, sans-serif;\n\tcolor: #111111;}\n\ntable.financial {\n\twidth: 600px;}\n\n.financial th, .financial td {\n\tpadding: 7px 10px 10px 10px;}\n\n.financial th {\n\ttext-transform: uppercase;\n\tletter-spacing: 0.1em;\n\tfont-size: 90%;\n\tborder-bottom: 2px solid #111111;\n\tborder-top: 1px solid #999;\n\ttext-align: left;}\n\n.financial tr.even {\n\tbackground-color: #efefef;}\n\n.financial tr:hover {\n\tbackground-color: #c3e6e5;}\n\n.financial tfoot td {\n\tborder-top: 2px solid #111111;\n\tborder-bottom: 1px solid #999;}\n\n.money {\n\ttext-align: right;}</script>"
  val tableTags = "<table class=\"financial\">\n%s\n</table>\n"
  //def headerRow(measure:MeasureName.MeasureName) = "<tr><td><b>First (F)</b></td><td><b>Second (S)</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td><td><b>%s</b></td></tr>".format("Measure", "#(F,S)", "#(S,F)", "#(F,*)", "#(S,*)")

  def headerRow(measure:MeasureName.MeasureName) = {
    val headElem = <thead>
      <tr>
      <td>First Arg1</td><td><b>First Rel</b></td><td><b>First Arg2</b></td>
      <td/><td/>
      <td><b>Second Arg1</b></td><td><b>Second Rel</b></td><td><b>Second Arg2</b></td>
      <td/><td/>
      <td><b>P(S|F)</b></td><td><b>#(F,S)</b></td><td><b>#(S,F)</b></td><td><b>#F</b></td><td><b>#S</b></td>
      </tr>
    </thead>
    headElem.toString
  }
  def wrapResultsTableTags(content:String) = {
    tableTags.format(content)
  }

  val rowTags = "<tr>%s<tr>\n"
  def toResultsRow(measureName:String, query:RelgramsQuery, measures:Measures, affinities:AffinityMeasures, even:Boolean):String = resultsRowContent(measureName, query, measures, affinities, even).toString

  def fromTabToColonFmt(text:String) = {
    "(%s)".format(text.replaceAll("\t", "; "))
  }


  val typeRe= """(.*?)[tT]ype:(.*)$""".r
  def getTypeName(text:String) = typeRe.findFirstMatchIn(text) match {
    case Some(m:Match) => m.group(2).replaceAll(""":Number""", "")
    case _ => ""
  }
  def displayTypeText(text: String): String = {
    val m = typeRe.findFirstMatchIn(text)
    if (m != None) {
      val before = m.get.group(1)
      val typeText = m.get.group(2).replaceAll(""":Number""", "")
      "%s[%s]".format(before, typeText)
    }else{
      text
    }
  }
  def toDisplayText(tuple:RelationTuple):(String, String, String)= (displayTypeText(tuple.arg1).replaceAll("""XVAR""", "X"), tuple.rel, displayTypeText(tuple.arg2).replaceAll("""XVAR""", "X"))

  def relationWithRelArgs(first:RelationTuple, firstArg1Counts:mutable.Map[String, Int], firstArg2Counts:mutable.Map[String, Int]): NodeBuffer = {

    relationWithRelArgs(first, firstArg1Counts.toArray.sortBy(x => -x._2).mkString(","), firstArg2Counts.toArray.sortBy(x => -x._2).mkString(","))
  }
  val xvartypeargsstyle,typeargsstyle,xvarargstyle="font-weight: bold; color: #585858"
  val relstyle = "font-weight: bold; color: #0B614B"
  val argstyle = ""
  def relationWithRelArgs(tuple:RelationTuple, arg1TopArgs:String, arg2TopArgs:String): NodeBuffer = {

    val (arg1:String, rel:String, arg2:String) = toDisplayText(tuple)

    def isType(argText:String) = argText.contains("type:") ||  argText.contains("Type:")
    def argStyle(argText:String) = {
      if (isType(argText) || argText.contains("XVAR"))
        xvarargstyle
      else
        argstyle
    }
    val arg1Style = argStyle(tuple.arg1)
    val arg2Style = argStyle(tuple.arg2)
     <td><span style={arg1Style} TITLE={arg1TopArgs}>{arg1}</span></td>
     <td><span style={relstyle}>{rel}</span></td>
     <td><span style={arg2Style} TITLE={arg2TopArgs}>{arg2}</span></td>


  }
  def resultsRowContent(measureName:String, query:RelgramsQuery, measures:Measures, affinities:AffinityMeasures, even:Boolean) = {

    val measureVal = "%.4f".format(getMeasure(measureName, measures, affinities))
    val undirRGC = measures.urgc
    val rgc = undirRGC.rgc
    val fscount = undirRGC.rgc.counts.values.max
    val bitermCount = undirRGC.bitermCounts.values.max
    val sfcount = bitermCount - fscount
    var farg1Counts: mutable.Map[String, Int] = rgc.argCounts.firstArg1Counts
    var farg2Counts: mutable.Map[String, Int] = rgc.argCounts.firstArg2Counts

    var tfarg1Counts = farg1Counts.filter(x => farg2Counts.contains(x._1))
    if(tfarg1Counts.isEmpty) tfarg1Counts = farg1Counts

    var tfarg2Counts = farg2Counts.filter(x => farg1Counts.contains(x._1))
    if(tfarg2Counts.isEmpty) tfarg2Counts = farg2Counts


    var sarg1Counts: mutable.Map[String, Int] = rgc.argCounts.secondArg1Counts
    var sarg2Counts: mutable.Map[String, Int] = rgc.argCounts.secondArg2Counts

    var tsarg1Counts = sarg1Counts.filter(x => sarg2Counts.contains(x._1))
    if(tsarg1Counts.isEmpty) tsarg1Counts = sarg1Counts

    var tsarg2Counts = sarg2Counts.filter(x => sarg1Counts.contains(x._1))
    if(tsarg2Counts.isEmpty) tsarg2Counts = sarg2Counts

    val evenString = if(even) "even" else ""
    <tr class={evenString}>
      {relationWithRelArgs(rgc.relgram.first, tfarg1Counts, tfarg2Counts)}<td/><td/>
      {relationWithRelArgs(rgc.relgram.second, tsarg1Counts, tsarg2Counts)}<td/><td/>
      <td>{measureVal}</td>
      <td>{fscount}</td>
      <td>{sfcount}</td>
      <td>{measures.firstCounts}</td>
      <td>{measures.secondCounts}</td>
    </tr>


  }

  def hasTypedEquality(relgram:Relgram):Boolean = hasTypedEquality(relgram.first) || hasTypedEquality(relgram.second)
  def hasTypedEquality(tuple:RelationTuple):Boolean = hasTypedEquality(tuple.arg1) || hasTypedEquality(tuple.arg2)
  def hasTypedEquality(arg:String):Boolean = hasEquality(arg) && hasType(arg)

  def hasType(relgram:Relgram):Boolean = hasType(relgram.first) && hasType(relgram.second)
  def hasType(tuple:RelationTuple):Boolean = hasType(tuple.arg1) && hasType(tuple.arg2)
  def hasType(string:String):Boolean = typeRe.findFirstMatchIn(string) != None


  def hasEquality(relgram:Relgram):Boolean = hasEquality(relgram.first) || hasEquality(relgram.second)
  def hasEquality(tuple:RelationTuple):Boolean = hasEquality(tuple.arg1) || hasEquality(tuple.arg2)
  def hasEquality(arg:String): Boolean =arg.contains("XVAR")

  def agreesWithEqualityOption(equalityOption: String, tuple: (Measures, AffinityMeasures)): Boolean = equalityOption match {
    case "typedequality" => hasEquality(tuple._1.urgc.rgc.relgram) && hasType(tuple._1.urgc.rgc.relgram)
    case "equality" => hasEquality(tuple._1.urgc.rgc.relgram)
    case "noequality" => !hasEquality(tuple._1.urgc.rgc.relgram)
    case "typednoequality" => !hasEquality(tuple._1.urgc.rgc.relgram) && hasType(tuple._1.urgc.rgc.relgram)
    case _ => true
  }

  def hasTypeQuantity(tuple:RelationTuple):Boolean = tuple.arg1.contains("Type:quantity") || tuple.arg2.contains("Type:quantity")
  def hasTypeQuantity(relgram:Relgram):Boolean = hasTypeQuantity(relgram.first) || hasTypeQuantity(relgram.second)

  var minFreq = 0
  def aboveThreshold(urgc:UndirRelgramCounts) = urgc.rgc.counts.values.max > minFreq

  def isIdentityRelgram(relgram:Relgram) = {
    relgram.first.isIdenticalTo(relgram.second)
  }

  def findEqualityVar(tuple:RelationTuple) = if(tuple.arg1.contains("XVAR")) tuple.arg1 else if(tuple.arg2.contains("XVAR")) tuple.arg2 else ""
  def equalityTypesAgree(relgram: Relgram) = {
    val fvar = findEqualityVar(relgram.first)
    val svar = findEqualityVar(relgram.second)
    val ftypeName = getTypeName(fvar)
    val stypeName = getTypeName(svar)
    ftypeName.equals(stypeName)
  }

  def renderSearchResults(measureName:String, query:RelgramsQuery, results:(String, Seq[(Measures, AffinityMeasures)])) = {
    var even = false
    css + wrapResultsTableTags(headerRow(query.measure) + "\n<br/>\n" +
                         results._2.filter(ma => !isIdentityRelgram(ma._1.urgc.rgc.relgram))
                                   .filter(ma => aboveThreshold(ma._1.urgc))
                                   .filter(ma => agreesWithEqualityOption(query.equalityOption, ma))
                                   .filter(ma => !hasTypeQuantity(ma._1.urgc.rgc.relgram))
                                   .filter(ma => equalityTypesAgree(ma._1.urgc.rgc.relgram))
                                   .map(ma => {
                           even = !even
                           toResultsRow(measureName, query, ma._1, ma._2, even)
                         }).mkString("\n"))
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
      logger.info("Query: " + relgramsQuery)
      val sortBy = ReqHelper.getSortBy(req)

      val results = if (isNonEmptyQuery(relgramsQuery)) search(relgramsQuery) else ("", Seq[(Measures, AffinityMeasures)]())
      val sortedResults = results._2.sortBy(ma => sortByMeasure(ma, sortBy))
      ResponseString(wrapHtml(HtmlHelper.createForm(relgramsQuery, "localhost", 10000) + "<br/><br/>"
        + renderSearchResults("conditional", relgramsQuery, (results._1, sortedResults))))
        //"" + "Arg1: " + relgramsQuery.toHTMLString) )
    }
  }

  var host = "localhost"
  var port = 10000
  val intentVal = unfiltered.netty.cycle.Planify {
    case req @ GET(Path("/relgrams")) => {
      val relgramsQuery = ReqHelper.getRelgramsQuery(req)
      logger.info("Query: " + relgramsQuery)
      val sortBy = ReqHelper.getSortBy(req)
      val results = if (isNonEmptyQuery(relgramsQuery)) search(relgramsQuery) else ("", Seq[(Measures, AffinityMeasures)]())
      val sortedResults = results._2.sortBy(ma => sortByMeasure(ma, sortBy))
      ResponseString(wrapHtml(HtmlHelper.createForm(relgramsQuery,host, port) + "<br/><br/>"
        + renderSearchResults("conditional", relgramsQuery, (results._1, sortedResults))))
      //"" + "Arg1: " + relgramsQuery.toHTMLString) )
    }
  }

  def main(args:Array[String]){

    //var port = 10000
    val parser = new OptionParser() {
      arg("solrURL", "hdfs input path", {str => solrManager = new SolrSearchWrapper(str)})
      opt("port", "port to run on.", {str => port = str.toInt})
      opt("host", "port to run on.", {str => host = str})
      opt("minFreq", "minimum frequency threshold", {str => minFreq = str.toInt})
    }
    if (!parser.parse(args)) return
    unfiltered.netty.Http(port).plan(intentVal).run()
  }

}
