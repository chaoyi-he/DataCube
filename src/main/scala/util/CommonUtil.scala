package util

import java.text.SimpleDateFormat
import org.jsoup.Jsoup
import sun.misc.BASE64Decoder

import scala.collection.mutable
import scala.io.Source

/**
  * Created by yangshuai on 2016/2/18.
  * update by hechaoyi
  */
object CommonUtil {

  val codeMap = mutable.HashMap[String, String]()

  def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def getDevice(str: String): String = {
    if (str.contains("CFNetwork") && str.contains("Darwin")) {
      "CFNetwork"
    } else if (str.contains("(iPhone")||str.contains("iPod")||str.contains("iOS")||str.contains("(iPad;")||str.contains("iPad4")){
      "IOS"
    } else if (str.contains("Android")||str.contains("MIDP")){
      "Android"
    } else if (str.contains("Windows Phone")) {
      "WindowsPhone"
    } else if (str.contains("Windows NT")||str.contains("LBBROWSER")||str.contains("X11")||str.contains("(Macintosh;") || str.contains("Windows XP") || str.contains("Windows 98") || str.contains("Windows ME")){
      "PC"
    } else if (str.contains("python-requests")){
      "python-requests"
    } else if (str.contains("Symbian")) {
      "Symbian"
    } else if (str.contains("BlackBerry") || str.contains("BB10")) {
      "BlackBerry"
    } else {
      str
    }
  }

  def parseDevice(str:String):String={
    val rs = CommonUtil.decodeBase64(str)
    CommonUtil.getDevice(rs)
  }

  def parseDate(timeStamp: String):String = {
    var date = ""
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    try {
      date = sdf.format(timeStamp.toLong)
    } catch {
      case e: Exception =>
        return "not a timestamp"
    }
    date
  }

  def getPrefix(arr: Array[String]):String = {
    parseDate(arr(0)) + "-->>" + parseDevice(arr(2)) + "-->>"
  }

  def initCodeMap(): Unit = {
    val sourceUrl = "http://www.hlhkys.com/dmair/cx.asp?Field=&keyword=&MaxPerPage=50&page=1"
    for (i <- 1 to 68) {
      val doc = Jsoup.connect(sourceUrl + i).get()
      val trs = doc.body().getElementsByTag("table").get(2).child(0).child(0).child(0).child(0).getElementsByTag("tr")
      println(i + "-->>" + trs.size())
      for (j <- 1 until trs.size()) {
        val ch = trs.get(j).child(0).child(0).text().substring(1)
        val en = trs.get(j).child(1).text()
        codeMap.put(ch, en)
      }
    }

  }

  def readCodeMap(): Unit ={
    val source=Source.fromFile("/Users/hechaoyi/Documents/KunYan/DataCube/src/main/resource/AirPortCode")
    val itera= source.getLines()
    for(line<-itera){
      println(line.toString())
      codeMap.put(line.split('*')(0),line.split('*')(1))
    }
  }
  def convertAirLine(line: String): String = {

    val arr = line.split("-")

    if (arr.length < 2) {
      println("less than 2" + line)
      return "none"
    }

    var depCn = arr(0)
    var arriveCn = arr(1)

    if (depCn.contains("(")) {
      depCn = depCn.substring(0, depCn.indexOf('('))
    }

    if (arriveCn.contains("(")) {
      arriveCn = arriveCn.substring(0, arriveCn.indexOf('('))
    }

    val dep = codeMap.getOrElse(depCn, depCn)
    val arrive = codeMap.getOrElse(arriveCn, arriveCn)

    println(dep + "-" + arrive)

    dep + "-" + arrive
  }

  def main(args: Array[String]): Unit = {
    //initCodeMap()
    readCodeMap()
  }
}
