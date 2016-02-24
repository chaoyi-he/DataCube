package util

import java.text.SimpleDateFormat
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

  def initCodeMap(): Unit ={
    val source=Source.fromFile("E:/DataCube/src/main/resource/UsageAirPort")
    val itera= source.getLines()
    for(line<-itera){
       val arr = line.split('*')
      if(arr.length==2) {
        codeMap.put(line.split('*')(0), line.split('*')(1))
      }
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

    val airLine: Array[String] = Array("(", "（")
    if (depCn.contains(airLine(0))) {
      depCn = depCn.substring(0, depCn.indexOf(airLine(0)))
    } else if (depCn.contains(airLine(1))) {
      depCn = depCn.substring(0, depCn.indexOf(airLine(1)))
    }

    if (arriveCn.contains(airLine(0))) {
      arriveCn = arriveCn.substring(0, arriveCn.indexOf(airLine(0)))
    } else if (arriveCn.contains(airLine(1))) {
      arriveCn = arriveCn.substring(0, arriveCn.indexOf(airLine(1)))
    }

    /*if(codeMap.getOrElse(depCn,1)==1)
    {
       depCn
    }else if(codeMap.getOrElse(arriveCn,1)==1){
      arriveCn
    }else{
      ""
    }*/

    if(codeMap.getOrElse(depCn,1)==1||codeMap.getOrElse(arriveCn,1)==1)
      {
        "none"
      }
    else {
      codeMap.getOrElse(depCn,1)+"-"+codeMap.getOrElse(arriveCn,1)
    }

  }

}
