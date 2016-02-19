package util

import java.text.SimpleDateFormat

import sun.misc.BASE64Decoder

/**
  * Created by yangshuai on 2016/2/18.
  * update by hechaoyi
  */
object CommonUtil {

  def decodeBase64(base64String : String): String = {
    val decoded = new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def getDevice(str: String): String = {
    if (str.contains("CFNetwork") && str.contains("Darwin")) {
      "cfnetwork"
    } else if(str.contains("(iPhone")||str.contains("iPod")||str.contains("iOS")||str.contains("(iPad;")||str.contains("iPad4")){
      "ios"
    } else if(str.contains("Android")||str.contains("MIDP")){
      println("Android")
      "android"
    }else if(str.contains("Windows NT")||str.contains("LBBROWSER")||str.contains("X11")||str.contains("(Macintosh;")){
      "pc"
    }else if(str.contains("python-requests")){
      "python-requests"
    } else {
      str
    }
  }

  def parseDevice(str:String):String={
    val rs = CommonUtil.decodeBase64(str)
    CommonUtil.getDevice(rs)
  }

  def parseDate(timeStamp: String):String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date: String = sdf.format(timeStamp.toLong)
    date
  }

  def getPrefix(arr: Array[String]):String = {
    parseDate(arr(0)) + ":" + parseDevice(arr(2)) + ":"
  }
}
