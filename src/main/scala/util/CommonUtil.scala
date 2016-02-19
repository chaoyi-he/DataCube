package util

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
      "android"
    }else if(str.contains("Windows NT")||str.contains("LBBROWSER")||str.contains("X11")||str.contains("(Macintosh;")){
      "pc"
    }else if(str.contains("python-requests")){
      "python-requests"
    } else {
      str
    }
  }
}
