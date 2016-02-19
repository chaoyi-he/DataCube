package platform.ctrip

import java.util
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Decoder

/**
  * Created by yangshuai on 2016/2/18.
  * update by hechaoyi
  */
object Parser extends App {

  val conf =  new SparkConf().setMaster("local").setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile("E:/CODE/000000_0").map(_.split("\t")).filter(x => x(3).contains("ctrip.com/")&&(x(2)!="NoDef")).map(x => base64Paser(x(2))).
  map(x => (equip(x), 1)).reduceByKey(_+_).sortBy(_._2, ascending = false).foreach(println)

  def base64Paser(base64String : String): String = {
    var decoded= new BASE64Decoder().decodeBuffer(base64String)
    new String(decoded,"utf-8")
  }

  def equip(str: String): String = {
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
    }
    else {
      println(str)
      "other"
    }
  }
}
