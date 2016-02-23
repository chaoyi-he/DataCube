package platform.ali


import java.net.URLDecoder

import _root_.util.CommonUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by niujiaojiao on 2016/2/19.
  */
object AliParser extends App {

  val source = args(0)
  val target = args(1)
  val conf =  new SparkConf().setAppName("ALI")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("alitrip.com/") && (x(2)!="NoDef")
     && x(3).contains("depCityName=") && x(3).contains("arrCityName="))
    .filter(x=> containsAirLine(x(3)))
    .map(x=>(CommonUtil.getPrefix(x) +extractAirLine(x(3)),1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false).saveAsTextFile(target)


  def extractAirLine(url: String): String = {
    var substring = ""
    var left = ""
    var right = ""
    if (url.contains("&depCityName=") && url.contains("&arrCityName=")) {
      substring = url.split("&depCityName=")(1)
      left = substring.split("&arrCityName=")(0)
      right = substring.split("&arrCityName=")(1).split('&')(0)
    }
    decodeString(left) + "-" + decodeString(right)
  }

  def containsAirLine(url: String): Boolean = {
    if (url.contains("&depCityName=") && url.contains("&arrCityName=")) {
      val tempFrom = url.split("&depCityName=")
      if (tempFrom.length < 2) {
        false
      } else {
        ifAirLine(tempFrom(1), "&depCityName=")
      }
    } else false
  }


  def ifAirLine(str: String,keyword:String): Boolean= {

    if(keyword=="&depCityName="){

      val parsefrom = str.split("&arrCityName=")

      if(parsefrom.length<2){
        false
      } else {
        ifExistChar(parsefrom(1))
      }
    } else {
      false
    }
  }

  def ifExistChar(str:String): Boolean={
    val temp = str.split('&')
    true
  }

  def decodeString(str:String):String= {
    val temp:String={
      URLDecoder.decode(str, "UTF8")
    }
    temp
  }

}
