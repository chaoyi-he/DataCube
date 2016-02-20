package platform.qunar

import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}
import java.net.URLDecoder
/**
  * Created by Administrator on 2016/2/20.
  */
object QunarParser extends App {
  val source = args(0)
  val target = args(1)

  val conf = new SparkConf().setMaster("local").setAppName("QUNAR")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("flight.qunar.com/") && (x(2) != "NoDef"))
    .filter(x => containsAirLine(x(3)))
    .map(x => (CommonUtil.getPrefix(x) + extractAirLine(x(3)), 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false).foreach(println)


  def extractAirLine(url: String): String = {
    var substring = ""
    var left = ""
    var right = ""
    if (url.contains("&from=") && url.contains("&to=")) {
      substring = url.split("&from=")(1)
      left = substring.split("&to=")(0)
      right = substring.split("&to=")(1).split('&')(0)

    } else if (url.contains("&dep=") && url.contains("&arr=")) {
      substring = url.split("&dep=")(1)
      left = substring.split("&arr=")(0)
      right = substring.split("&arr=")(1).split('&')(0)
    }
    decodeString(left) + "-" + decodeString(right)
  }

  def containsAirLine(url: String): Boolean = {
    if (url.contains("&from=") && url.contains("&to=")) {
      val tempfrom = url.split("&from=")
      if (tempfrom.length < 2) {
        return false
      } else {
        ifAirLine(tempfrom(1), "&from=")
      }
    } else if (url.contains("&dep=") && url.contains("&arr=")) {
      val tempdep = url.split("&dep=")
      if (tempdep.length < 2) {
        return false
      } else {
        ifAirLine(tempdep(1), "&dep=")
      }
    } else false
  }


  def ifAirLine(str: String,keyword:String): Boolean= {
    if(keyword=="&from="){
      val parsefrom = str.split("&to=")
      if(parsefrom.length<2){
        return false
      }else{
        ifexistchar(parsefrom(1))
      }
    }else if(keyword=="&dep="){
      val parsedep = str.split("&arr=")
      if(parsedep.length<2){
        return false
      }else{
        ifexistchar(parsedep(1))
      }
    }else return false
  }

  def ifexistchar(str:String): Boolean={
    val temp = str.split('&')
    if(temp.length<2){
      false
    }else true
  }

  def decodeString(str:String):String= {
    val temp:String={
      URLDecoder.decode(str, "UTF8")
    }
    temp
  }
}
