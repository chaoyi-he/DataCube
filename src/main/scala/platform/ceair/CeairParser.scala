package platform.ceair

import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by niujiaojiao on 2016/2/19.
  */
object CeAirParser extends App{
  val source = args(0)
  val target = args(1)

  val conf =  new SparkConf().setAppName("CEAIR")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("ceair.com/flight2014/") && (x(2)!="NoDef"))
    .filter(x=> containsAirLine(x(3)))
    .map(x=>(CommonUtil.getPrefix(x) +extractAirLine(x(3)),1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false).saveAsTextFile(target)


  def extractAirLine(str:String): String ={

    val str1 = str.split("ceair.com/flight2014/")
    var rightStr = ""
    if(str1(1).contains("booking")){ //some urls contain two "booking"
    val str2 = str1(1).split("booking/")
      rightStr=str2(1)
    } else {
      rightStr = str1(1)
    }
    rightStr.substring(0,7).toUpperCase()
  }

  def AirLine(url: Array[String]): Boolean = {
    if (url.length < 2) {
      return false
    }
    val rightStr = url(1)
    if(rightStr.length < 7) {
      false
    } else {
      ifAirLine(rightStr)
    }
  }
  def containsAirLine(url: String):Boolean={
    val arr = url.split("ceair.com/flight2014/")
    var rightStr = ""
    if(arr.length <2){
      return false
    }
    if(arr(1).contains("booking")){
      AirLine(arr(1).split("booking"))
    }
    AirLine(arr)
  }

  def ifAirLine(str:String): Boolean={
    val st= str.charAt(7)
    if ((st >= 'a' && st <= 'z') || (st >= 'A' && st <= 'Z')) {
      false
    } else {
      if (str.charAt(3) != '-'){
        false
      } else {
        true
      }
    }
  }

}
