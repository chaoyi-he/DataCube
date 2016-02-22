package platform.ctrip

import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by niujiaojiao on 2016/2/19.
  */

//根据样本数据: 统计每个设备上各个城市往返的航班的个数
object CtripParser extends App {

  val source = args(0)
  val target = args(1)

  val conf =  new SparkConf().setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("flights.ctrip.com/booking") && (x(2)!="NoDef"))
    .filter(x=> containsAirLine(x(3)))
    .map(x=>(CommonUtil.getPrefix(x) + extractAirLine(x(3)),1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false).saveAsTextFile(target)


  def extractAirLine(str:String): String ={

    val str1 = str.split("flights.ctrip.com/booking/")
    var rightStr = ""
    if(str1(1).contains("booking")){ //some urls contain two "booking"
      val str2 = str1(1).split("booking/")
      rightStr=str2(1)
    } else {
      rightStr = str1(1)
    }
    rightStr.substring(0,7).toUpperCase()
  }

  def containsAirLine(url: String): Boolean = {
    val arr = url.split("flights.ctrip.com/booking/")
    var rightStr=""
    if (arr.length < 2) {
      return false
    } else if(arr(1).contains("booking")) {
      //some urls contain two "booking"
      val str2 = arr(1).split("booking/")
      if(str2.length < 2){
        return false
      } else {
        rightStr= str2(1)
      }
    } else {
      rightStr= arr(1)
    }

    if(rightStr.length < 7) {
      false
    } else {
      ifAirLine(rightStr)
    }
  }


  // according the seventh char
  def ifAirLine(str:String): Boolean={
    val st= str.charAt(7)
    if ((st >= 'a' && st <= 'z') || (st >= 'A' && st <= 'Z')) {
      false
    } else {
      if (str.charAt(3) != '-' && str.charAt(3) != '.'){
        false
      } else {
        true
      }
    }
  }

}
