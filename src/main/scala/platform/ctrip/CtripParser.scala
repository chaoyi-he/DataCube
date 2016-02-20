package platform.ctrip

import java.text.SimpleDateFormat
import java.util.Date
import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by niujiaojiao on 2016/2/19.
  */

//根据样本数据: 统计每个设备上各个城市往返的航班的个数
object CtripParser extends App {

  var source = "file:///Users/yang/code/TEMP/000000_0"
  var target = "file:///Users/yang/code/TEMP/result"
  if (args.length > 0) {
    source = args(0)
    target = args(1)
  }

  val conf =  new SparkConf().setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("ctrip.com/") && (x(2)!="NoDef"))
    .filter(x=> parse(x(3)) != "null")
    .map(x=>(CommonUtil.getPrefix(x) + parse(x(3)),1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false)
    .saveAsTextFile(target)

  def parse(str:String): String ={

    if(str.contains("flights.ctrip.com/booking")){

      val str1 = str.split("flights.ctrip.com/booking/")
      var rightStr = ""
      if (str1.length < 2)
        return "null"

      if(str1(1).contains("booking")){ //some urls contain two "booking"
        val str2 = str1(1).split("booking/")
        if (str2.length < 2)
          return "null"
        rightStr=str2(1)
      } else {
        rightStr=str1(1)
      }

      val gt=parseCity(rightStr)

      if(getCity(gt)!="airport") { // filter the "airport"
        getCity(gt)
      } else {
        "null"
      }

    } else {
      "null"
    }
  }

  // according the seventh char
  def parseCity(str:String):String={
    if (str.length < 7)
      return "null"
    val st= str.charAt(7)
    st match{
      case '/' => str
      case '-' => str
      case _ =>"null"
    }
  }

  //get the first seven string : xxx-xxx
  def getCity(str:String):String= {
    var str1 = "null"
    if (str.toString != "null") {
      str1 = str.substring(0,7)
    }
    str1
  }

}
