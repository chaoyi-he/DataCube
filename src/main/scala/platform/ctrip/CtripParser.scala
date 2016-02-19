package platform.ctrip

import java.util
import java.util.Date
import java.text.SimpleDateFormat
import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Decoder


/**
  * Created by niujiaojiao on 2016/2/19.
  */

//根据样本数据: 统计每个设备上各个城市往返的航班的个数
object CtripParser extends App {

  val conf =  new SparkConf().setMaster("local").setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile("E:/CODE/000000_0").map(_.split("\t")).filter(x => x(3).contains("ctrip.com/")&&(x(2)!="NoDef")).filter(x=>(parse(x(3))!="null")).
     map(x=>("Date: "+DateFormat(x(0))+" Device: "+parseDevice(x(2))+":  "+parse(x(3)),1)).
      reduceByKey(_+_).sortBy(_._2,ascending = false).foreach(println)

  def parseDevice(str:String):String={
    val rs = CommonUtil.decodeBase64(str)
    CommonUtil.getDevice(rs)
  }

  def parse(str:String): String ={
    if(str.contains("flights.ctrip.com/booking")){
      var str1= str.split("flights.ctrip.com/booking/")
      var leftString = ""
      if(str1(1).contains("booking")){ //some urls contain two "booking"
        val str2 = str1(1).split("booking/")
        leftString=str2(1)
      }
      else{
        leftString=str1(1)
      }
      val gt=parseCity(leftString)
      if(getCity(gt)!="airport") { // filter the "airport"
        return getCity(gt)
      }
      else
        "null"
    }
    else
      "null"
  }

  // according the seventh char
  def parseCity(str:String):String={
    val st= str.charAt(7)
    st match{
      case '/' => return str
      case '-' => return str
      case _ =>return "null"
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

  // get the date: year-month-date
  def DateFormat(time:String):String={
     var sdf= new SimpleDateFormat("yyyy-MM-dd")
     var date = sdf.format(new Date((time.toLong)))
     date
  }
}
