package platform.ctrip

import java.util
import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Decoder


/**
  * Created by niujiaojiao on 2016/2/19.
  */

//统计 样本数据 每个设备上各个城市往返的航班的个数
object CtripParser extends App {

  val conf =  new SparkConf().setMaster("local").setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile("E:/CODE/000000_0").map(_.split("\t")).filter(x => x(3).contains("ctrip.com/")&&(x(2)!="NoDef")).map(x=>(parseDevice(x(2))+":  "+parse(x(3)),1)).
    reduceByKey(_+_).sortBy(_._2,ascending = false).foreach(println)

  def parseDevice(str:String):String={
    val rs = CommonUtil.decodeBase64(str)
    CommonUtil.getDevice(rs)
  }

  def parse(str:String): String ={
    if(str.contains("flights.ctrip.com/booking")){
      val str1= str.split("/booking/")
      val str2 = str1(1).substring(0,7)
      val three = str1(1).substring(0,3)
      var bl =true
      for(j <- three){
        if(j.isUpper){
        }
        else{
          bl = false
        }
      }
      if(bl){
        str2
      }else{
        "null"
      }
    }
    else{
      "null"
    }
  }
}
