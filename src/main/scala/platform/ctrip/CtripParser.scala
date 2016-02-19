package platform.ctrip

import java.util
import platform.ctrip.Parser
import org.apache.spark.{SparkContext, SparkConf}
import sun.misc.BASE64Decoder
import scala.util.matching.Regex

/**
  * Created by niujiaojiao on 2016/2/19.
  */

//统计 样本数据 每个设备上各个城市往返的航班的个数
object CtripParser extends App{
  val conf =  new SparkConf().setMaster("local").setAppName("CTRIP")
  val sc = new SparkContext(conf)
  sc.textFile("E:/CODE/000000_0").map(_.split("\t")).filter(x => x(3).contains("ctrip.com/")&&(x(2)!="NoDef")).map(x
  =>(parseDevice(x(2))+":  "+parse(x(3)).filter(x=> x!=null),1)).
    reduceByKey(_+_).sortBy(_._2,ascending = false).foreach(println)

  def parseDevice(str:String):String={
    val rs = Parser.base64Paser(str)
    Parser.equip(rs)
  }
 // def parse1(str:String): String = {
    //val regex2 = new Regex("\\w[3]\-\\w[3]")

 //   str match{
     //case regex2(num,str) => str
     // case _=> "not matched"
   // }
 // }
  def parse(str:String): String ={
    if(str.contains("flights.ctrip.com/booking")){
      val str1= str.split("/booking/")
      val cityGo = str1(1).substring(0,3)
      val cityBack= str1(1).substring(4,7)
      if((cityGo.toString.length==3) && (cityBack.toString.length==3)){
        str+"********"+cityGo+"*"+cityBack
      }
      else
        "null"
    }
    else{
      "null"
    }
  }
}
