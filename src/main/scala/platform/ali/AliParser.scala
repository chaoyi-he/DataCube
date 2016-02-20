package platform.ali


import _root_.util.CommonUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by niujiaojiao on 2016/2/19.
  */
object AliParser extends App{
  val source = args(0)
  val target = args(1)

  val conf =  new SparkConf().setAppName("ALI")
  val sc = new SparkContext(conf)

  sc.textFile(source)
    .map(_.split("\t"))
    .filter(_.length == 5)
    .filter(x => x(3).contains("alitrip.com/") && (x(2)!="NoDef")
     && (x(3).contains("depCity=")) && (x(3).contains("arrCity=")))
    .filter(x=> containsAirLine(x(3)))
    .map(x=>(CommonUtil.getPrefix(x) +extractAirLine(x(3)),1))
    .reduceByKey(_+_)
    .sortBy(_._2,ascending = false).saveAsTextFile(target)


  def extractAirLine(str:String): String ={
    val str1 = str.split("depCity=")
    val str2 = str.split("arrCity=")
    str1(1).substring(0,3).toUpperCase()+"-"+str2(1).substring(0,3).toUpperCase()
  }

  def containsAirLine(url: String): Boolean = {
    val arr = url.split("depCity=")
    val arr1 = url.split("arrCity=")
    if ((arr.length<2)||(arr1.length<2)) {
      return false
    }
    if((arr(1).length<3)||(arr1(1).length<3)){
      return false
    }
    if((!ifAirLine(arr(1))) || (!ifAirLine(arr1(1)))) {
      return false
    }else{
      return true
    }
  }

  def ifAirLine(str:String): Boolean={
    if (str.charAt(3) != '&'){
      false
    } else {
      true
    }
  }

}
