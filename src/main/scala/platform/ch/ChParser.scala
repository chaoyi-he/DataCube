package platform.ch

import java.text.SimpleDateFormat
import java.util.{Date, Calendar}

import org.apache.spark.{SparkContext, SparkConf}
import util.CommonUtil

/**
  * Created by yangshuai on 2/19/16.
  */
object ChParser {

  def main(args: Array[String]): Unit = {

    var source = "file:///Users/yang/code/TEMP/sample"
    var target = "file:///Users/yang/code/TEMP/result"
    if (args.length > 0) {
      source = args(0)
      target = args(1)
    }

    val conf =  new SparkConf().setMaster("local").setAppName("CH")
    val sc = new SparkContext(conf)

    sc.textFile(source)
      .map(_.split("\t"))
      .filter(x => x(2) != "NoDef" && x(3).contains("flights.ch.com/"))
      .filter(x => filter(x(3)))
      .map(x=>(parseDate(x(0)) + ":" + parseDevice(x(2)) + ":" + parse(x(3)),1))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)
      .saveAsTextFile(target)

    sc.stop()
  }

  def parseDevice(str:String):String={
    val rs = CommonUtil.decodeBase64(str)
    CommonUtil.getDevice(rs)
  }

  def filter(str: String): Boolean = {
    if (str.matches(".*flights.ch.com/(round-)?\\w{3}-\\w{3}.*"))
      return true
    false
  }

  def parse(url:String): String ={
    if (url.matches(".*flights.ch.com/(round-)?\\w{3}-\\w{3}.*")) {
      "(?<=.*flights.ch.com/(round-)?)\\w{3}-\\w+(?=.*)".r.findFirstIn(url).getOrElse(url).toUpperCase()
    } else {
      "NOT FOUND"
    }
  }

  def parseDate(timeStamp: String):String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
    val date: String = sdf.format(timeStamp.toLong)
    date
  }
}
