package platform.ch

import org.apache.spark.{SparkContext, SparkConf}
import util.CommonUtil

/**
  * Created by yangshuai on 2/19/16.
  */
object ChParser {

  def main(args: Array[String]): Unit = {

    var source = "file:///Users/yang/code/TEMP/000000_0"
    var target = "file:///Users/yang/code/TEMP/result"
    if (args.length > 0) {
      source = args(0)
      target = args(1)
    }

    val conf =  new SparkConf().setAppName("CH")
    val sc = new SparkContext(conf)

    sc.textFile(source)
      .map(_.split("\t"))
      .filter(_.length == 5)
      .filter(x => x(2) != "NoDef" && x(3).contains("flights.ch.com/"))
      .filter(x => filter(x(3)))
      .map(x=>(CommonUtil.getPrefix(x) + parse(x(3)),1))
      .reduceByKey(_+_)
      .sortBy(_._2,ascending = false)
      .saveAsTextFile(target)

    sc.stop()
  }

  def filter(url: String): Boolean = {
    val arr = url.split("flights.ch.com/")
    if (arr.length < 2)
      return false
    if (arr(1).length() >= 13 && arr(1).substring(0, 6) == "round-" && arr(1).charAt(9) == '-') {
      true
    } else if (arr(1).length() >= 7 && arr(1).charAt(3) == '-') {
      true
    } else {
      false
    }
  }

  def parse(url: String): String = {
    val arr = url.split("flights.ch.com/")
    if (arr(1).substring(0, 6) == "round-" && arr(1).charAt(9) == '-') {
      arr(1).substring(6, 13)
    } else if (arr(1).charAt(3) == '-') {
      arr(1).substring(0, 7)
    } else {
      ""
    }
  }
}
