package platform.ctrip

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by yangshuai on 2016/2/18.
  */
object Parser extends App {

  val conf =  new SparkConf().setMaster("local").setAppName("CTRIP")
  val sc = new SparkContext(conf)

  sc.textFile("E:\\CODE\\000000_0").map(_.split("\t")).filter(_(3).contains("ctrip.com/")).foreach{x => println(x(3))}

  sc.stop()
}
