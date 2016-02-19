package test

import java.util
import java.util.{Locale, Date}
import java.text.SimpleDateFormat
/**
  * Created by Administrator on 2016/2/19.
  */
object MyTest extends App{
  val loc = new Locale("en")
  val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",loc)
  val tm = "30/Jul/2015:05:00:50"
  val dt2 = fm.parse(tm);
  //println(dt2.getTime())
  val str =  "9fa6e225d4342de266018a5c878a5b7a32c41fd5"
  DateFormat(str)
  def DateFormat(time:String):String={
    var sdf= new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var date = sdf.format(new Date((time.toLong)))

    date

  }
}
