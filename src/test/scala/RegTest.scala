import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by yangshuai on 2/19/16.
  */
object RegTest extends App {


//  val URL= "http://flights.ch.com/SHA-SHE.html?IfRet=false&OriCity=%E4%B8%8A%E6%B5%B7&OriCode=SHA&DestCity=%E6%B2%88%E9%98%B3&DestCode=SHE&FDate=2016-02-20&MType=0&ANum=1&CNum=0&INum=0&SType=0"
//
//  val pattern = ".*flights.ch.com/(round-)?\\w{3}-\\w{3}.html.*"
//
//  println(URL.matches(pattern))
//  println("(?<=.*flights.ch.com/(round-)?)\\w{3}-\\w{3}(?=.html.*)".r.findFirstIn(URL).getOrElse(""))

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH")
  val date: String = sdf.format(new Date().getTime)
  println(date)

}
