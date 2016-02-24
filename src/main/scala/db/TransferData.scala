package db

import java.io.File
import java.nio.charset.Charset
import java.sql.{Connection, DriverManager}

import com.google.common.io.Files
import util.CommonUtil


/**
  * Created by yangshuai on 2/20/16.
  */
object TransferData extends App {

  CommonUtil.initCodeMap()

  val url = "jdbc:mysql://222.73.34.91:3306/travel"
  val driver = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "hadoop"
  var connection: Connection = _

  val file = new File("E:/part-00000")
  var line = ""

  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    connection.setAutoCommit(false)
    val insertStr = "insert into lines_get (platform, date, hour, device, line, count) values (?, ?, ?, ?, ?, ?)"
    val ps = connection.prepareStatement(insertStr)

    var i = 0
//    val writer = new PrintWriter(new File("E:/DataCube/src/main/resource/test.txt" ))
    Files.readLines(file, Charset.forName("utf-8")).toArray.foreach(line => {

      val length = line.toString.length - 1
      val source = line.toString.substring(1, length)

      val arr = source.split("-->>")
      if (arr(0) != "not a timestamp") {

        val date = arr(0).substring(0, 10)
        val hour = arr(0).substring(11, 13)
        val device = arr(1)
        val arr2 = arr(2).split(",")
        val airLine = arr2(0)

        val airLineCn = CommonUtil.convertAirLine(airLine)

        /*if(airLineCn!="") {
          writer.write(airLineCn + "\n")
        }*/
        if (airLineCn!="none") {
          val count = arr2(1).toInt

          ps.setString(1, "qn")
          ps.setString(2, date)
          ps.setInt(3, hour.toInt)
          ps.setString(4, device)
          ps.setString(5, airLineCn)
          ps.setInt(6, count)

          ps.addBatch()

          if (i % 1000 == 0)
            ps.executeBatch()

          i += 1
        }
      }
    })
//    writer.close()
    ps.executeBatch()
    connection.commit()

  } catch {
    case e: Exception => e.printStackTrace()
  }

  connection.close()

}
