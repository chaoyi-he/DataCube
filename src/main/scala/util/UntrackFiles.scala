package util

import scala.sys.process._

/**
  * Created by yangshuai on 2016/1/17.
  * 慎用!!!
  */
object UntrackFiles extends App {

//  val cd = "dir".!!
//  val cdr = cd.!!
  val cmd = "git status"
  val output = cmd.!!
  val arr = output.split("\n")

  for (i <- 13 until arr.size-3) {
        val untrackCmd = "git update-index --assume-unchanged " + arr(i).substring(12)
        val op = untrackCmd.!!
//        println(arr(i))
    //    println(arr)
  }
}
