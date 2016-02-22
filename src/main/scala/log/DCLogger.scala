package log

import org.apache.log4j.Logger


/**
  * Created by yangshuai on 2/20/16.
  */
object DCLogger {

  val logger = Logger.getRootLogger

  def warn(str: String): Unit = {
    logger.warn(str)
  }

  def error(str: String): Unit = {
    logger.error(str)
  }

  def exception(e: Exception) = {
    logger.error(e.printStackTrace())
  }

}
