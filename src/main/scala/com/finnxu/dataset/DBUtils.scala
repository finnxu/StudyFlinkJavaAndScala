package com.finnxu.dataset

import scala.util.Random

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-15 23:24
 * Description : TODO
 */
object DBUtils {
  def getConnection() = {
    new Random().nextInt(10) + ""
  }

  def returnConnection(connection: String): Unit = {

  }

}
