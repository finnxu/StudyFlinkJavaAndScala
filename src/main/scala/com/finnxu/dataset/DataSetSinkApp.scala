package com.finnxu.dataset

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 18:16
 * Description : TODO
 */
object DataSetSinkApp {

  def sinkTest(env: ExecutionEnvironment): Unit = {
    val info = 1 to 10
    val data = env.fromCollection(info)
    val path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/output/test.txt"
    data.writeAsText(path, WriteMode.OVERWRITE).setParallelism(2)
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    sinkTest(env)
    env.execute("DataSetSinkApp")
  }
}
