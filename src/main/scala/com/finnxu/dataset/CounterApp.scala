package com.finnxu.dataset

import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 20:27
 * Description : TODO
 */
object CounterApp {

  def countTest(env: ExecutionEnvironment): Unit = {
    val data = env.fromElements("Hadoop", "Scala", "Spark", "Flink", "Sotrm", "Hive")
    val filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/output/counter/"
    data.map(new RichMapFunction[String, String]() {
      val counter = new IntCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("counter-scala", counter)
      }

      override def map(value: String): String = {
        counter.add(1)
        value
      }
    }).setParallelism(6).writeAsText(filePath, WriteMode.OVERWRITE)

    val job = env.execute("CounterApp")
    val num = job.getAccumulatorResult[Int]("counter-scala")
    println("num : " + num)
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    countTest(env)
  }
}
