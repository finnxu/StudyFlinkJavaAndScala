package com.finnxu.timewindows

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * PackageName : com.finnxu.timewindows
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-19 22:33
 * Description : TODO
 */
object WindowReduceApp {
  def timeWindows(env: StreamExecutionEnvironment): Unit = {
    val data = env.socketTextStream("localhost", 9999)
    data.flatMap(_.split(" "))
      .map(data => (1, data.toInt))
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce((x, y) => {
        println(x + "...." + y)
        (x._1, x._2 + y._2)
      })
      .setParallelism(1).print()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    timeWindows(env)
    env.execute("WindowAppl")
  }

}
