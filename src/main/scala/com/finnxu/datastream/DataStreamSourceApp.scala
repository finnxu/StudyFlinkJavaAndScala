package com.finnxu.datastream

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 01:40
 * Description : TODO
 */
object DataStreamSourceApp {

  def richParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomRichParallelSourceFunction).setParallelism(4)
    data.print()
  }

  def parallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomParallelSourceFunction).setParallelism(4)
    data.print()
  }

  def nonParallelSourceFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.print()
  }

  def socktFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.socketTextStream("localhost", 9999)
    data.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1).print()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //    socktFunction(env)
    //    nonParallelSourceFunction(env)
    //    parallelSourceFunction(env)
    richParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

}
