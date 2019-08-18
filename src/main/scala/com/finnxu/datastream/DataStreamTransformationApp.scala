package com.finnxu.datastream

import java.{lang, util}

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.collector.selector.OutputSelector

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 15:59
 * Description : TODO
 */
object DataStreamTransformationApp {

  def spiltSelectFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    val splits = data.split(new OutputSelector[Long] {
      override def select(value: Long): lang.Iterable[String] = {
        val list = new util.ArrayList[String]()
        if (value % 2 == 0) {
          list.add("even")
        } else {
          list.add("odd")
        }
        list
      }

      /*(num: Int) =>
        (num % 2) match {
          case 0 => List("even")
          case 1 => List("odd")
        }*/
    })
    //    splits.select("even").print()
    //    splits.select("odd").print()
    splits.select("odd", "even").print()
  }

  def unionFunction(env: StreamExecutionEnvironment): Unit = {
    val data1 = env.addSource(new CustomNonParallelSourceFunction)
    val data2 = env.addSource(new CustomNonParallelSourceFunction)
    data1.union(data2).print()
  }

  def filterFunction(env: StreamExecutionEnvironment): Unit = {
    val data = env.addSource(new CustomNonParallelSourceFunction)
    data.map(x => {
      println("receiveed : " + x)
      x
    }).filter(_ % 2 == 0).print()
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    filterFunction(env)
    //    unionFunction(env)
    spiltSelectFunction(env)
    env.execute("DataStreamTransformationApp")
  }

}
