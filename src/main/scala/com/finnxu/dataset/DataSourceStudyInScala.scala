package com.finnxu.dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

/**
 * Created with IDEA
 * Author: finnxu
 * Date:2019-08-14 21:44
 * Description: FLink学习Scala版本
 **/
object DataSourceStudyInScala {

  def fromCsvFile(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.csv"
    println("-----------直接读取---")
    env.readCsvFile[(String, Int, String)](path, ignoreFirstLine = true).print()
    println("-----------读取前面两个元素---")
    env.readCsvFile[(String, Int)](path, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
    println("-----------自定义cese类读取---")
    env.readCsvFile[MyCase](path, ignoreFirstLine = true, includedFields = Array(0, 1)).print()
    println("-----------自定义Java pojo类读取---")
    env.readCsvFile[Person](path, ignoreFirstLine = true, pojoFields = Array("name", "age", "sex")).print()
  }

  case class MyCase(name: String, age: Int)

  def fromTextFile(env: ExecutionEnvironment): Unit = {
    val path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.txt"
    env.readTextFile(path).print()
  }

  def fromCollections(env: ExecutionEnvironment): DataSet[Int] = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
    env.fromCollection(data)
  }

  def main(args: Array[String]): Unit = {
    import org.apache.flink.api.scala._
    val env = ExecutionEnvironment.getExecutionEnvironment
    val collectionData = fromCollections(env)
    collectionData.map(data => {
      data.toString
    })
    //    fromTextFile(env)
    fromCsvFile(env)
  }
}
