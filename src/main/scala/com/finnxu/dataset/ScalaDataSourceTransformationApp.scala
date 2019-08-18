package com.finnxu.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

import scala.collection.mutable.ListBuffer

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-15 23:08
 * Description : TODO
 */
object ScalaDataSourceTransformationApp {

  // 笛卡尔积
  def crossFuncation(env: ExecutionEnvironment): Unit = {
    val info1 = List("中超", "重庆力帆")
    val info2 = List(1, 4, 8)
    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)
    data1.cross(data2).print()
  }

  def joinFuncation(env: ExecutionEnvironment): Unit = {
    val info1 = new ListBuffer[(Int, String)]
    info1.append((1, "马云"))
    info1.append((2, "李彦宏"))
    info1.append((3, "周涛"))
    info1.append((4, "马化腾"))

    val info2 = new ListBuffer[(Int, String)]
    info2.append((1, "杭州"))
    info2.append((2, "北京"))
    info2.append((3, "成都"))
    info2.append((6, "深证"))

    val data1 = env.fromCollection(info1)
    val data2 = env.fromCollection(info2)

    println("~~~~~~~~~~~~~~~~~~~~join")
    data1.join(data2).where(0).equalTo(0).apply((first, second) => {
      (first._1, first._2, second._2)
    }).print()
    println("~~~~~~~~~~~~~~~~~~~~leftOuterJoin")
    data1.leftOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
      if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
    println("~~~~~~~~~~~~~~~~~~~~rightOuterJoin")
    data1.rightOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "-", second._2)
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
    println("~~~~~~~~~~~~~~~~~~~~fullOuterJoin")
    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first, second) => {
      if (first == null) {
        (second._1, "-", second._2)
      } else if (second == null) {
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }

  def distinctFuncation(env: ExecutionEnvironment): Unit = {
    val list = new ListBuffer[String]
    list.append("hadoop,spark")
    list.append("spark,flink")
    list.append("flink,flink")
    val data = env.fromCollection(list)
    data.flatMap(_.split("[,]")).distinct().map((_, 1)).groupBy(0).sum(1).print()
  }

  def flatmapFuncation(env: ExecutionEnvironment): Unit = {
    val list = new ListBuffer[String]
    list.append("hadoop,spark")
    list.append("spark,flink")
    list.append("flink,flink")
    val data = env.fromCollection(list)
    data.print()
    data.flatMap(_.split("[,]")).map((_, 1)).groupBy(0).sum(1).print()

  }

  def fisrtFuncation(env: ExecutionEnvironment): Unit = {
    val list = new ListBuffer[(Int, String)]
    list.append((1, "Hadoop"))
    list.append((1, "Spark"))
    list.append((1, "Flink"))
    list.append((2, "Java"))
    list.append((2, "SSH"))
    list.append((3, "MySQL"))
    list.append((3, "SqlServer"))
    list.append((4, "Scala"))

    val data = env.fromCollection(list)
    println("取前三个")
    data.first(3).print()
    println("按第一个分组，取第一个")
    data.groupBy(0).first(1).print()
    println("按第一个分组，对分组后的数据升序排列取前两个")
    data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print()
    println("按第一个分组，对分组后的数据降序排列取前两个")
    data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print()
  }

  // 100个元素入到数据库中
  def mapPartitionFuncation(env: ExecutionEnvironment): Unit = {
    val students = new ListBuffer[String]
    for (i <- 1 to 100) {
      students.append("students: " + i)
    }
    val data = env.fromCollection(students).setParallelism(4)
    data.mapPartition(partStudent => {
      val connection = DBUtils.getConnection()
      println("connect :" + connection)
      DBUtils.returnConnection(connection)
      partStudent
    }).print()
  }

  def filterFuncation(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.map(_ + 1).filter(_ > 5).print()

  }

  def mapFuncation(env: ExecutionEnvironment): Unit = {
    val data = env.fromCollection(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    data.map(_ + 1).print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //    mapFuncation(env)
    //    filterFuncation(env)
    //    mapPartitionFuncation(env)
    //    fisrtFuncation(env)
    //    flatmapFuncation(env)
    //    distinctFuncation(env)
    //    joinFuncation(env)
    crossFuncation(env)
  }

}
