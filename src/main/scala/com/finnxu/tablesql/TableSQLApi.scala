package com.finnxu.tablesql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.types.Row

/**
 * PackageName : com.finnxu.tablesql
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 23:09
 * Description : TODO
 */
object TableSQLApi {
  def tableSQLApi(env: ExecutionEnvironment, table: BatchTableEnvironment): Unit = {
    val filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.csv"
    val csvData = env.readCsvFile[PersonInfo](filePath, ignoreFirstLine = true)
    val tableInfo = table.fromDataSet(csvData)
    table.registerTable("person", tableInfo)
    val result = table.sqlQuery("select * from person where age >= 20")
    table.toDataSet[Row](result).print()
  }

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val table = BatchTableEnvironment.create(env)
    tableSQLApi(env, table)
  }

  case class PersonInfo(name: String, age: Int, sex: String, address1: String, address2: String)

}
