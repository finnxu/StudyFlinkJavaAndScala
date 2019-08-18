package com.finnxu.dataset

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 22:12
 * Description : TODO
 */
object DistributionCacheApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements("Hadoop", "Scala", "Spark", "Flink", "Sotrm", "Hive")
    val filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.txt"
    // step1 注册
    env.registerCachedFile(filePath, "local-cache")

    data.map(new RichMapFunction[String, String] {

      override def open(parameters: Configuration): Unit = {
        // step2 获取到分布式缓存的内容即可
        val chFile = getRuntimeContext.getDistributedCache.getFile("local-cache")
        val lines = FileUtils.readLines(chFile)
        import scala.collection.JavaConverters._
        for (line <- lines.asScala) {
          println(line)
        }
      }

      override def map(value: String): String = {
        value
      }
    }).print()
  }

}
