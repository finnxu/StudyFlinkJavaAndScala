package com.finnxu.connection

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.fs.StringWriter
import org.apache.flink.streaming.connectors.fs.bucketing.{BucketingSink, DateTimeBucketer}

/**
 * PackageName : com.finnxu.connection
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-20 21:56
 * Description : TODO
 */
object FileSyatemSinkApp {

  def fileSystemSink(env: StreamExecutionEnvironment): Unit = {
    val data = env.socketTextStream("localhost", 9999)
    val filePath = "/media/alvinxu/学习工作/Flink/Project/StudyFlinkJavaAndScala/src/main/output/hdfsSink"
    val sink = new BucketingSink[String](filePath)
    sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
    sink.setWriter(new StringWriter())
    //    sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
    sink.setBatchRolloverInterval(20);

    data.addSink(sink)

  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    fileSystemSink(env)
    env.execute("FileSyatemSinkApp")
  }

}
