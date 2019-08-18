package com.finnxu.datastream

import org.apache.flink.streaming.api.functions.source.SourceFunction

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 12:28
 * Description : TODO
 */
class CustomNonParallelSourceFunction extends SourceFunction[Long] {
  var count = 1L
  var isrunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isrunning) {
      ctx.collect(count)
      count += 1
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isrunning = false
  }
}
