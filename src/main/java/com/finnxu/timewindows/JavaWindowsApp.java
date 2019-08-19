package com.finnxu.timewindows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * PackageName : com.finnxu.timewindows
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-19 22:40
 * Description : TODO
 */
public class JavaWindowsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        timeWindows(env);
        env.execute("JavaWindowsApp");
    }

    private static void timeWindows(StreamExecutionEnvironment env) {
        DataStreamSource<String> data = env.socketTextStream("localhost", 9999);
        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String elem : split) {
                    if (elem.length() > 0) {
                        out.collect(new Tuple2<>(elem, 1));
                    }
                }
            }
        }).keyBy(0)
//                .timeWindow(Time.seconds(5))
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .sum(1)
                .setParallelism(1)
                .print();
    }
}
