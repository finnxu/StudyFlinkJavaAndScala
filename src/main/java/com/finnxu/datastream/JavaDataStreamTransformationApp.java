package com.finnxu.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 16:04
 * Description : TODO
 */
public class JavaDataStreamTransformationApp {
    private static void spiltSelectFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        SplitStream<Long> split = data.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                ArrayList<String> list = new ArrayList<>();
                if (value % 2 == 0) {
                    list.add("even");
                } else {
                    list.add("odd");
                }
                return list;
            }
        });
//        split.select("even").print();
//        split.select("odd").print();
        split.select("odd", "even").print();
    }

    private static void unionFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data1 = env.addSource(new JavaCustomNonParallelSourceFunction());
        DataStreamSource<Long> data2 = env.addSource(new JavaCustomNonParallelSourceFunction());
        data1.union(data2).print();
    }

    private static void filterFunction(StreamExecutionEnvironment env) {
        DataStreamSource<Long> data = env.addSource(new JavaCustomNonParallelSourceFunction());
        data.map((MapFunction<Long, Long>) value -> {
            System.out.println("receiveed : " + value);
            return value;
        }).filter((FilterFunction<Long>) value -> value % 2 == 0).setParallelism(1).print();

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        filterFunction(env);
//        unionFunction(env);
        spiltSelectFunction(env);
        env.execute("JavaDataStreamTransformationApp");
    }
}
