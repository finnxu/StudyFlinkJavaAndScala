package com.finnxu.dataset;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-15 23:09
 * Description : TODO
 */
public class JavaDataSourceTransformationApp {

    public static void corssFuncation(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<>();
        info1.add("中超");
        info1.add("重启力帆");
        info1.add("苏宁易购");
        List<Integer> info2 = new ArrayList<>();
        info2.add(3);
        info2.add(5);
        info2.add(1);

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<Integer> data2 = env.fromCollection(info2);
        data1.cross(data2).print();
    }

    public static void joinFuncation(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<>();
        List<Tuple2<Integer, String>> info2 = new ArrayList<>();
        info1.add(new Tuple2<>(1, "马云"));
        info1.add(new Tuple2<>(2, "李彦宏"));
        info1.add(new Tuple2<>(3, "周涛"));
        info1.add(new Tuple2<>(4, "马化腾"));

        info2.add(new Tuple2<>(1, "杭州"));
        info2.add(new Tuple2<>(2, "北京"));
        info2.add(new Tuple2<>(3, "成都"));
        info2.add(new Tuple2<>(6, "深证"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        System.out.println("~~~~~~~~~~~~~~~~~~~~join");
        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<>(first.f0, first.f1, second.f1);
            }
        }).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~leftOuterJoin");
        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~rightOuterJoin");
        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {

                if (first == null) {
                    return new Tuple3<>(second.f0, "-", second.f1);
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
            }
        }).print();
        System.out.println("~~~~~~~~~~~~~~~~~~~~fullOuterJoin");
        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            @Override
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<>(first.f0, first.f1, second.f1);
                }
            }
        }).print();

    }

    public static void distinctFuncation(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("spark,flink");
        list.add("flink,flink");
        DataSource<String> data = env.fromCollection(list);
        data.print();
        data.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] elems = value.split(",");
                for (String elem : elems) {
                    if (elem.length() > 0) {
                        out.collect(elem);
                    }
                }
            }
        }).distinct().print();
    }

    public static void flatmapFuncation(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<>();
        list.add("hadoop,spark");
        list.add("spark,flink");
        list.add("flink,flink");
        DataSource<String> data = env.fromCollection(list);
        data.print();
        data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String input, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] elems = input.split("[,]");
                for (String elem : elems) {
                    if (elem.length() > 0) {
                        out.collect(new Tuple2<>(elem, 1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();

    }

    public static void firstFuncation(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> list = new ArrayList<>();
        list.add(new Tuple2<>(1, "Hadoop"));
        list.add(new Tuple2<>(1, "Spark"));
        list.add(new Tuple2<>(1, "Flink"));
        list.add(new Tuple2<>(2, "Java"));
        list.add(new Tuple2<>(2, "SSH"));
        list.add(new Tuple2<>(3, "MySQL"));
        list.add(new Tuple2<>(3, "SqlServer"));
        list.add(new Tuple2<>(4, "Scala"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(list);
        System.out.println("取前三个");
        data.first(3).print();
        System.out.println("按第一个分组，取第一个");
        data.groupBy(0).first(1).print();
        System.out.println("按第一个分组，对分组后的数据升序排列取前两个");
        data.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("按第一个分组，对分组后的数据降序排列取前两个");
        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();
    }


    public static void mapPartitionFuncation(ExecutionEnvironment env) throws Exception {
        List<String> students = new ArrayList<>();
        for (int i = 1; i <= 100; i++) {
            students.add("student : " + i);
        }
        DataSource<String> data = env.fromCollection(students).setParallelism(6);
        data.mapPartition((new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> input, Collector<String> out) throws Exception {
                String connection = DBUtils.getConnection();
                System.out.println(connection);
                DBUtils.returnConnection(connection);
            }
        })).print();
    }

    public static void filterFuncation(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map((MapFunction<Integer, Integer>) input -> input + 1)
                .filter((FilterFunction<Integer>) input -> input > 5).print();
    }

    public static void mapFuncation(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        data.map((MapFunction<Integer, Integer>) input -> input + 1).print();
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        mapFuncation(env);
//        filterFuncation(env);
//        mapPartitionFuncation(env);
//        firstFuncation(env);
//        flatmapFuncation(env);
//        distinctFuncation(env);
//        joinFuncation(env);
        corssFuncation(env);
    }

}
