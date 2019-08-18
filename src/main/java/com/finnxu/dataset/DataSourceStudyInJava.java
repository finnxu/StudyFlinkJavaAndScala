package com.finnxu.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * Created with IDEA
 * Author: finnxu
 * Date:2019-08-14 22:13
 * Description: TODO
 */
public class DataSourceStudyInJava {
    public static void fromCsvFile(ExecutionEnvironment env) throws Exception {
        String path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.csv";
        System.out.println("-----------直接读取---");
        env.readCsvFile(path).ignoreFirstLine().types(String.class, Integer.class, String.class).print();
    }

    public static void fromTextFile(ExecutionEnvironment env) throws Exception {
        String path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.txt";
        env.readTextFile(path).print();
    }

    public static void fromCollections(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        env.fromElements(list).print();
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        fromCollections(env);
//        fromTextFile(env);
        fromCsvFile(env);
    }
}
