package com.finnxu.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 19:16
 * Description : TODO
 */
public class JavaDataSetSink {
    public static void sinkTest(ExecutionEnvironment env) {
        List<Integer> list = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);
        String path = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/output/";
        data.writeAsText(path, FileSystem.WriteMode.OVERWRITE).setParallelism(3);
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        sinkTest(env);
        env.execute("JavaDataSetSink");

    }
}
