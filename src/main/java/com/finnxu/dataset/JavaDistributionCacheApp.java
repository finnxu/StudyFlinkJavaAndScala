package com.finnxu.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 22:27
 * Description : TODO
 */
public class JavaDistributionCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("Hadoop", "Scala", "Spark", "Flink", "Sotrm", "Hive");
        String filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.txt";
        env.registerCachedFile(filePath, "local-cache");

        data.map(new RichMapFunction<String, String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("local-cache");
                List<String> lines = FileUtils.readLines(file);
                lines.forEach(System.out::println);
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
    }
}
