package com.finnxu.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-17 20:56
 * Description : TODO
 */
public class JavaCounerApp {

    private static void counterTest(ExecutionEnvironment env) throws Exception {
        DataSource<String> data = env.fromElements("Hadoop", "Scala", "Spark", "Flink", "Sotrm", "Hive");
        String filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/output/counter/";
        data.map(new RichMapFunction<String, String>() {
            IntCounter counter = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                getRuntimeContext().addAccumulator("counter-java", counter);
            }

            @Override
            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        }).setParallelism(2).writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);

        JobExecutionResult javaCounerApp = env.execute("JavaCounerApp");
        Integer num = javaCounerApp.getAccumulatorResult("counter-java");
        System.out.println("num : " + num);
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        counterTest(env);
    }
}
