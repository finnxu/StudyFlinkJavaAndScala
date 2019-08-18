package com.finnxu.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 20:09
 * Description : 自定义Sink（socket发送数据过来，把string类型转成对象，然后把Java对象保存到MySQL数据库中）
 */
public class JavaCustomSinkToMySQL {

    private static void sinkToMySQL(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<Student> studentStream = source.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                String[] split = value.split(",");
                Student student = new Student();
                student.setId(Integer.parseInt(split[0]));
                student.setName(split[1]);
                student.setAge(Integer.parseInt(split[2]));
                return student;
            }
        });

        studentStream.addSink(new SinkToMySQL());

    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        sinkToMySQL(env);
        env.execute("JavaCustomSinkToMySQL");
    }

}
