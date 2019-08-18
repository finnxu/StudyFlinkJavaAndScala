package com.finnxu.tablesql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * PackageName : com.finnxu.tablesql
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-19 00:20
 * Description : TODO
 */
public class JavaTableSQLApi {
    private static void tableSQLApi(ExecutionEnvironment env, BatchTableEnvironment table) throws Exception {
        String filePath = "/media/alvinxu/学习工作/flink/Project/StudyFlinkJavaAndScala/src/main/resources/worldCount.csv";
        DataSource<Person> data = env.readCsvFile(filePath).ignoreFirstLine()
                .pojoType(Person.class, "name", "age", "sex", "address1", "address2");
        Table tableInfo = table.fromDataSet(data);
        table.registerTable("person", tableInfo);
        Table result = table.sqlQuery("select * from person where age >= 20");
        table.toDataSet(result, Row.class).print();
    }

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment table = BatchTableEnvironment.getTableEnvironment(env);
        tableSQLApi(env, table);
    }

    public static class Person {
        public String name;
        public int age;
        public String sex;
        public String address1;
        public String address2;

        @Override
        public String toString() {
            return "Person{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", sex='" + sex + '\'' +
                    ", address1='" + address1 + '\'' +
                    ", address2='" + address2 + '\'' +
                    '}';
        }
    }
}
