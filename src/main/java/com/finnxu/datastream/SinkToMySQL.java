package com.finnxu.datastream;


import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * PackageName : com.finnxu.datastream
 * ProjectName : StudyFlinkJavaAndScala
 * Author : finnxu
 * Date : 2019-08-18 20:27
 * Description : 自定义Sink RichSinkFunction<T> T就是自定义类型
 */

public class SinkToMySQL extends RichSinkFunction<Student> {
    private static final Logger logger = LoggerFactory.getLogger(SinkToMySQL.class);
    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = getConnection();
        String sql = "insert into student(id, name,age) values(?, ?,?);";
        ps = this.connection.prepareStatement(sql);
    }


    @Override
    public void close() throws Exception {
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        //组装数据，执行插入操作
        ps.setInt(1, value.getId());
        ps.setString(2, value.getName());
        ps.setInt(3, value.getAge());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            String driver = "com.mysql.jdbc.Driver";
            Class.forName(driver);
            String url = "jdbc:mysql://localhost:3306/flink?&useSSL=false&serverTimezone=UTC";
            String username = "root";
            String passworld = "root";
            con = DriverManager.getConnection(url, username, passworld);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return con;
    }
}
