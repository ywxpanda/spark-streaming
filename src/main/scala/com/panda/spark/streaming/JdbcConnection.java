package com.panda.spark.streaming;

import com.alibaba.druid.pool.DruidDataSource;

import java.sql.Connection;

public class JdbcConnection {

    private JdbcConnection() {

    }

    public static Connection getConn() {
        Connection conn = null;
        try {
            conn = Singleton.INSTANCE.getInstance();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    public static Integer add(){
        return Singleton.INSTANCE.add();
    }
    enum Singleton {
        INSTANCE;
        private DruidDataSource dataSource;
        private Integer i;

        Singleton() {
            dataSource = new DruidDataSource();
            dataSource.setDriverClassName("com.mysql.jdbc.Driver");
            dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/spark");
            dataSource.setUsername("root");
            dataSource.setPassword("root");
            dataSource.setMaxActive(5);
            i = 0;
        }

        public Integer add(){
            i++;
            return i;
        }
        public Connection getInstance() throws Exception {
            return dataSource.getConnection();
        }

    }
}
