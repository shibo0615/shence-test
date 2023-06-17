package com.shence.shencetest;

import parquet.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HiveConnection {

    private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static final String url = "jdbc:hive2://172.31.28.234:21050/rawdata;auth=noSasl";
    private static final String dbName = "";
    private static Connection con = null;
    private static Statement state = null;
    private static ResultSet res = null;

    Logger logger = Logger.getLogger("com.shence.shencetest.HiveConnection");


    /**
     * 释放资源
     */
    public void destory() throws SQLException {
        if (res != null) state.close();
        if (state != null) state.close();
        if (con != null) con.close();
    }

    /**
     * 查询所有数据库
     */
    public void showtDb() throws SQLException, ClassNotFoundException {
        //System.out.println("开始。。。。。。。。。。。。。。。。");
        logger.log(Level.INFO,"开始。。。。。。。。。。。。。。。。");

        Class.forName(driverName);
        con = DriverManager.getConnection(url);
        state = con.createStatement();

        logger.log(Level.INFO,"执行sql前。。。。。。。。。。。。。。。。");

        // 这句可以执行   看 events 表有哪些字段
        //res = state.executeQuery("DESC events /*SA*/");


        res = state.executeQuery("SELECT user_id,distinct_id,event FROM events LIMIT 10 /*SA*/");


        // System.out.println("执行。。。。。。。。。。。。。。。。");
        logger.log(Level.INFO,"执行。。。。。。。。。。。。。。。。");

        while (res.next()) {
            System.out.println(res.getString(3));
            logger.log(Level.INFO,res.getString(3));
        }

        // System.out.println("结束。。。。。。。。。。。。。。。。");
        logger.log(Level.INFO,"结束。。。。。。。。。。。。。。。。");
    }

    /**
     * 创建数据库
     */
    public void createDb() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(url);
        state = con.createStatement();
        state.execute("create database db_test");
    }

    public static void init() {
        try {
            Class.forName(driverName);
            con = DriverManager.getConnection(url + "/" + dbName);
            state = con.createStatement();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

    }


    /**
     * 删除数据库
     * DROP DATABASE IF EXISTS DbName CASCADE;
     */
    public void dropDb() throws SQLException {
        init();
        state.execute("drop database if exists " + dbName + " CASCADE");
    }

    /*
     * 内部表基本操作 创建表
     * */
    public void createTab() throws SQLException {
        init();
        state.execute("create table if not exists student ( " +
                "name string , " +
                "age int , " +
                "agent string  ," +
                "adress struct<street:STRING,city:STRING>) " +
                "row format delimited " +
                "fields terminated by ',' " +//字段与字段之间的分隔符
                "collection items terminated by ':'" +//一个字段各个item的分隔符
                "lines terminated by '\n' ");//行分隔符
    }
    /**
     * 查询所有表
     */
    public void showTab() throws SQLException {
        init();
        res = state.executeQuery("show tables");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
    /**
     * 查看表结构
     */
    public void descTab() throws SQLException {
        res = state.executeQuery("desc student");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }
    /**
     * 加载数据
     */
    public void loadData() throws SQLException {
        init();
        state.execute("load data local inpath '/root/studentData.txt' overwrite into table student");
    }
    /**
     * 查询数据
     */
    public void selectTab() throws SQLException {
        init();
        res = state.executeQuery("select * from student");
        while (res.next()) {
            System.out.println(res.getString(1) + "-" + res.getString(2) + "-" + res.getString(3) + "-" + res.getString(4));
        }
    }
    /**
     * 统计查询（会运行mapreduce作业，资源开销较大）
     */
    public void countData() throws SQLException {
        init();
        res = state.executeQuery("select count(1) from student");
        while (res.next()) {
            System.out.println(res.getInt(1));
        }
    }
    /**
     * 删除表
     */
    public void dropTab() throws SQLException {
        init();
        state.execute("drop table student");
    }

    /**
     * 创建外部表外部表基本操作
     * 外部表删除后，hdfs文件系统上的数据还在，重新创建同路径外部表后，其数据仍然存在
     * 不指定路径时默认使用hive.metastore.warehouse.dir指定的路径
     */
    public void createExTab() throws SQLException {
        init();
        state.execute("create external table if not exists student_ext ( " +
                "name string , " +
                "age int , " +
                "agent string  ," +
                "adress struct<street:STRING,city:STRING>) " +
                "row format delimited " +
                "fields terminated by ',' " +
                "collection items terminated by ':'" +
                "lines terminated by '\n' " +
                "stored as textfile " +
                "location '/testData/hive/student1' ");
    }
    /**
     * 从一张已经存在的表上复制其表结构，并不会复制其数据
     * 创建表，携带数据 create table student1 as select * from student
     * 创建表，携带表结构 create table student1 like student
     */
    public void copyExTab() throws SQLException {
        init();
        state.execute("create external table if not exists student2 like " +
                "student location '/testData/hive/student1'");
    }
    /*
     *创建分区格式表, 必须在表定义时创建partition
     * */
    public void creatPartab() throws SQLException {
        init();
        state.execute("create table if not exists emp (" +
                "name string ," +
                "salary int ," +
                "subordinate array<string> ," +
                "deductions map<string,float> ," +
                "address struct<street:string,city:string>) " +
                "partitioned by (city string,street string) " +
                "row format delimited " +
                "fields terminated by '\t' " +
                "collection items terminated by ',' " +
                "map keys terminated by ':' " +
                "lines terminated by '\n' " +
                "stored as textfile");
    }


    /**
     * 添加分区表
     */
    public void addPartition() throws SQLException {
        init();
        state.execute("alter table emp add partition(city='shanghai',street='jinkelu') ");
    }
    /**
     * 查看分区表信息 city=shanghai/street=jinkelu
     */
    public void showPartition() throws SQLException {
        init();
        res = state.executeQuery("show partitions emp");
        while (res.next()) {
            System.out.println(res.getString(1));
        }
    }
    /**
     * 插入数据
     */
    public void loadParData() throws SQLException {
        init();
        String filepath = " '/root/emp.txt' ";
        state.execute("load data local inpath " + filepath + " overwrite into table emp partition (city='shanghai',street='jinkelu')");
    }
    /**
     * 删除分区表
     */
    public void dropPartition() throws SQLException {
        init();
        state.execute("alter table employees drop partition (city='shanghai',street='jinkelu') ");
        /*
        *
        * 1，把一个分区打包成一个har包
             alter table emp archive partition (city='shanghai',street='jinkelu')
          2, 把一个分区har包还原成原来的分区
    `        alter table emp unarchive partition (city='shanghai',street='jinkelu')
          3, 保护分区防止被删除
             alter table emp partition (city='shanghai',street='jinkelu') enable no_drop
          4,保护分区防止被查询
             alter table emp partition (city='shanghai',street='jinkelu') enable offline
          5，允许分区删除和查询
             alter table emp partition (city='shanghai',street='jinkelu') disable no_drop
             alter table emp partition (city='shanghai',street='jinkelu') disable offline
        * */
    }
    /*
    动态分区
    当需要一次插入多个分区的数据时，可以使用动态分区，根据查询得到的数据动态分配到分区里。
     动态分区与静态分区的区别就是不指定分区目录，由hive根据实际的数据选择插入到哪一个分区。
    set hive.exec.dynamic.partition=true; 启动动态分区功能
    set hive.exec.dynamic.partition.mode=nonstrict   分区模式，默认nostrict
    set hive.exec.max.dynamic.partitions=1000       最大动态分区数,默认1000
    */
    /**
     * 创建分区格式表
     */
    public void creatPartab1() throws SQLException {
        init();
        state.execute("create table if not exists emp1 (" +
                "name string ," +
                "salary int ," +
                "subordinate array<string> ," +
                "deductions map<string,float> ," +
                "address struct<street:string,city:string>) " +
                "partitioned by (city string,street string) " +
                "row format delimited " +
                "fields terminated by '\t' " +
                "collection items terminated by ',' " +
                "map keys terminated by ':' " +
                "lines terminated by '\n' " +
                "stored as textfile");
    }
    /**
     * 靠查询到的数据来分区
     */
    public void loadPartitionData() throws SQLException {
        init();
        state.execute("insert overwrite table emp1 partition (city='shanghai',street) " +
                "select name,salary,subordinate,deductions,address,address.street from emp");
    }


}
