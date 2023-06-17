package com.shence.shencetest;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Test001 {

    public static Logger logger = Logger.getLogger("com.shence.shencetest.HiveConnection");

    public static void main(String[] args) throws Exception {
        System.out.println("11111111111111");
        System.out.println("22222222222222");
        System.out.println("33333333333333");

//        HiveConnection hiveConnection = new HiveConnection();
//        hiveConnection.showtDb();

    }


    public void test(){
        String connectionURL = "jdbc:hive2://172.31.28.234:21050/rawdata;auth=noSasl";
        String drivername = "org.apache.hive.jdbc.HiveDriver";
        String username = "";
        String password = "";
        try {
            Class.forName(drivername);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        try {
            Connection con = DriverManager.getConnection(connectionURL, username, password);
            if (con != null) {
                logger.log(Level.INFO,"Connected");
            } else {
                logger.log(Level.INFO,"Not Connected");
            }


            logger.log(Level.INFO,"执行sql前。。。。。。。。。。。。。。。。");

            Statement stmt = con.createStatement();
            String sql;
            ResultSet res;
            sql = "SELECT user_id,distinct_id,event,time,$lib as lib  FROM events WHERE `date` = CURRENT_DATE() LIMIT 10 /*SA(default)*/";

            logger.log(Level.INFO,"Running: " + sql);

            res = stmt.executeQuery(sql);

            logger.log(Level.INFO,"执行sql后。。。。。。。。。。。。。。。。");

            String resStr = "";
            while (res.next()) {


                resStr += "【" + String.valueOf(res.getDouble(1)) + "】" +
                          "【" + String.valueOf(res.getString(2)) + "】" +
                          "【" + String.valueOf(res.getString(3)) + "】" +
                          "【" + String.valueOf(res.getTimestamp(4)) + "】" +
                           "\n";
                
            }

            logger.log(Level.INFO,resStr);

        } catch (SQLException se) {
            se.printStackTrace();
        }
    }




}
