package com.shence.shencetest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ShenceTestApplication {

    public static void main(String[] args) {
        SpringApplication.run(ShenceTestApplication.class, args);

        try{
//            HiveConnection hiveConnection = new HiveConnection();
//            hiveConnection.showtDb();

            Test001 test001 = new Test001();
            test001.test();


        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
