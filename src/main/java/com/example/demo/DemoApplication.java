package com.example.demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.List;

@SpringBootApplication
public class DemoApplication implements CommandLineRunner {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        System.setProperty("hadoop.home.dir","E:\\Tools\\hadoop-common-2.2.0-bin-master\\bin");
        SparkConf conf = new SparkConf().setMaster("spark://172.24.62.135:7077").setAppName("HelloSpark");
        try (JavaSparkContext sc = new JavaSparkContext(conf)) {
            List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
            JavaRDD<Integer> distData = sc.parallelize(data).filter(p -> p > 3);
            distData.foreach(System.out::println);
        }
    }
}
