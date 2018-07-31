package com.example.springboot.spark.springbootspark.config;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.Environment;


@Configuration
@PropertySource("classpath:application.properties")

public class ApplicationConfig {

    @Autowired
    private Environment env;

    @Value("${app.name:Springboot-Spark}")
    private String appName;

    @Value("${spark.home}")
    private String sparkHome;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
               // .setSparkHome(sparkHome)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        // System.setProperty("hadoop.home.dir", "F:\\res\\learn\\hadoop\\hadoop-2.7.6");
        SparkConf sconf=sparkConf();
        return new JavaSparkContext(sconf);
    }

    @Bean
    public SparkSession sparkSession() {
        System.out.println("==========================");
        JavaSparkContext jsc=javaSparkContext();
        SparkContext sc=jsc.sc();
        return SparkSession
                .builder()
                .sparkContext(sc)
                .appName("Java Spark SQL basic example")
                //.config("spark.sql.broadcastTimeout",3000)
                .getOrCreate();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

}