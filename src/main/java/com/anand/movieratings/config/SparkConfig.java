package com.anand.movieratings.config;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {
    @Bean
    SparkConf sparkConf() {
        final SparkConf conf = new SparkConf().setAppName("Test").setMaster("local[*]");
        conf.set("spark.driver.allowMultipleContexts", "true");
        return conf;
    }

}
