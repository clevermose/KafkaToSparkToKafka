package com.spark.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.spark.streaming.SparkStreaming;

public class RunMain {

	
	public static void main(String[] args) {
	
		SparkConf sparkConfig = new SparkConf();
		sparkConfig.setAppName("TopGunStreaming").setMaster("local[20]");
		
		JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConfig);
		SQLContext sqlContext = new SQLContext(javaSparkContext);
		
		Configuration config = new Configuration();
		FileSystem fileSystem = null;
		try {
			fileSystem = FileSystem.get(config);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		
		String groupId = "haiswang-1";
		
		SparkStreaming streaming = new SparkStreaming(javaSparkContext, sqlContext, fileSystem, groupId);
		streaming.startStreaming();
	}
}
