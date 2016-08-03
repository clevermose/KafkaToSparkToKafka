package com.spark.common;

import java.io.Serializable;

public class Constants implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final String CONSUMER_TOPIC = "tracking_events";
	public static final int CONSUMER_TOPIC_PARTITION_COUNT = 8; 
	public static final String PRODUCER_TOPIC = "tracking_events_location";
	
	public static final String MT_TABLE_PRODUCTLOCATION = "MT_TABLE_PRODUCTLOCATION";
	public static final String MT_TABLE_INCREMENT = "MT_EVENT_INCREMENT";
	public static final String HQL_STATICTIS = "select genreId,genre,count(*) cnt from " + MT_TABLE_INCREMENT + " group by genre,genreId";
	public static final String HQL_STATICTIS_WITH_LOCATION = "select a.*,b.location from (select genreId,genre,products,count(*) cnt from "  +
														      MT_TABLE_INCREMENT + " group by genre,genreId,products) a left join " + MT_TABLE_PRODUCTLOCATION + 
														      " b on a.products=b.product";
	
	public static final long SPARK_STREAMING_DURATION = 10000l;
	
	public static final String ZK_BROKERS = "chao-zk1-3546.slc01.dev.ebayc3.com:2181,chao-zk2-6549.slc01.dev.ebayc3.com:2181,chao-zk3-3574.slc01.dev.ebayc3.com:2181/kafka";
	public static final String KAFKA_BROKERS = "chao-kafka-9-7554.slc01.dev.ebayc3.com:9092,chao-kafka-9-2-3400.slc01.dev.ebayc3.com:9092,chao-kafka-9-3-4401.slc01.dev.ebayc3.com:9092";
	
	public static final String HDFS_DATA_PATH = "hdfs://10.249.73.142:9000/";
	public static final String HDFS_DATA_PATH_FULL = HDFS_DATA_PATH + "/person/full/";
	public static final String HDFS_DATA_PATH_INCREMENT = HDFS_DATA_PATH + "/person/increment/";
}
