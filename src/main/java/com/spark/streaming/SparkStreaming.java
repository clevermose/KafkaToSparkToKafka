package com.spark.streaming;


import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import scala.Tuple2;

import com.alibaba.fastjson.JSONObject;
import com.spark.bean.ProductLocation;
import com.spark.bean.TEvent;
import com.spark.bean.TEvents;
import com.spark.bean.TrackingEvent;
import com.spark.bean.TrackingEvents;
import com.spark.common.CommonUtils;
import com.spark.common.Constants;
import com.spark.decoder.KeyDecode;
import com.spark.decoder.TrackingEventsDecoder;

/**
 * 
 * @author haiswang
 *
 */
public class SparkStreaming implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static JavaSparkContext javaSparkContext = null;
	private static SQLContext sqlContext = null;
	private static FileSystem fileSystem = null;
	
	private Duration duration = new Duration(Constants.SPARK_STREAMING_DURATION);
	private String groupId = null;
	
	/**
	 * 
	 * @param javaSparkContext
	 * @param sqlContext
	 * @param groupId
	 */
	public SparkStreaming(JavaSparkContext javaSparkContext, SQLContext sqlContext, FileSystem fileSystem, String groupId) {
		SparkStreaming.javaSparkContext = javaSparkContext;
		SparkStreaming.sqlContext = sqlContext;
		SparkStreaming.fileSystem = fileSystem;
		this.groupId = groupId;
	}
	
	/**
	 * start
	 */
	public void startStreaming() {
		
		JavaRDD<String> locations = javaSparkContext.textFile(Constants.HDFS_DATA_PATH + "location/location.nb");
		JavaRDD<ProductLocation> plRdd = locations.map(new Function<String, ProductLocation>() {
			private static final long serialVersionUID = 1L;
			public ProductLocation call(String line) throws Exception {
				String[] infos = line.split(",");
				return new ProductLocation(infos[0], infos[1]);
			}
		});
		
		//product - location
		DataFrame plDataFrame = sqlContext.createDataFrame(plRdd, ProductLocation.class);
		plDataFrame.registerTempTable(Constants.MT_TABLE_PRODUCTLOCATION);
		
		JavaStreamingContext javaStreamingContext = new JavaStreamingContext(javaSparkContext, duration);
		
		Map<String, String> params = new HashMap<String, String>();
		params.put("auto.offset.reset", "largest"); //smallest,largest
		params.put("zookeeper.connect", Constants.ZK_BROKERS);
		params.put("group.id", groupId);
		
		HashMap<String, Integer> topics = new HashMap<String, Integer>();
		topics.put(Constants.CONSUMER_TOPIC, Constants.CONSUMER_TOPIC_PARTITION_COUNT);
		
		JavaPairInputDStream<String, TrackingEvents> pairInputDStream = KafkaUtils.createStream(javaStreamingContext, String.class, TrackingEvents.class, KeyDecode.class, TrackingEventsDecoder.class, params, topics, StorageLevel.MEMORY_AND_DISK());
		
		JavaPairDStream<String, TrackingEvents> javaPairDStream = pairInputDStream.window(new Duration(60 * 60 * 1000l));
		
		JavaDStream<TEvents> tEvents = toTEvent(javaPairDStream);
		
		tEvents.foreachRDD(new VoidFunction<JavaRDD<TEvents>>() {
			private static final long serialVersionUID = 1L;
			public void call(JavaRDD<TEvents> tEvents) throws Exception {
				JavaRDD<TEvent> tEvent = tEvents.flatMap(new FlatMapFunction<TEvents, TEvent>() {
					private static final long serialVersionUID = 1L;
					public Iterable<TEvent> call(TEvents t) throws Exception {
						return t.gettEvents();
					}
				}).filter(new Function<TEvent, Boolean>() {
					private static final long serialVersionUID = 1L;
					public Boolean call(TEvent v1) throws Exception {
						return null!=v1 && "1".equals(v1.getCoreProductView());
					}
				});
				
				long tEventCount = tEvent.count();
				if(0!=tEventCount) {
					System.out.println("statistics info...");
					//statistics info
					loadStatisticsInfo(tEvent);
					//System.out.println("full data info...");
					//all data to hdfs
					//writeToHdfs(tEvent);
				} else {
					System.out.println("has no data...");
				}
			}
		});
		
		javaStreamingContext.start();
		javaStreamingContext.awaitTermination();
	}
	
	/**
	 * 
	 * @param rddList
	 */
	private void loadStatisticsInfo(JavaRDD<TEvent> javaRDD) {
		DataFrame incrementDataFrame = sqlContext.createDataFrame(javaRDD, TEvent.class);
		incrementDataFrame.registerTempTable(Constants.MT_TABLE_INCREMENT);
		DataFrame resultDataFrame = sqlContext.sql(Constants.HQL_STATICTIS_WITH_LOCATION);
		List<Row> rows = resultDataFrame.collectAsList();
		sendKafka(rows);
	}
	
	/**
	 * send statistics info to kafka
	 * @param rows
	 */
	private void sendKafka(final List<Row> rows) {
		ProducerConfig config = getProducerConfig();
		Producer<String, String> producer = new Producer<String, String>(config);
		
		List<KeyedMessage<String, String>> messages = new LinkedList<KeyedMessage<String,String>>();
		for (Row row : rows) {
			String message = getMessage(row);
			if(CommonUtils.isEmpty(message)) {
				System.out.println(row.getString(2) + " ,error data, has no location, skip.");
				continue;
			}
			System.out.println("send kafka - " + message);
			KeyedMessage<String, String> kafkaMessage = new KeyedMessage<String, String>(Constants.PRODUCER_TOPIC, message);
			messages.add(kafkaMessage);
		}
		
		producer.send(messages);
		producer.close();
	}
	
	/**
	 * 
	 * @param row
	 * @return
	 */
	private String getMessage(final Row row) {
		String genreId = row.getString(0);
		String genre = row.getString(1);
		String products = row.getString(2);
		long count = row.getLong(3);
		String location = row.getString(4);
		if(CommonUtils.isEmpty(location)) {
			return "";
		}
		
		JSONObject json = new JSONObject();
		json.put("genreId", genreId);
		json.put("genre", genre);
		json.put("products", products);
		json.put("count", count+"");
		json.put("location", location);
		
		return json.toJSONString();
	}
	
	/**
	 * kafka producer config
	 * @return
	 */
	private ProducerConfig getProducerConfig() {
		Properties props = new Properties();
		props.put("metadata.broker.list", Constants.KAFKA_BROKERS);
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		return new ProducerConfig(props);
	}
	
	/**
	 * write full data to hdfs
	 * @param rddList
	 */
	private void writeToHdfs(JavaRDD<TEvent> javaRDD) {
		
		Path writeFilePath = getWriteFilePath();
		FSDataOutputStream dos = null;
		
		try {
			if(fileSystem.exists(writeFilePath)) {
				dos = fileSystem.append(writeFilePath);
			} else {
				dos = fileSystem.create(writeFilePath);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
			IOUtils.closeStream(fileSystem);
			return;
		} 
		
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(dos));
		
		List<TEvent> tEvents = javaRDD.collect();
		StringBuilder sb = new StringBuilder();
		for (TEvent tEvent : tEvents) {
			System.out.println("send hdfs - " + tEvent);
			sb.append(tEvent.toString()).append("\n");
		}
		
		try {
			br.write(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(dos);
		}
	}
	
	/**
	 * Get hdfs file Path
	 * @return
	 */
	private Path getWriteFilePath() {
		StringBuilder sb = new StringBuilder();
		sb.append(Constants.HDFS_DATA_PATH_FULL).append("person_").append(CommonUtils.getCurrDate()).append(".nb");
		return new Path(sb.toString());
	}
	
	private JavaDStream<TEvents> toTEvent(JavaPairDStream<String, TrackingEvents> messages) {
		JavaDStream<TEvents> values = messages.map(new Function<Tuple2<String,TrackingEvents>, TEvents>() {
			private static final long serialVersionUID = 1L;
			public TEvents call(Tuple2<String,TrackingEvents> v1) throws Exception {
				TrackingEvents events = v1._2;
				TEvents tEvents = new TEvents();
				TEvent event = null;
				Map<String, String> context = events.getContext();
				String userAgent = context.get("userAgent");
				String ipAddress = context.get("ipaddress");
				String serverTimestamp = context.get("serverTimestamp");
				String visitorId = context.get("visitorId");
				String userId = context.get("userId");
				String referer = context.get("referer");
				
				String host = CommonUtils.getHost(referer);
				String genreIdDetail = CommonUtils.getGenreIdDetail(referer);
				
				String pageName = null;
				String siteSections = null;
				String coreProductView = null; 
				String category = null;
				String subCategory = null;
				String genre = null;
				String genreId = null;
				String products = null;
				
				for (TrackingEvent trackingEvent : events.getEvents()) {
					Map<String, String> properties = trackingEvent.getProperties();
					event = new TEvent();
					pageName = properties.get("pageName");
					siteSections = properties.get("siteSections");
					coreProductView = properties.get("core:productView");
					category = properties.get("category");
					subCategory = properties.get("subCategory");
					genre = properties.get("genre");
					genreId = properties.get("genreId");
					products = properties.get("products");
					event.setUserAgent(userAgent);
					event.setIpaddress(ipAddress);
					event.setServerTimestamp(CommonUtils.formatTime(serverTimestamp));
					event.setVisitorId(visitorId);
					event.setUserId(userId);
					event.setReferer(referer);
					event.setHost(host);
					event.setGenreIdDetail(genreIdDetail);
					event.setPageName(pageName);
					event.setSiteSections(siteSections);
					event.setCoreProductView(coreProductView);
					event.setCategory(category);
					event.setSubCategory(subCategory);
					event.setGenre(genre);
					event.setGenreId(genreId);
					event.setProducts(products);
					tEvents.add(event);
				}
				return tEvents;
			}
		});
		
		return values;
	}
	
//	private JavaDStream<TEvents> toTEvent(JavaPairInputDStream<String, TrackingEvents> messages) {
//		
//		JavaDStream<TEvents> values = messages.map(new Function<Tuple2<String,TrackingEvents>, TEvents>() {
//			private static final long serialVersionUID = 1L;
//			public TEvents call(Tuple2<String,TrackingEvents> v1) throws Exception {
//				TrackingEvents events = v1._2;
//				TEvents tEvents = new TEvents();
//				TEvent event = null;
//				Map<String, String> context = events.getContext();
//				String userAgent = context.get("userAgent");
//				String ipAddress = context.get("ipaddress");
//				String serverTimestamp = context.get("serverTimestamp");
//				String visitorId = context.get("visitorId");
//				String userId = context.get("userId");
//				String referer = context.get("referer");
//				
//				String host = CommonUtils.getHost(referer);
//				String genreIdDetail = CommonUtils.getGenreIdDetail(referer);
//				
//				String pageName = null;
//				String siteSections = null;
//				String coreProductView = null; 
//				String category = null;
//				String subCategory = null;
//				String genre = null;
//				String genreId = null;
//				String products = null;
//				
//				for (TrackingEvent trackingEvent : events.getEvents()) {
//					Map<String, String> properties = trackingEvent.getProperties();
//					event = new TEvent();
//					pageName = properties.get("pageName");
//					siteSections = properties.get("siteSections");
//					coreProductView = properties.get("core:productView");
//					category = properties.get("category");
//					subCategory = properties.get("subCategory");
//					genre = properties.get("genre");
//					genreId = properties.get("genreId");
//					products = properties.get("products");
//					event.setUserAgent(userAgent);
//					event.setIpaddress(ipAddress);
//					event.setServerTimestamp(CommonUtils.formatTime(serverTimestamp));
//					event.setVisitorId(visitorId);
//					event.setUserId(userId);
//					event.setReferer(referer);
//					event.setHost(host);
//					event.setGenreIdDetail(genreIdDetail);
//					event.setPageName(pageName);
//					event.setSiteSections(siteSections);
//					event.setCoreProductView(coreProductView);
//					event.setCategory(category);
//					event.setSubCategory(subCategory);
//					event.setGenre(genre);
//					event.setGenreId(genreId);
//					event.setProducts(products);
//					tEvents.add(event);
//				}
//				return tEvents;
//			}
//		});
//		
//		return values;
//	}
}
