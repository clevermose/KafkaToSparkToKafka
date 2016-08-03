package com.spark.decoder;

import java.io.Serializable;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

import com.alibaba.fastjson.JSON;
import com.spark.bean.TrackingEvents;

/**
 * Kafka value decoder
 * @author haiswang
 *
 */
public class TrackingEventsDecoder implements Decoder<TrackingEvents> ,Serializable {
	
	private static final long serialVersionUID = 8042565531457633889L;
	
	public TrackingEvents fromBytes(byte[] bytes) {
		return JSON.parseObject(new String(bytes), TrackingEvents.class);
	}
	
    public TrackingEventsDecoder(VerifiableProperties verifiableProperties){  
  
    }  
}
