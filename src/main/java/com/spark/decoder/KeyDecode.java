package com.spark.decoder;

import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

/**
 * kafka key decode
 * @author haiswang
 *
 */
public class KeyDecode implements Decoder<String> {
	
	public KeyDecode(VerifiableProperties verifiableProperties){  
		  
    }  
	
	public String fromBytes(byte[] bytes) {
		return new String(bytes);
	}
}