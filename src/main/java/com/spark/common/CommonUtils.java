package com.spark.common;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class CommonUtils implements Serializable {
	private static final long serialVersionUID = 1L;

	public static String getCurrDate() {
		String format = "yyyyMMdd";
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(new Date());
	}
	
	public static boolean isEmpty(String input) {
		return null==input || "".equals(input) ? true : false;
	}
	
	public static String getHost(String referer) {
		return "";
	}
	
	public static String getGenreIdDetail(String referer) {
		return "";
	}
	
	public static long formatTime(String time) {
		return System.currentTimeMillis();
	}
}
