package com.spark.bean;

import java.io.Serializable;

import com.alibaba.fastjson.JSON;

public class TEvent implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -1989578200438509357L;
	private String userAgent;
	private String ipaddress;
	private long serverTimestamp;
	private String pageName;
	private String userId;
	private String visitorId;
	private String siteSections;
	private String coreProductView; 
	private String category;
	private String subCategory;
	private String genre;
	private String genreId;
	private String products;
	private String host;
	private String genreIdDetail;
	private String referer;
	
	public String getUserAgent() {
		return userAgent;
	}
	public void setUserAgent(String userAgent) {
		this.userAgent = userAgent;
	}
	public String getIpaddress() {
		return ipaddress;
	}
	public void setIpaddress(String ipaddress) {
		this.ipaddress = ipaddress;
	}
	public long getServerTimestamp() {
		return serverTimestamp;
	}
	public void setServerTimestamp(long serverTimestamp) {
		this.serverTimestamp = serverTimestamp;
	}
	public String getPageName() {
		return pageName;
	}
	public void setPageName(String pageName) {
		this.pageName = pageName;
	}
	public String getUserId() {
		return userId;
	}
	public void setUserId(String userId) {
		this.userId = userId;
	}
	public String getVisitorId() {
		return visitorId;
	}
	public void setVisitorId(String visitorId) {
		this.visitorId = visitorId;
	}
	public String getSiteSections() {
		return siteSections;
	}
	public void setSiteSections(String siteSections) {
		this.siteSections = siteSections;
	}
	public String getCoreProductView() {
		return coreProductView;
	}
	public void setCoreProductView(String coreProductView) {
		this.coreProductView = coreProductView;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	public String getSubCategory() {
		return subCategory;
	}
	public void setSubCategory(String subCategory) {
		this.subCategory = subCategory;
	}
	public String getGenre() {
		return genre;
	}
	public void setGenre(String genre) {
		this.genre = genre;
	}
	public String getGenreId() {
		return genreId;
	}
	public void setGenreId(String genreId) {
		this.genreId = genreId;
	}
	public String getProducts() {
		return products;
	}
	public void setProducts(String products) {
		this.products = products;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public String getGenreIdDetail() {
		return genreIdDetail;
	}
	public void setGenreIdDetail(String genreIdDetail) {
		this.genreIdDetail = genreIdDetail;
	}
	public String getReferer() {
		return referer;
	}
	public void setReferer(String referer) {
		this.referer = referer;
	}
	
	@Override
	public String toString() {
		return JSON.toJSONString(this);
	}
}
