package com.spark.bean;

import java.io.Serializable;

public class ProductLocation implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private String product;
	private String location;
	
	public ProductLocation(String product, String location) {
		this.product = product;
		this.location = location;
	}
	
	public String getProduct() {
		return product;
	}
	public void setProduct(String product) {
		this.product = product;
	}
	public String getLocation() {
		return location;
	}
	public void setLocation(String location) {
		this.location = location;
	}
}
