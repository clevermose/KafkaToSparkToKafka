package com.spark.bean;


/**
 * Copyright (c) 2013 StubHub Inc. All rights reserved.
 */

import java.io.Serializable;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.avro.reflect.Nullable;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "event")
@XmlType(name = "", propOrder = { "name", "properties", "timestamp" })
@JsonIgnoreProperties(ignoreUnknown = true)
public class TrackingEvent implements Serializable {

	private static final long serialVersionUID = 1L;

	@XmlElement(name = "name", required = true)
    private String name;

    @XmlElementWrapper(name = "properties", required = false)
    @Nullable
    private HashMap<String, String> properties;

    @XmlElement(name = "timestamp", required = false)
    @Nullable
    private String timestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HashMap<String, String> getProperties() {
        return properties;
    }

    public void setProperties(HashMap<String, String> properties) {
        this.properties = properties;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "TrackingEvent [name=" + name + ", properties=" + properties + ", timestamp=" + timestamp + "]";
    }
} 