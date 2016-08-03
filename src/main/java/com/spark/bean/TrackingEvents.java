package com.spark.bean;


/**
 * Copyright (c) 2013 StubHub Inc. All rights reserved.
 */

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;

import org.apache.avro.reflect.Nullable;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import shaded.parquet.org.codehaus.jackson.map.annotate.JsonRootName;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "trackingEvents")
@JsonRootName(value = "trackingEvents")
@XmlType(name = "", propOrder = { "account", "events", "context" })
@JsonIgnoreProperties(ignoreUnknown = true)
public class TrackingEvents implements Serializable {
	private static final long serialVersionUID = 1L;

	@XmlElement(name = "account", required = true)
    private String account;
    
    @XmlElement(name = "events", required = true)
    private List<TrackingEvent> events;

    @XmlElementWrapper(name = "context", required = false)
    @Nullable
    private HashMap<String, String> context = new HashMap<String, String>();

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public List<TrackingEvent> getEvents() {
        return events;
    }

    public void setEvents(List<TrackingEvent> eventList) {
        this.events =  eventList;
    }

    public HashMap<String, String> getContext() {
        return context;
    }

    public void setContext(HashMap<String, String> context) {
        this.context = context;
    }

    @Override
    public String toString() {
        return "TrackingEvents [account=" + account + ", events=" + events + ", context=" + context + "]";
    }
}
