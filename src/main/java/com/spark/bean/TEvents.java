package com.spark.bean;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TEvents implements Serializable {
	private static final long serialVersionUID = -6657777106736827608L;
	
	List<TEvent> tEvents = null;

	public TEvents() {
		tEvents = new ArrayList<TEvent>();
	}
	
	public void add(TEvent tEvent) {
		tEvents.add(tEvent);
	}
	
	public List<TEvent> gettEvents() {
		return tEvents;
	}

	public void settEvents(List<TEvent> tEvents) {
		this.tEvents = tEvents;
	}
	
}
