package com.tibco.as.io;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class TestChannel extends Channel {

	private Map<String, List<Object>> lists = new HashMap<String, List<Object>>();

	public synchronized List<Object> getList(String name) {
		if (!lists.containsKey(name)) {
			lists.put(name, new Vector<Object>());
		}
		return lists.get(name);
	}

}
