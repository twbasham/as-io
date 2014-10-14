package com.tibco.as.io;

import com.tibco.as.convert.Field;
import com.tibco.as.convert.Space;

public class TestFieldConfig extends Field {

	public TestFieldConfig(Space space) {
		super(space);
	}

	@Override
	public Class<?> getJavaType() {
		return String.class;
	}

}
