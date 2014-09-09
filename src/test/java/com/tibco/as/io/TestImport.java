package com.tibco.as.io;

public class TestImport extends AbstractImport {

	@Override
	public TestImport clone() {
		TestImport config = new TestImport();
		copyTo(config);
		return config;
	}

}
