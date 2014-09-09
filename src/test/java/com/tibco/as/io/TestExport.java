package com.tibco.as.io;

public class TestExport extends AbstractExport {

	@Override
	public TestExport clone() {
		TestExport export = new TestExport();
		copyTo(export);
		return export;
	}

}
