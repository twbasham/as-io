package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.util.BrowserConfig;

public class ExportConfig extends TransferConfig {

	private Collection<String> fieldNames = new ArrayList<String>();
	private BrowserConfig browserConfig = new BrowserConfig();

	public void copyTo(ExportConfig target) {
		target.fieldNames = new ArrayList<String>(fieldNames);
		browserConfig.copyTo(target.browserConfig);
		super.copyTo(target);
	}

	public BrowserConfig getBrowserConfig() {
		return browserConfig;
	}

	public void setBrowserConfig(BrowserConfig browserConfig) {
		this.browserConfig = browserConfig;
	}

}
