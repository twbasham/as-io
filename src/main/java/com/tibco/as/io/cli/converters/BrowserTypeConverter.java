package com.tibco.as.io.cli.converters;

import com.tibco.as.space.browser.BrowserDef.BrowserType;

public class BrowserTypeConverter extends AbstractEnumConverter<BrowserType> {

	@Override
	protected BrowserType valueOf(String name) {
		return BrowserType.valueOf(name);
	}

	@Override
	protected BrowserType[] getValues() {
		return BrowserType.values();
	}

}