package com.tibco.as.io.cli.converters;

import com.tibco.as.space.browser.BrowserDef.DistributionScope;

public class BrowserDistributionScopeConverter extends
		AbstractEnumConverter<DistributionScope> {

	@Override
	protected DistributionScope valueOf(String name) {
		return DistributionScope.valueOf(name);
	}

	@Override
	protected DistributionScope[] getValues() {
		return DistributionScope.values();
	}

}
