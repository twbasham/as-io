package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.Direction;
import com.tibco.as.io.cli.converters.BrowserDistributionScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTimeScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTypeConverter;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public abstract class AbstractExportCommand extends Command {

	@Parameter(description = "The list of spaces to export")
	private Collection<String> spaceNames = new ArrayList<String>();
	@Parameter(description = "Browser type", names = { "-browser_type" }, converter = BrowserTypeConverter.class, validateWith = BrowserTypeConverter.class)
	private BrowserType browserType;
	@Parameter(description = "Browser time scope", names = { "-time_scope" }, converter = BrowserTimeScopeConverter.class, validateWith = BrowserTimeScopeConverter.class)
	private TimeScope timeScope;
	@Parameter(description = "Browser distribution scope", names = { "-distribution_scope" }, converter = BrowserDistributionScopeConverter.class, validateWith = BrowserDistributionScopeConverter.class)
	private DistributionScope distributionScope;
	@Parameter(description = "Browser timeout", names = { "-timeout" })
	private Long timeout;
	@Parameter(description = "Browser prefetch", names = { "-prefetch" })
	private Long prefetch;
	@Parameter(description = "Browser query limit", names = { "-query_limit" })
	private Long queryLimit;
	@Parameter(description = "Browser filter", names = { "-filter" })
	private String filter;

	@Override
	public void configure(ChannelConfig config) throws Exception {
		for (String spaceName : spaceNames) {
			DestinationConfig destination = createDestinationConfig();
			destination.setSpace(spaceName);
			config.getDestinations().add(destination);
		}
		if (config.getDestinations().isEmpty()) {
			config.getDestinations().add(createDestinationConfig());
		}
		super.configure(config);
	}

	protected abstract DestinationConfig createDestinationConfig();

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.EXPORT);
		config.setBrowserType(browserType);
		config.setTimeScope(timeScope);
		config.setDistributionScope(distributionScope);
		config.setTimeout(timeout);
		config.setPrefetch(prefetch);
		config.setQueryLimit(queryLimit);
		config.setFilter(filter);
		super.configure(config);
	}

}