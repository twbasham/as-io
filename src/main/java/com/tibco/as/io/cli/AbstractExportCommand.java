package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.tibco.as.convert.Direction;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.cli.converters.BrowserDistributionScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTimeScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTypeConverter;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public abstract class AbstractExportCommand extends AbstractCommand {

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
		List<DestinationConfig> destinations = config.getDestinations();
		for (String spaceName : spaceNames) {
			DestinationConfig destination = createDestinationConfig();
			destination.setSpace(spaceName);
			destinations.add(destination);
		}
		if (destinations.isEmpty()) {
			destinations.add(createDestinationConfig());
		}
		super.configure(config);
	}

	protected abstract DestinationConfig createDestinationConfig();

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.EXPORT);
		if (browserType != null) {
			config.setBrowserType(browserType);
		}
		if (timeScope != null) {
			config.setTimeScope(timeScope);
		}
		if (distributionScope != null) {
			config.setDistributionScope(distributionScope);
		}
		if (timeout != null) {
			config.setTimeout(timeout);
		}
		if (prefetch != null) {
			config.setPrefetch(prefetch);
		}
		if (queryLimit != null) {
			config.setQueryLimit(queryLimit);
		}
		if (filter != null) {
			config.setFilter(filter);
		}
		super.configure(config);
	}

}