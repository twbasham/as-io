package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelTransfer;
import com.tibco.as.io.Destination;
import com.tibco.as.io.DestinationExport;
import com.tibco.as.io.ExportConfig;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IDestinationTransfer;
import com.tibco.as.io.TransferConfig;
import com.tibco.as.io.cli.converters.BrowserDistributionScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTimeScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTypeConverter;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;
import com.tibco.as.util.BrowserConfig;
import com.tibco.as.util.Utils;

public abstract class ExportCommand extends AbstractCommand {

	@Parameter(description = "The list of spaces to export")
	private List<String> spaceNames = new ArrayList<String>();
	@Parameter(names = "-browser_type", description = "Browser type", converter = BrowserTypeConverter.class, validateWith = BrowserTypeConverter.class)
	private BrowserType browserType;
	@Parameter(names = "-time_scope", description = "Browser time scope", converter = BrowserTimeScopeConverter.class, validateWith = BrowserTimeScopeConverter.class)
	private TimeScope timeScope;
	@Parameter(names = "-distribution_scope", description = "Browser distribution scope", converter = BrowserDistributionScopeConverter.class, validateWith = BrowserDistributionScopeConverter.class)
	private DistributionScope distributionScope;
	@Parameter(names = "-timeout", description = "Browser timeout")
	private Long timeout;
	@Parameter(names = "-prefetch", description = "Browser prefetch")
	private Long prefetch;
	@Parameter(names = "-query_limit", description = "Browser query limit")
	private Long queryLimit;
	@Parameter(names = "-filter", description = "Browser filter")
	private String filter;

	@Override
	public ChannelTransfer getTransfer(IChannel channel) throws Exception {
		for (String pattern : spaceNames) {
			for (String spaceName : channel.getMetaspace().getUserSpaceNames()) {
				if (Utils.matches(spaceName, pattern, false)) {
					Destination destination = createDestination(channel);
					destination.getSpaceDef().setName(spaceName);
					channel.getDestinations().add(destination);
				}
			}
		}
		return super.getTransfer(channel);
	}

	@Override
	protected void configure(TransferConfig config) {
		super.configure(config);
		ExportConfig exportConfig = (ExportConfig) config;
		BrowserConfig browserConfig = exportConfig.getBrowserConfig();
		if (browserType != null) {
			browserConfig.setBrowserType(browserType);
		}
		if (timeScope != null) {
			browserConfig.setTimeScope(timeScope);
		}
		if (distributionScope != null) {
			browserConfig.setDistributionScope(distributionScope);
		}
		if (timeout != null) {
			browserConfig.setTimeout(timeout);
		}
		if (prefetch != null) {
			browserConfig.setPrefetch(prefetch);
		}
		if (queryLimit != null) {
			browserConfig.setQueryLimit(queryLimit);
		}
		if (filter != null) {
			browserConfig.setFilter(filter);
		}

	}

	@Override
	protected IDestinationTransfer getTransfer(IDestination destination) {
		return new DestinationExport(destination);
	}

}