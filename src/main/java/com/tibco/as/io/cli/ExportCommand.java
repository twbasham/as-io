package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.io.AbstractChannelTransfer;
import com.tibco.as.io.Channel;
import com.tibco.as.io.Destination;
import com.tibco.as.io.cli.converters.BrowserDistributionScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTimeScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTypeConverter;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;

public class ExportCommand implements ICommand {

	@ParametersDelegate
	private Transfer transfer = new Transfer();
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

	protected void configure(Destination destination) {
		destination.setSpaceLimit(transfer.getLimit());
		destination.setExportWorkerCount(transfer.getWorkerCount());
		destination.setBrowserType(browserType);
		destination.setTimeScope(timeScope);
		destination.setDistributionScope(distributionScope);
		destination.setTimeout(timeout);
		destination.setPrefetch(prefetch);
		destination.setQueryLimit(queryLimit);
		destination.setFilter(filter);
	}

	@Override
	public AbstractChannelTransfer getTransfer(Channel channel)
			throws Exception {
		for (String spaceName : spaceNames) {
			channel.addDestination().setSpace(spaceName);
		}
		configure(channel.getDefaultDestination());
		return channel.getExport();
	}
}