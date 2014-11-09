package com.tibco.as.io.cli;

import java.util.ArrayList;
import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.io.ChannelExport;
import com.tibco.as.io.ExportConfig;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.cli.converters.BrowserDistributionScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTimeScopeConverter;
import com.tibco.as.io.cli.converters.BrowserTypeConverter;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;
import com.tibco.as.util.BrowserConfig;

public class ExportCommand extends AbstractCommand {

	@ParametersDelegate
	private Transfer transfer = new Transfer();
	@Parameter(description = "The list of spaces to export")
	private List<String> spaceNames = new ArrayList<String>();
	@Parameter(names = "-fields", description = "Names of specific fields to export, e.g. field1 field2", variableArity = true)
	private List<String> fieldNames;
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
	protected void configure(IDestination destination) {
		ExportConfig exportConfig = destination.getExportConfig();
		if (fieldNames != null) {
			destination.setFieldNames(fieldNames);
		}
		transfer.configure(exportConfig);
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
	protected void addDestinations(IChannel channel) {
		channel.setSpaceNames(spaceNames);
	}

	@Override
	protected ChannelExport createTransfer(IChannel channel) {
		return channel.getExport();
	}
}