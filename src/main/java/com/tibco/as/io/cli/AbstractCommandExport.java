package com.tibco.as.io.cli;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.AbstractExport;
import com.tibco.as.io.IMetaspaceTransfer;
import com.tibco.as.io.ITransfer;
import com.tibco.as.space.ASException;
import com.tibco.as.space.InvokeOptions;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.browser.BrowserDef.BrowserType;
import com.tibco.as.space.browser.BrowserDef.DistributionScope;
import com.tibco.as.space.browser.BrowserDef.TimeScope;
import com.tibco.as.space.remote.InvokeResult;
import com.tibco.as.space.remote.InvokeResultList;
import com.tibco.as.space.remote.MemberInvocable;

public abstract class AbstractCommandExport extends AbstractCommand implements
		MemberInvocable {

	private static final String FIELD_BROWSER_TYPE = "browserType";

	private static final String FIELD_TIME_SCOPE = "timeScope";

	private static final String FIELD_DISTRIBUTION_SCOPE = "distributionScope";

	private static final String FIELD_TIMEOUT = "timeout";

	private static final String FIELD_PREFETCH = "prefetch";

	private static final String FIELD_QUERY_LIMIT = "queryLimit";

	private static final String FIELD_FILTER = "filter";

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
	public void execute(Metaspace metaspace) {
		if (isRemote()) {
			Collection<String> spaceNames = new ArrayList<String>(
					this.spaceNames);
			if (spaceNames.isEmpty()) {
				try {
					spaceNames.addAll(metaspace.getUserSpaceNames());
				} catch (ASException e) {
					System.err.println("Could not get user space names");
				}
			}
			InvokeOptions options = InvokeOptions.create();
			Tuple context = Tuple.create();
			configure(context);
			options.setContext(context);
			for (String spaceName : spaceNames) {
				try {
					InvokeResultList resultList = metaspace.getSpace(spaceName)
							.invokeSeeders(getClass().getName(), options);
					if (resultList.hasError()) {
						for (InvokeResult result : resultList) {
							if (result.hasError()) {
								System.err.println(result.getError()
										.getMessage());
							}
						}
					}
				} catch (ASException e) {
					System.err.println(MessageFormat
							.format("Could not get members of space ''{0}''",
									spaceName));
				}
			}
		} else {
			super.execute(metaspace);
		}
	}

	protected boolean isRemote() {
		return false;
	}

	@Override
	protected void configure(Tuple context) {
		super.configure(context);
		if (browserType != null) {
			context.putString(FIELD_BROWSER_TYPE, browserType.name());
		}
		if (timeScope != null) {
			context.putString(FIELD_TIME_SCOPE, timeScope.name());
		}
		if (distributionScope != null) {
			context.putString(FIELD_DISTRIBUTION_SCOPE,
					distributionScope.name());
		}
		if (timeout != null) {
			context.putLong(FIELD_TIMEOUT, timeout);
		}
		if (prefetch != null) {
			context.putLong(FIELD_PREFETCH, prefetch);
		}
		if (queryLimit != null) {
			context.putLong(FIELD_QUERY_LIMIT, queryLimit);
		}
		context.putString(FIELD_FILTER, filter);
	}

	@Override
	protected void initialize(Space space, Tuple context) {
		super.initialize(space, context);
		String browserTypeName = context.getString(FIELD_BROWSER_TYPE);
		if (browserTypeName != null) {
			browserType = BrowserType.valueOf(browserTypeName);
		}
		String timeScopeName = context.getString(FIELD_TIME_SCOPE);
		if (timeScopeName != null) {
			timeScope = TimeScope.valueOf(timeScopeName);
		}
		String distributionScopeName = context
				.getString(FIELD_DISTRIBUTION_SCOPE);
		if (distributionScopeName == null) {
			distributionScope = DistributionScope.SEEDED;
		} else {
			distributionScope = DistributionScope
					.valueOf(distributionScopeName);
		}
		timeout = context.getLong(FIELD_TIMEOUT);
		prefetch = context.getLong(FIELD_PREFETCH);
		queryLimit = context.getLong(FIELD_QUERY_LIMIT);
		filter = context.getString(FIELD_FILTER);
	}

	public void configure(AbstractExport transfer) {
		super.configure(transfer);
		transfer.setBrowserType(browserType);
		transfer.setTimeScope(timeScope);
		transfer.setDistributionScope(distributionScope);
		transfer.setTimeout(timeout);
		transfer.setPrefetch(prefetch);
		transfer.setQueryLimit(queryLimit);
		transfer.setFilter(filter);
	}

	@Override
	protected Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace) {
		return getMetaspaceTransfers(metaspace, spaceNames);
	}

	@Override
	protected String getExecutingMessage(Collection<ITransfer> transfers) {
		return MessageFormat.format("Exporting {0} space(s)", transfers.size());
	}

	@Override
	protected String getOpenedMessage(ITransfer transfer) {
		return MessageFormat.format("Exporting {0}", transfer.getInputStream()
				.getName());
	}

	@Override
	protected String getClosedMessage(ITransfer transfer) {
		return MessageFormat.format("Exported {0}", transfer.getInputStream()
				.getName());
	}

	protected abstract Collection<IMetaspaceTransfer> getMetaspaceTransfers(
			Metaspace metaspace, Collection<String> spaceNames);

	@Override
	public Tuple invoke(Space space, Tuple context) {
		Tuple result = Tuple.create();
		spaceNames.add(space.getName());
		initialize(space, context);
		try {
			execute(space.getMetaspace());
		} catch (ASException e) {
			e.printStackTrace();
		}
		return result;
	}

}