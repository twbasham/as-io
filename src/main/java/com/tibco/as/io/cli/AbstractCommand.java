package com.tibco.as.io.cli;

import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationConfig;

public abstract class AbstractCommand implements ICommand {

	@Parameter(names = { "-writer_thread_count" }, description = "Number of writer threads")
	private Integer workerCount;
	@Parameter(names = { "-limit" }, description = "Max number of entries to read from input")
	private Long limit;
	@Parameter(names = { "-no_transfer" }, description = "Only initialize input and output without data transfer")
	private Boolean noTransfer;

	@Override
	public void configure(Collection<DestinationConfig> destinations) {
		populate(destinations);
		if (destinations.isEmpty()) {
			destinations.add(newDestination());
		}
		for (DestinationConfig destination : destinations) {
			configure(destination);
		}
	}

	protected abstract void populate(Collection<DestinationConfig> destinations);

	protected abstract DestinationConfig newDestination();

	protected void configure(DestinationConfig config) {
		if (limit != null) {
			config.setLimit(limit);
		}
		if (workerCount != null) {
			config.setWorkerCount(workerCount);
		}
		if (noTransfer != null) {
			config.setNoTransfer(noTransfer);
		}
	}

}
