package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.Destination;
import com.tibco.as.io.MetaspaceTransfer;

public abstract class AbstractCommand implements ICommand {

	@Parameter(names = { "-transfer_thread_count" }, description = "Number of worker threads to use for transfer")
	private Integer workerCount;
	@Parameter(names = { "-limit" }, description = "Max number of entries to read from input")
	private Long limit;
	@Parameter(names = { "-no_transfer" }, description = "Only initialize input and output without data transfer")
	private Boolean noTransfer;

	@Override
	public MetaspaceTransfer getTransfer(AbstractChannel channel)
			throws Exception {
		configure(channel.getDefaultDestination());
		return channel.getTransfer(isExport());
	}

	protected void configure(Destination destination) {
		if (isExport()) {
			destination.setExportLimit(limit);
			destination.setExportWorkerCount(workerCount);
		} else {
			destination.setImportLimit(limit);
			destination.setImportWorkerCount(workerCount);
		}
	}

	protected abstract boolean isExport();

}
