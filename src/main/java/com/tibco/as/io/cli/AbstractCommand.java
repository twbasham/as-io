package com.tibco.as.io.cli;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelTransfer;
import com.tibco.as.io.Destination;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IDestinationTransfer;
import com.tibco.as.io.TransferConfig;

public abstract class AbstractCommand implements ICommand {

	@Parameter(names = { "-transfer_thread_count" }, description = "Number of worker threads to use for transfer")
	private Integer workerCount;
	@Parameter(names = { "-limit" }, description = "Max number of entries to read from input")
	private Long limit;
	@Parameter(names = { "-batch_size" }, description = "Transfer output batch size")
	private Integer batchSize;
	@Parameter(names = "-fields", description = "Names of specific fields to transfer, e.g. field1 field2", variableArity = true)
	private List<String> fieldNames;

	@Override
	public ChannelTransfer getTransfer(IChannel channel) throws Exception {
		ChannelTransfer transfer = new ChannelTransfer();
		for (IDestination destination : channel.getDestinations()) {
			IDestinationTransfer destinationTransfer = getTransfer(destination);
			TransferConfig config = destinationTransfer.getConfig();
			configure(config);
			transfer.addDestinationTransfer(destinationTransfer);
		}
		return transfer;
	}

	protected void configure(TransferConfig config) {
		if (limit != null) {
			config.setLimit(limit);
		}
		if (workerCount != null) {
			config.setWorkerCount(workerCount);
		}
		if (batchSize != null) {
			config.setBatchSize(batchSize);
		}
		if (fieldNames != null) {
			config.setFieldNames(fieldNames);
		}
	}

	protected abstract IDestinationTransfer getTransfer(IDestination destination);

	protected abstract Destination createDestination(IChannel channel);

}
