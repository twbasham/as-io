package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.io.Channel;
import com.tibco.as.io.ChannelImport;
import com.tibco.as.io.Destination;
import com.tibco.as.io.OperationType;
import com.tibco.as.io.cli.converters.DistributionRoleConverter;
import com.tibco.as.io.cli.converters.OperationTypeConverter;
import com.tibco.as.space.Member.DistributionRole;

public class ImportCommand implements ICommand {

	@ParametersDelegate
	private Transfer transfer = new Transfer();
	@Parameter(names = { "-space" }, description = "Space name")
	private String space;
	@Parameter(names = { "-space_batch_size" }, description = "Batch size for space operations")
	private Integer spaceBatchSize;
	@Parameter(names = { "-distribution_role" }, description = "Distribution role (none, leech, seeder)", converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;
	@Parameter(names = { "-operation" }, description = "Space operation (get, load, none, partial, put, take)", converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;
	@Parameter(names = { "-wait_for_ready_timeout" }, description = "Wait for ready timeout")
	private Long waitForReadyTimeout;

	protected void configure(Destination destination) {
		destination.setImportLimit(transfer.getLimit());
		destination.setImportWorkerCount(transfer.getWorkerCount());
		destination.setSpace(space);
		destination.setSpaceBatchSize(spaceBatchSize);
		destination.setDistributionRole(distributionRole);
		destination.setOperation(operation);
		destination.setWaitForReadyTimeout(waitForReadyTimeout);
	}

	@Override
	public ChannelImport getTransfer(Channel channel) throws Exception {
		configure(channel.getDefaultDestination());
		return channel.getImport();
	}
}
