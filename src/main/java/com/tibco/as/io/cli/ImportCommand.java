package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.Direction;
import com.tibco.as.io.OperationType;
import com.tibco.as.io.cli.converters.DistributionRoleConverter;
import com.tibco.as.io.cli.converters.OperationTypeConverter;
import com.tibco.as.space.Member.DistributionRole;

public class ImportCommand extends Command {

	@Parameter(names = { "-space_batch_size" }, description = "Batch size for space operations")
	private Integer spaceBatchSize;
	@Parameter(names = { "-distribution_role" }, description = "Distribution role (none, leech, seeder)", converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;
	@Parameter(names = { "-operation" }, description = "Space operation (get, load, none, partial, put, take)", converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;
	@Parameter(names = { "-wait_for_ready_timeout" }, description = "Wait for ready timeout")
	private Long waitForReadyTimeout;

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.IMPORT);
		config.setSpaceBatchSize(spaceBatchSize);
		config.setDistributionRole(distributionRole);
		config.setOperation(operation);
		config.setWaitForReadyTimeout(waitForReadyTimeout);
		super.configure(config);
	}

}
