package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.convert.Direction;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.OperationType;
import com.tibco.as.io.cli.converters.DistributionRoleConverter;
import com.tibco.as.io.cli.converters.OperationTypeConverter;
import com.tibco.as.space.Member.DistributionRole;

public abstract class AbstractImportCommand extends AbstractCommand {

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

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.IMPORT);
		if (space != null) {
			config.setSpace(space);
		}
		if (spaceBatchSize != null) {
			config.setSpaceBatchSize(spaceBatchSize);
		}
		if (distributionRole != null) {
			config.setDistributionRole(distributionRole);
		}
		if (operation != null) {
			config.setOperation(operation);
		}
		if (waitForReadyTimeout != null) {
			config.setWaitForReadyTimeout(waitForReadyTimeout);
		}
		super.configure(config);
	}

}
