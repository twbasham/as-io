package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.Direction;
import com.tibco.as.io.OperationType;
import com.tibco.as.space.Member.DistributionRole;

public abstract class AbstractImportCommand extends AbstractCommand {

	@Parameter(description = "Distribution role (none, leech, seeder)", names = { "-distribution_role" }, converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;

	@Parameter(description = "Space operation (get, load, none, partial, put, take)", names = { "-operation" }, converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;

	@Parameter(description = "Wait for ready timeout", names = { "-wait_for_ready_timeout" })
	private Long waitForReadyTimeout;

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.IMPORT);
		config.setDistributionRole(distributionRole);
		config.setOperation(operation);
		config.setWaitForReadyTimeout(waitForReadyTimeout);
		super.configure(config);
	}

}
