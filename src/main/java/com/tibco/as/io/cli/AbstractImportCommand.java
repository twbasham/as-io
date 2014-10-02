package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.Direction;
import com.tibco.as.io.OperationType;
import com.tibco.as.space.Member.DistributionRole;

public abstract class AbstractImportCommand extends AbstractCommand {

	@Parameter(description = "Batch size", names = { "-batch_size" })
	private Integer batchSize;

	@Parameter(description = "Distribution role (none, leech, seeder)", names = { "-distribution_role" }, converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;

	@Parameter(description = "Space operation (get, load, none, partial, put, take)", names = { "-operation" }, converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;

	@Parameter(description = "Wait for ready timeout", names = { "-wait_for_ready_timeout" })
	private Long waitForReadyTimeout;

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

	public OperationType getOperation() {
		return operation;
	}

	public void setOperation(OperationType operation) {
		this.operation = operation;
	}

	public Long getWaitForReadyTimeout() {
		return waitForReadyTimeout;
	}

	public void setWaitForReadyTimeout(Long waitForReadyTimeout) {
		this.waitForReadyTimeout = waitForReadyTimeout;
	}

	@Override
	protected void configure(DestinationConfig config) {
		config.setDirection(Direction.IMPORT);
		config.setBatchSize(batchSize);
		config.setDistributionRole(distributionRole);
		config.setOperation(operation);
		config.setWaitForReadyTimeout(waitForReadyTimeout);
		super.configure(config);
	}

}
