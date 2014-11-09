package com.tibco.as.io;

import com.tibco.as.space.Member.DistributionRole;

public class ImportConfig extends TransferConfig {

	private OperationType operation;
	private Long waitForReadyTimeout;
	private DistributionRole distributionRole;

	public void copyTo(ImportConfig target) {
		if (target.operation == null) {
			target.operation = operation;
		}
		if (target.waitForReadyTimeout == null) {
			target.waitForReadyTimeout = waitForReadyTimeout;
		}
		if (target.distributionRole == null) {
			target.distributionRole = distributionRole;
		}
		super.copyTo(target);
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

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

}
