package com.tibco.as.io;

import com.tibco.as.space.Member.DistributionRole;

public abstract class AbstractImport extends AbstractTransfer {

	private DistributionRole distributionRole;

	private Boolean keepSpaceOpen;

	private Operation operation;

	private Long waitForReadyTimeout;

	public DistributionRole getDistributionRole() {
		return distributionRole;
	}

	public void setDistributionRole(DistributionRole distributionRole) {
		this.distributionRole = distributionRole;
	}

	public Long getWaitForReadyTimeout() {
		return waitForReadyTimeout;
	}

	public void setWaitForReadyTimeout(Long waitForReadyTimeout) {
		this.waitForReadyTimeout = waitForReadyTimeout;
	}

	public Boolean isKeepSpaceOpen() {
		return keepSpaceOpen;
	}

	public void setKeepSpaceOpen(Boolean keepSpaceOpen) {
		this.keepSpaceOpen = keepSpaceOpen;
	}

	public Operation getOperation() {
		return operation;
	}

	public void setOperation(Operation operation) {
		this.operation = operation;
	}

	public void copyTo(AbstractImport target) {
		target.distributionRole = distributionRole;
		target.keepSpaceOpen = keepSpaceOpen;
		target.operation = operation;
		super.copyTo(target);
	}

}
