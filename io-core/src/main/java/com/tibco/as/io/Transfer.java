package com.tibco.as.io;

import com.tibco.as.convert.Attributes;

public class Transfer implements Cloneable {

	private Integer batchSize;

	private Integer workerCount;

	private Integer queueCapacity;

	private String spaceName;

	private Long limit;

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	private Attributes attributes = new Attributes();

	public String getSpaceName() {
		return spaceName;
	}

	public void setSpaceName(String spaceName) {
		this.spaceName = spaceName;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}

	public Integer getWorkerCount() {
		return workerCount;
	}

	public void setWorkerCount(Integer workerCount) {
		this.workerCount = workerCount;
	}

	public Integer getQueueCapacity() {
		return queueCapacity;
	}

	public void setQueueCapacity(Integer queueCapacity) {
		this.queueCapacity = queueCapacity;
	}

	public Attributes getAttributes() {
		return attributes;
	}

	@Override
	public Transfer clone() {
		Transfer transfer = new Transfer();
		copyTo(transfer);
		return transfer;
	}

	public void copyTo(Transfer target) {
		target.batchSize = batchSize;
		target.attributes = attributes;
		target.queueCapacity = queueCapacity;
		target.spaceName = spaceName;
		target.workerCount = workerCount;
		target.limit = limit;
	}

}
