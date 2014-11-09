package com.tibco.as.io;

public class TransferConfig {

	private Integer workerCount;
	private Long limit;
	private Integer batchSize;

	public void copyTo(TransferConfig target) {
		if (target.workerCount == null) {
			target.workerCount = workerCount;
		}
		if (target.limit == null) {
			target.limit = limit;
		}
		if (target.batchSize == null) {
			target.batchSize = batchSize;
		}
	}

	public Integer getWorkerCount() {
		return workerCount;
	}

	public void setWorkerCount(Integer workerCount) {
		this.workerCount = workerCount;
	}

	public Long getLimit() {
		return limit;
	}

	public void setLimit(Long limit) {
		this.limit = limit;
	}

	public Integer getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(Integer batchSize) {
		this.batchSize = batchSize;
	}
}
