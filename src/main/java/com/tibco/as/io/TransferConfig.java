package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

public class TransferConfig {

	private Integer workerCount;
	private Long limit;
	private Integer batchSize;
	private Collection<String> fieldNames = new ArrayList<String>();

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
		target.fieldNames = new ArrayList<String>(fieldNames);
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

	public Collection<String> getFieldNames() {
		return fieldNames;
	}

	public void setFieldNames(Collection<String> fieldNames) {
		this.fieldNames = fieldNames;
	}

	public boolean isBatch() {
		if (batchSize == null) {
			return false;
		}
		return batchSize > 1;
	}
}
