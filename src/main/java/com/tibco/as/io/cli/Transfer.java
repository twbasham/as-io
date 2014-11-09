package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.TransferConfig;

public class Transfer {

	@Parameter(names = { "-transfer_thread_count" }, description = "Number of worker threads to use for transfer")
	private Integer workerCount;
	@Parameter(names = { "-limit" }, description = "Max number of entries to read from input")
	private Long limit;
	@Parameter(names = { "-batch_size" }, description = "Transfer output batch size")
	private Integer batchSize;
	@Parameter(names = { "-no_transfer" }, description = "Only initialize input and output without data transfer")
	private Boolean noTransfer;

	public Long getLimit() {
		return limit;
	}

	public Integer getWorkerCount() {
		return workerCount;
	}

	public Boolean getNoTransfer() {
		return noTransfer;
	}

	public void configure(TransferConfig config) {
		if (limit != null) {
			config.setLimit(limit);
		}
		if (workerCount != null) {
			config.setWorkerCount(workerCount);
		}
		if (batchSize != null) {
			config.setBatchSize(batchSize);
		}
	}
}
