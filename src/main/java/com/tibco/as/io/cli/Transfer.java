package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;

public class Transfer {

	@Parameter(names = { "-transfer_thread_count" }, description = "Number of worker threads to use for transfer")
	private Integer workerCount;
	@Parameter(names = { "-limit" }, description = "Max number of entries to read from input")
	private Long limit;
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
}
