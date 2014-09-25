package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationConfig;
import com.tibco.as.io.IChannel;

public abstract class AbstractCommand implements ICommand {

	@Parameter(description = "Batch size", names = { "-batch_size" })
	private Integer batchSize;
	@Parameter(description = "Number of writer threads", names = { "-writer_thread_count" })
	private Integer workerCount;

	protected void configure(DestinationConfig config) {
		config.setBatchSize(batchSize);
		config.setWorkerCount(workerCount);
	}

	@Override
	public void configure(IChannel channel) throws Exception {
		for (DestinationConfig config : channel.getConfigs()) {
			configure(config);
		}
	}

}
