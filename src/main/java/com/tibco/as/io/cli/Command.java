package com.tibco.as.io.cli;

import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.DestinationConfig;

public class Command implements ICommand {

	@Parameter(names = { "-writer_thread_count" }, description = "Number of writer threads")
	private Integer workerCount;

	@Override
	public void configure(ChannelConfig config) throws Exception {
		Collection<DestinationConfig> destinations = config.getDestinations();
		for (DestinationConfig destination : destinations) {
			configure(destination);
		}
	}

	protected void configure(DestinationConfig config) {
		config.setWorkerCount(workerCount);
	}

}
