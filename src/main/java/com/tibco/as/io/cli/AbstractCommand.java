package com.tibco.as.io.cli;

import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ChannelConfig;
import com.tibco.as.io.DestinationConfig;

public abstract class AbstractCommand implements ICommand {

	@Parameter(description = "Number of writer threads", names = { "-writer_thread_count" })
	private Integer workerCount;

	@Override
	public void configure(ChannelConfig config) throws Exception {
		Collection<DestinationConfig> destinations = config.getDestinations();
		configure(destinations);
		for (DestinationConfig destination : destinations) {
			configure(destination);
		}
	}

	protected void configure(DestinationConfig config) {
		config.setWorkerCount(workerCount);
	}

	public Integer getWorkerCount() {
		return workerCount;
	}

	public void setWorkerCount(Integer workerCount) {
		this.workerCount = workerCount;
	}

	protected abstract void configure(Collection<DestinationConfig> destinations);
}
