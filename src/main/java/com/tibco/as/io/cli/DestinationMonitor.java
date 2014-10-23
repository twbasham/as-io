package com.tibco.as.io.cli;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.tibco.as.io.ChannelAdapter;
import com.tibco.as.io.IDestination;

public class DestinationMonitor extends ChannelAdapter {

	@Override
	public void started(IDestination destination) {
		AbstractConsole progressBar = getConsole(destination);
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.execute(progressBar);
	}

	private AbstractConsole getConsole(IDestination destination) {
		Long size = destination.size();
		if (size == null) {
			return new SimpleConsole(destination);
		}
		return new ProgressConsole(destination, size);
	}

}
