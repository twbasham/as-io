package com.tibco.as.io.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.ChannelAdapter;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IInputStream;
import com.tibco.as.log.LogFactory;

public class DestinationMonitor extends ChannelAdapter {

	private Logger log = LogFactory.getLog(DestinationMonitor.class);

	private Map<IDestination, AbstractConsole> progressBars = new HashMap<IDestination, AbstractConsole>();
	private ExecutorService executor;

	@Override
	public void opened(IDestination destination) {
		AbstractConsole progressBar = getConsole(destination);
		progressBars.put(destination, progressBar);
		executor = Executors.newSingleThreadExecutor();
		executor.execute(progressBar);
	}

	private AbstractConsole getConsole(IDestination destination) {
		IInputStream in = destination.getInputStream();
		Long size = in.size();
		if (size == null) {
			return new SimpleConsole(destination);
		}
		return new ProgressConsole(destination, size);
	}

	@Override
	public void closed(IDestination destination) {
		if (progressBars.containsKey(destination)) {
			progressBars.remove(destination).stop();
			executor.shutdown();
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.log(Level.FINE, "Progress monitor interrupted", e);
			}
		}
	}

}
