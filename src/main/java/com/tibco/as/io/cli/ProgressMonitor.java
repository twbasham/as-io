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
import com.tibco.as.log.LogFactory;

public class ProgressMonitor extends ChannelAdapter {

	private Logger log = LogFactory.getLog(ProgressMonitor.class);

	private Map<IDestination, ProgressBar> progressBars = new HashMap<IDestination, ProgressBar>();
	private ExecutorService executor;

	@Override
	public void opened(IDestination destination) {
		ProgressBar progressBar = new ProgressBar(destination);
		progressBars.put(destination, progressBar);
		executor = Executors.newSingleThreadExecutor();
		executor.execute(progressBar);
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
