package com.tibco.as.io.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.io.ChannelAdapter;
import com.tibco.as.io.ITransfer;
import com.tibco.as.log.LogFactory;

public class ProgressMonitor extends ChannelAdapter {

	private Logger log = LogFactory.getLog(ProgressMonitor.class);

	private Map<ITransfer, ProgressBar> progressBars = new HashMap<ITransfer, ProgressBar>();
	private ExecutorService executor;

	@Override
	public void opened(ITransfer transfer) {
		ProgressBar progressBar = new ProgressBar(transfer);
		progressBars.put(transfer, progressBar);
		executor = Executors.newSingleThreadExecutor();
		executor.execute(progressBar);
	}

	@Override
	public void closed(ITransfer transfer) {
		if (progressBars.containsKey(transfer)) {
			progressBars.remove(transfer).stop();
			executor.shutdown();
			try {
				executor.awaitTermination(3, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				log.log(Level.FINE, "Progress monitor interrupted", e);
			}
		}
	}

}
