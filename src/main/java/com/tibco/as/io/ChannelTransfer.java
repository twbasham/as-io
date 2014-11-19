package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ChannelTransfer {

	private Collection<IChannelTransferListener> listeners = new ArrayList<IChannelTransferListener>();
	private Collection<IDestinationTransfer> transfers = new ArrayList<IDestinationTransfer>();

	public void execute() throws Exception {
		if (transfers.isEmpty()) {
			return;
		}
		int nThreads = transfers.size();
		ExecutorService executor = Executors.newFixedThreadPool(nThreads);
		for (IDestinationTransfer transfer : transfers) {
			for (IChannelTransferListener listener : listeners) {
				listener.executing(transfer);
			}
			executor.execute(transfer);
		}
		executor.shutdown();
		while (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
			// do nothing
		}
	}

	public void addDestinationTransfer(IDestinationTransfer transfer) {
		transfers.add(transfer);
	}

	public void addListener(IChannelTransferListener listener) {
		listeners.add(listener);
	}

}
