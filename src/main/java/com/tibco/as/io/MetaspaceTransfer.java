package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class MetaspaceTransfer {

	private Collection<IMetaspaceTransferListener> listeners = new ArrayList<IMetaspaceTransferListener>();
	private Collection<AbstractTransfer> transfers = new ArrayList<AbstractTransfer>();

	public void add(AbstractTransfer transfer) {
		transfers.add(transfer);
	}

	public void execute() throws InterruptedException {
		if (transfers.size() == 0) {
			return;
		}
		ExecutorService executor = Executors.newFixedThreadPool(transfers
				.size());
		for (AbstractTransfer transfer : transfers) {
			for (IMetaspaceTransferListener listener : listeners) {
				listener.executing(transfer);
			}
			executor.execute(transfer);
		}
		executor.shutdown();
		while (!executor.awaitTermination(100, TimeUnit.MILLISECONDS)) {
			// do nothing
		}
	}

	public void addListener(IMetaspaceTransferListener listener) {
		listeners.add(listener);
	}

}
