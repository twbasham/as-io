package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class AbstractChannelTransfer implements IChannelTransfer {

	private Channel channel;
	private Collection<IChannelTransferListener> listeners = new ArrayList<IChannelTransferListener>();
	private Collection<IDestinationTransfer> transfers = new ArrayList<IDestinationTransfer>();
	private ExecutorService executor;

	public AbstractChannelTransfer(Channel channel) {
		this.channel = channel;
	}

	@Override
	public void prepare() throws Exception {
		for (Destination destination : getDestinations(channel)) {
			IDestinationTransfer transfer = getTransfer(destination);
			transfer.prepare();
			transfers.add(transfer);
		}
		if (transfers.isEmpty()) {
			return;
		}
		executor = Executors.newFixedThreadPool(transfers.size());
	}

	protected abstract Collection<Destination> getDestinations(Channel channel)
			throws Exception;

	@Override
	public void execute() throws Exception {
		if (executor == null) {
			return;
		}
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

	public Collection<IDestinationTransfer> getTransfers() {
		return transfers;
	}

	protected abstract IDestinationTransfer getTransfer(Destination destination);

	@Override
	public void addListener(IChannelTransferListener listener) {
		listeners.add(listener);
	}

}
