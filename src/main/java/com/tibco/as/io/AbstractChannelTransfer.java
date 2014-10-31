package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public abstract class AbstractChannelTransfer {

	private Collection<IMetaspaceTransferListener> listeners = new ArrayList<IMetaspaceTransferListener>();
	private Channel channel;

	public AbstractChannelTransfer(Channel channel) {
		this.channel = channel;
	}

	public void execute() throws Exception {
		Collection<Destination> destinations = new ArrayList<Destination>();
		destinations.addAll(channel.getDestinations());
		if (destinations.isEmpty()) {
			destinations.addAll(getDestinations(channel));
		}
		if (destinations.isEmpty()) {
			return;
		}
		ExecutorService executor = Executors.newFixedThreadPool(destinations
				.size());
		for (Destination destination : destinations) {
			channel.getDefaultDestination().copyTo(destination);
			AbstractDestinationTransfer transfer = getTransfer(destination);
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

	protected abstract AbstractDestinationTransfer getTransfer(
			Destination destination) throws Exception;

	protected abstract Collection<Destination> getDestinations(Channel channel)
			throws Exception;

	public void addListener(IMetaspaceTransferListener listener) {
		listeners.add(listener);
	}

}
