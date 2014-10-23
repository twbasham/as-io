package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Utils;

public abstract class AbstractChannel implements IChannel {

	private Logger log = LogFactory.getLog(AbstractChannel.class);

	private ChannelConfig config;
	private Metaspace metaspace;
	private Collection<IDestination> destinations = new ArrayList<IDestination>();
	private Collection<IChannelListener> listeners = new ArrayList<IChannelListener>();

	protected AbstractChannel(ChannelConfig config) {
		this.config = config;
	}

	@Override
	public void addListener(IChannelListener listener) {
		listeners.add(listener);
	}

	@Override
	public void start() throws Exception {
		String metaspaceName = config.getMetaspace();
		metaspace = Utils.getMetaspace(metaspaceName);
		if (metaspace == null) {
			log.fine("Connecting to metaspace");
			metaspace = Utils.connect(metaspaceName, config.getMember());
		}
		configure();
		for (DestinationConfig destinationConfig : config.getDestinations()) {
			destinationConfig.getConversion().setDefaults(
					config.getConversion());
			destinations.add(createDestination(destinationConfig));
		}
		for (IDestination destination : destinations) {
			try {
				destination.start();
				for (IChannelListener listener : listeners) {
					listener.started(destination);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not open destination "
						+ destination.getName(), e);
			}
		}
	}

	@Override
	public void awaitTermination() {
		for (IDestination destination : destinations) {
			try {
				destination.awaitTermination();
			} catch (InterruptedException e) {
				log.log(Level.SEVERE,
						"Interrupted while waiting for termination of destination ''{0}''",
						destination.getName());
			}
		}
	}

	@Override
	public void stop() throws Exception {
		for (IDestination destination : destinations) {
			try {
				destination.stop();
				for (IChannelListener listener : listeners) {
					listener.stopped(destination);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close destination"
						+ destination.getName(), e);
			}
		}
		log.fine("Disconnecting from metaspace");
		metaspace.close();
		metaspace = null;
	}

	protected void configure() throws Exception {
		Collection<DestinationConfig> destinations = new ArrayList<DestinationConfig>();
		for (DestinationConfig destination : config.getDestinations()) {
			if (destination.isWildcard()) {
				for (String spaceName : metaspace.getUserSpaceNames()) {
					DestinationConfig destinationConfig = destination.clone();
					destinationConfig.setSpace(spaceName);
					destinations.add(destinationConfig);
				}
			} else {
				destinations.add(destination);
			}
		}
		config.setDestinations(destinations);
	}

	protected abstract IDestination createDestination(DestinationConfig config);

	public Metaspace getMetaspace() {
		return metaspace;
	}

	@Override
	public Collection<IDestination> getDestinations() {
		return destinations;
	}

}
