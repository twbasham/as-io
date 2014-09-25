package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.Metaspace;

public abstract class AbstractChannel implements IChannel {

	private Logger log = LogFactory.getLog(AbstractChannel.class);

	private Metaspace metaspace;

	private Collection<DestinationConfig> configs = new ArrayList<DestinationConfig>();

	private Collection<IDestination> destinations = new ArrayList<IDestination>();

	private Collection<IChannelListener> listeners = new ArrayList<IChannelListener>();

	protected AbstractChannel(Metaspace metaspace) {
		this.metaspace = metaspace;
	}

	@Override
	public Metaspace getMetaspace() {
		return metaspace;
	}

	public void addListener(IChannelListener listener) {
		listeners.add(listener);
	}

	@Override
	public void open() throws Exception {
		for (DestinationConfig config : configs) {
			try {
				IDestination destination = getDestination(config);
				destination.open();
				for (IChannelListener listener : listeners) {
					listener.opened(destination);
				}
				destinations.add(destination);
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not open destination", e);
			}
		}
	}

	protected abstract IDestination getDestination(DestinationConfig config)
			throws Exception;

	@Override
	public void close() throws Exception {
		for (IDestination destination : destinations) {
			try {
				destination.close();
				for (IChannelListener listener : listeners) {
					listener.closed(destination);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not close destination", e);
			}
		}
	}

	@Override
	public Collection<DestinationConfig> getConfigs() {
		return configs;
	}

	@Override
	public void addConfig(DestinationConfig config) {
		configs.add(config);
	}

}
