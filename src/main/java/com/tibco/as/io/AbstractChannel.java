package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.MemberDef;
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

	protected void open() throws Exception {
		String metaspaceName = config.getMetaspace();
		metaspace = Utils.getMetaspace(metaspaceName);
		if (metaspace == null) {
			log.info("Connecting to metaspace");
			metaspace = Metaspace.connect(metaspaceName, getMemberDef());
		}
		discover();
		for (DestinationConfig destinationConfig : config.getDestinations()) {
			destinations.add(createDestination(destinationConfig));
		}
	}

	@Override
	public void start() throws Exception {
		open();
		for (IDestination destination : destinations) {
			for (IChannelListener listener : listeners) {
				listener.starting(destination);
			}
			try {
				destination.start();
				for (IChannelListener listener : listeners) {
					listener.started(destination);
				}
			} catch (Exception e) {
				log.log(Level.SEVERE, "Could not open destination "
						+ destination.getName(), e);
			}
			if (config.isSequential()) {
				destination.stop();
			}
		}
	}

	@Override
	public void stop() throws Exception {
		for (IDestination destination : destinations) {
			for (IChannelListener listener : listeners) {
				listener.stopping(destination);
			}
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
		close();
	}

	protected void discover() throws Exception {
		for (DestinationConfig destination : config.getDestinations()) {
			if (destination.isWildcard()) {
				config.removeDestinationConfig(destination);
				for (String spaceName : metaspace.getUserSpaceNames()) {
					DestinationConfig destinationConfig = destination.clone();
					destinationConfig.setSpace(spaceName);
					config.addDestinationConfig(destinationConfig);
				}
			}
		}
	}

	private MemberDef getMemberDef() {
		MemberDef memberDef = MemberDef.create();
		if (config.getClusterSuspendThreshold() != null) {
			memberDef.setClusterSuspendThreshold(config
					.getClusterSuspendThreshold());
		}
		if (config.getConnectTimeout() != null) {
			memberDef.setConnectTimeout(config.getConnectTimeout());
		}
		if (config.getDataStore() != null) {
			memberDef.setDataStore(config.getDataStore());
		}
		if (config.getDiscovery() != null) {
			memberDef.setDiscovery(config.getDiscovery());
		}
		if (config.getIdentityPassword() != null) {
			memberDef.setIdentityPassword(config.getIdentityPassword()
					.toCharArray());
		}
		if (config.getListen() != null) {
			memberDef.setListen(config.getListen());
		}
		if (config.getMember() != null) {
			memberDef.setMemberName(config.getMember());
		}
		if (config.getMemberTimeout() != null) {
			memberDef.setMemberTimeout(config.getMemberTimeout());
		}
		if (config.getRemoteDiscovery() != null) {
			memberDef.setRemoteDiscovery(config.getRemoteDiscovery());
		}
		if (config.getRemoteListen() != null) {
			memberDef.setRemoteListen(config.getRemoteListen());
		}
		if (config.getRxBufferSize() != null) {
			memberDef.setRxBufferSize(config.getRxBufferSize());
		}
		if (config.getSecurityPolicyFile() != null) {
			memberDef.setSecurityPolicyFile(config.getSecurityPolicyFile());
		}
		if (config.getSecurityTokenFile() != null) {
			memberDef.setSecurityTokenFile(config.getSecurityTokenFile());
		}
		if (config.getTransportThreadCount() != null) {
			memberDef.setTransportThreadCount(config.getTransportThreadCount());
		}
		if (config.getWorkerThreadCount() != null) {
			memberDef.setWorkerThreadCount(config.getWorkerThreadCount());
		}
		return memberDef;
	}

	protected abstract IDestination createDestination(DestinationConfig config);

	protected void close() throws Exception {
		for (IDestination destination : destinations) {
			while (!destination.isClosed()) {
				Thread.sleep(100);
			}
		}
		log.info("Disconnecting from metaspace");
		metaspace.close();
		metaspace = null;
	}

	public Metaspace getMetaspace() {
		return metaspace;
	}

	@Override
	public Collection<IDestination> getDestinations() {
		return destinations;
	}

}
