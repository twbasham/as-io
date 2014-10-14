package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tibco.as.convert.Direction;
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

	@Override
	public void open() throws Exception {
		metaspace = Utils.getMetaspace(config.getMetaspace());
		if (metaspace == null) {
			metaspace = Metaspace
					.connect(config.getMetaspace(), getMemberDef());
		}
		for (DestinationConfig destinationConfig : config.getDestinations()) {
			for (DestinationConfig found : discover(destinationConfig)) {
				IDestination destination = createDestination(found);
				for (IChannelListener listener : listeners) {
					listener.opening(destination);
				}
				try {
					destination.open(metaspace);
					destinations.add(destination);
					for (IChannelListener listener : listeners) {
						listener.opened(destination);
					}
					if (!config.getParallel()) {
						close(destination);
					}
				} catch (Exception e) {
					log.log(Level.SEVERE, "Could not open destination", e);
				}
			}
		}
	}

	protected Collection<? extends DestinationConfig> discover(
			DestinationConfig destination) throws Exception {
		if (isWildcard(destination)) {
			Collection<DestinationConfig> destinations = new ArrayList<DestinationConfig>();
			for (String spaceName : metaspace.getUserSpaceNames()) {
				DestinationConfig destinationConfig = destination.clone();
				destinationConfig.setSpace(spaceName);
				destinations.add(destinationConfig);
			}
			return destinations;
		}
		return Arrays.asList(destination);
	}

	protected boolean isWildcard(DestinationConfig destination) {
		if (destination.getDirection() == Direction.EXPORT) {
			return destination.getSpace() == null;
		}
		return false;
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

	@Override
	public void close() throws Exception {
		for (IDestination destination : destinations) {
			close(destination);
		}
		for (IDestination destination : destinations) {
			while (!destination.isClosed()) {
				Thread.sleep(100);
			}
		}
		metaspace.close();
		metaspace = null;
	}

	private void close(IDestination destination) {
		for (IChannelListener listener : listeners) {
			listener.closing(destination);
		}
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
