package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;
import com.tibco.as.util.Utils;
import com.tibco.as.util.convert.Settings;
import com.tibco.as.util.log.LogFactory;

public class Channel implements IChannel {

	private Logger log = LogFactory.getLog(Channel.class);

	private String metaspaceName;
	private Member member = new Member();
	private Settings settings = new Settings();
	private Collection<IDestination> destinations = new ArrayList<IDestination>();
	private Metaspace metaspace;

	public Channel(String metaspaceName) {
		this.metaspaceName = metaspaceName;
	}

	@Override
	public Member getMember() {
		return member;
	}

	@Override
	public void open() throws Exception {
		if (metaspace == null) {
			metaspace = Utils.getMetaspace(metaspaceName);
			if (metaspace == null) {
				log.fine("Connecting to metaspace");
				metaspace = Utils.connect(metaspaceName, member);
			}
		}
	}

	@Override
	public void close() throws Exception {
		if (metaspace == null) {
			return;
		}
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

	@Override
	public Settings getSettings() {
		return settings;
	}

}
