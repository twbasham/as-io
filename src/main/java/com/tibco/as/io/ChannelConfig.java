package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.util.Member;

public class ChannelConfig {

	private String metaspace;
	private Member member;
	private Collection<DestinationConfig> destinations = new ArrayList<DestinationConfig>();

	protected ChannelConfig() {
	}

	public Member getMember() {
		return member;
	}

	public void setMember(Member member) {
		this.member = member;
	}

	public void addDestinationConfig(DestinationConfig config) {
		destinations.add(config);
	}

	public DestinationConfig addDestinationConfig() {
		DestinationConfig destination = newDestinationConfig();
		destinations.add(destination);
		return destination;
	}

	protected DestinationConfig newDestinationConfig() {
		return new DestinationConfig();
	}

	public String getMetaspace() {
		return metaspace;
	}

	public void setMetaspace(String metaspace) {
		this.metaspace = metaspace;
	}

	public DestinationConfig[] getDestinations() {
		return destinations.toArray(new DestinationConfig[destinations.size()]);
	}

	public boolean removeDestinationConfig(DestinationConfig destination) {
		return destinations.remove(destination);
	}

}
