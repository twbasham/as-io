package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.convert.Settings;
import com.tibco.as.util.Member;

public class ChannelConfig {

	private String metaspace;
	private Member member;
	private Collection<DestinationConfig> destinations = new ArrayList<DestinationConfig>();
	private Settings conversionConfig = new Settings();

	public Member getMember() {
		return member;
	}

	public void setMember(Member member) {
		this.member = member;
	}

	public Collection<DestinationConfig> getDestinations() {
		return destinations;
	}

	public void setDestinations(Collection<DestinationConfig> destinations) {
		this.destinations = destinations;
	}

	public String getMetaspace() {
		return metaspace;
	}

	public void setMetaspace(String metaspace) {
		this.metaspace = metaspace;
	}

	public Settings getConversion() {
		return conversionConfig;
	}

}
