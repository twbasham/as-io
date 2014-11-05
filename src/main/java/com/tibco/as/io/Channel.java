package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;
import com.tibco.as.util.Utils;

public class Channel {

	private Logger log = LogFactory.getLog(Channel.class);

	private String metaspaceName;
	private Member member = new Member();
	private Collection<Destination> destinations = new ArrayList<Destination>();
	private Destination defaultDestination = newDestination();
	private Metaspace metaspace;

	public Member getMember() {
		return member;
	}

	public String getMetaspaceName() {
		return metaspaceName;
	}

	public void setMetaspaceName(String metaspaceName) {
		this.metaspaceName = metaspaceName;
	}

	public Destination getDefaultDestination() {
		return defaultDestination;
	}

	public void open() throws Exception {
		if (metaspace == null) {
			metaspace = Utils.getMetaspace(metaspaceName);
			if (metaspace == null) {
				log.fine("Connecting to metaspace");
				metaspace = Utils.connect(metaspaceName, member);
			}
		}
	}

	public ChannelExport getExport() {
		return new ChannelExport(this);
	}

	public void close() throws Exception {
		if (metaspace == null) {
			return;
		}
		metaspace.close();
		metaspace = null;
	}

	public void setMember(Member member) {
		this.member = member;
	}

	protected Destination newDestination() {
		return new Destination(this);
	}

	public Destination addDestination() {
		Destination destination = newDestination();
		destinations.add(destination);
		return destination;
	}

	public Metaspace getMetaspace() {
		return metaspace;
	}

	public Collection<Destination> getDestinations() {
		return destinations;
	}

	public ChannelImport getImport() {
		return new ChannelImport(this);
	}

	public Collection<Destination> getExportDestinations() throws Exception {
		Collection<Destination> ed = new ArrayList<Destination>(destinations);
		if (ed.isEmpty()) {
			for (String spaceName : metaspace.getUserSpaceNames()) {
				Destination destination = newDestination();
				destination.setSpace(spaceName);
				ed.add(destination);
			}
		}
		configure(ed);
		return ed;
	}

	protected void configure(Collection<Destination> destinations) {
		for (Destination destination : destinations) {
			defaultDestination.copyTo(destination);
		}
	}

	public Collection<Destination> getImportDestinations() throws Exception {
		ArrayList<Destination> id = new ArrayList<Destination>(destinations);
		configure(id);
		return id;
	}

}
