package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;
import com.tibco.as.util.Utils;

public class AbstractChannel {

	private Logger log = LogFactory.getLog(AbstractChannel.class);

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

	public void close() throws Exception {
		if (metaspace == null) {
			return;
		}
		metaspace.close();
		metaspace = null;
	}

	public MetaspaceTransfer getTransfer(boolean export) throws Exception {
		Collection<Destination> destinations = new ArrayList<Destination>();
		destinations.addAll(this.destinations);
		if (destinations.isEmpty()) {
			if (export) {
				for (String spaceName : metaspace.getUserSpaceNames()) {
					Destination destination = newDestination();
					destination.setSpace(spaceName);
					destinations.add(destination);
				}
			} else {
				destinations.addAll(getImportDestinations());
			}
		}
		MetaspaceTransfer transfer = new MetaspaceTransfer();
		for (Destination destination : destinations) {
			defaultDestination.copyTo(destination);
			transfer.add(getDestinationTransfer(destination, export));
		}
		return transfer;

	}

	private AbstractTransfer getDestinationTransfer(
			Destination destination, boolean export) throws Exception {
		if (export) {
			return destination.getExport();
		}
		return destination.getImport();
	}

	protected Collection<Destination> getImportDestinations() {
		return Collections.emptyList();
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

}
