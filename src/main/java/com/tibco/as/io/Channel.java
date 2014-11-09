package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Logger;

import com.tibco.as.log.LogFactory;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Metaspace;
import com.tibco.as.util.Member;
import com.tibco.as.util.Utils;

public class Channel implements IChannel {

	private Logger log = LogFactory.getLog(Channel.class);

	private String metaspaceName;
	private Member member = new Member();
	private Collection<Destination> destinations = new ArrayList<Destination>();
	private Destination defaultDestination = newDestination();
	private Metaspace metaspace;

	protected Channel(String metaspaceName) {
		this.metaspaceName = metaspaceName;
	}

	public Destination newDestination() {
		return new Destination(this);
	}

	@Override
	public Member getMember() {
		return member;
	}

	@Override
	public IDestination getDefaultDestination() {
		return defaultDestination;
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
	public ChannelExport getExport() {
		return new ChannelExport(this);
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
	public ChannelImport getImport() {
		return new ChannelImport(this);
	}

	public Collection<Destination> getExportDestinations() throws Exception {
		Collection<Destination> destinations = new ArrayList<Destination>();
		for (Destination destination : this.destinations) {
			for (Destination ed : getExportDestinations(destination)) {
				defaultDestination.copyTo(ed);
				destinations.add(ed);
			}
		}
		return destinations;
	}

	protected Collection<Destination> getExportDestinations(
			Destination destination) throws ASException {
		Collection<Destination> destinations = new ArrayList<Destination>();
		String destinationSpaceName = destination.getSpaceName();
		for (String spaceName : metaspace.getUserSpaceNames()) {
			if (Utils.matches(spaceName, destinationSpaceName, false)) {
				Destination found = newDestination();
				found.setSpaceName(spaceName);
				destination.copyTo(found);
				destinations.add(found);
			}
		}
		if (destinations.isEmpty()) {
			destinations.add(destination);
		}
		return destinations;
	}

	public Collection<Destination> getImportDestinations() throws Exception {
		ArrayList<Destination> destinations = new ArrayList<Destination>();
		for (Destination destination : this.destinations) {
			for (Destination id : getImportDestinations(destination)) {
				defaultDestination.copyTo(id);
				destinations.add(id);
			}
		}
		return destinations;
	}

	protected Collection<Destination> getImportDestinations(
			Destination destination) throws Exception {
		Collection<Destination> destinations = new ArrayList<Destination>();
		destinations.add(destination);
		return destinations;
	}

	public Collection<Destination> getDestinations() {
		return destinations;
	}

	@Override
	public void setSpaceNames(Collection<String> spaceNames) {
		for (String spaceName : spaceNames) {
			Destination destination = newDestination();
			destination.setSpaceName(spaceName);
			addDestination(destination);
		}
	}

	protected void addDestination(Destination destination) {
		destinations.add(destination);
	}

}
