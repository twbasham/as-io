package com.tibco.as.io;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.ASStatus;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public abstract class Exporter<T> extends MetaspaceTransfer<Tuple, T> {

	public Exporter(Metaspace metaspace) {
		super(metaspace);
	}

	@Override
	protected Export createTransfer() {
		return new Export();
	}

	@Override
	protected Collection<Transfer> getTransfers(Metaspace metaspace) {
		Collection<Transfer> transfers = new ArrayList<Transfer>();
		try {
			for (String spaceName : metaspace.getUserSpaceNames()) {
				addExport(spaceName);
			}
		} catch (ASException e) {
			EventManager.error(e,
					"Could not get user space names in metaspace {0}",
					metaspace.getName());
		}
		return transfers;
	}

	@Override
	protected SpaceDef getSpaceDef(Metaspace metaspace, Transfer transfer)
			throws ASException {
		String spaceName = ((Export) transfer).getSpaceName();
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			throw new ASException(ASStatus.NOT_FOUND, MessageFormat.format(
					"No space named ''{0}''", spaceName));
		}
		return spaceDef;
	}

	@Override
	protected IInputStream<Tuple> getInputStream(Metaspace metaspace,
			Transfer transfer, SpaceDef spaceDef) {
		Export export = (Export) transfer;
		return new SpaceInputStream(metaspace, spaceDef.getName(), export);
	}

	@Override
	protected IOutputStream<T> getOutputStream(Metaspace metaspace,
			Transfer transfer, SpaceDef spaceDef) throws TransferException {
		throw new TransferException("Output not set");
	}

	public Export addExport(String spaceName) {
		Export export = (Export) getDefaultTransfer().clone();
		export.setSpaceName(spaceName);
		addTransfer(export);
		return export;
	}

	@Override
	protected int getBatchSize(Transfer transfer) {
		Export export = (Export) transfer;
		Integer batchSize = transfer.getBatchSize();
		if (batchSize == null || batchSize == 0) {
			if (export.isAllOrNew()) {
				return 1;
			}
		}
		return super.getBatchSize(transfer);
	}
}
