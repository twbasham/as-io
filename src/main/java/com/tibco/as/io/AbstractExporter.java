package com.tibco.as.io;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.ASStatus;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public abstract class AbstractExporter<T> extends AbstractMetaspaceTransfer<Tuple, T> {

	public AbstractExporter(Metaspace metaspace) {
		super(metaspace);
	}

	@Override
	protected Collection<AbstractTransfer> getTransfers(Metaspace metaspace) {
		Collection<AbstractTransfer> transfers = new ArrayList<AbstractTransfer>();
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
	protected SpaceDef getSpaceDef(Metaspace metaspace, AbstractTransfer transfer)
			throws ASException {
		String spaceName = ((AbstractExport) transfer).getSpaceName();
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			throw new ASException(ASStatus.NOT_FOUND, MessageFormat.format(
					"No space named ''{0}''", spaceName));
		}
		return spaceDef;
	}

	@Override
	protected IInputStream<Tuple> getInputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef) {
		AbstractExport export = (AbstractExport) transfer;
		return new SpaceInputStream(metaspace, spaceDef.getName(), export);
	}

	public AbstractExport addExport(String spaceName) {
		AbstractExport export = (AbstractExport) getDefaultTransfer().clone();
		export.setSpaceName(spaceName);
		addTransfer(export);
		return export;
	}

	@Override
	protected int getBatchSize(AbstractTransfer transfer) {
		AbstractExport export = (AbstractExport) transfer;
		Integer batchSize = transfer.getBatchSize();
		if (batchSize == null || batchSize == 0) {
			if (export.isAllOrNew()) {
				return 1;
			}
		}
		return super.getBatchSize(transfer);
	}
}
