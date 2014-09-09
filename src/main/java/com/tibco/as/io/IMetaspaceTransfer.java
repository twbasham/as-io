package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.space.Metaspace;

public interface IMetaspaceTransfer {

	void addListener(IMetaspaceTransferListener listener);

	void stop() throws Exception;

	boolean isStopped();

	void execute() throws TransferException;

	void addTransfer(AbstractTransfer transfer);

	Metaspace getMetaspace();

	void setDefaultTransfer(AbstractTransfer transfer);

	Collection<ITransfer> getExecutors();

}
