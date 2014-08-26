package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.space.Metaspace;

public interface IMetaspaceTransfer {

	void addListener(IMetaspaceTransferListener listener);

	void stop() throws Exception;

	boolean isStopped();

	void execute() throws TransferException;

	void addTransfer(Transfer transfer);

	Metaspace getMetaspace();

	void setDefaultTransfer(Transfer transfer);

	Collection<ITransfer> getExecutors();

}
