package com.tibco.as.io;

import java.util.Collection;

public interface IMetaspaceTransferListener {

	void opening(Collection<ITransfer> transfers);
	
	void executing(ITransfer transfer);

}
