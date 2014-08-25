package com.tibco.as.io;

public interface ITransferListener {

	void opened();

	void transferred(int count);

	void closed();

}
