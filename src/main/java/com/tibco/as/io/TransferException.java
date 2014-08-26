package com.tibco.as.io;

public class TransferException extends Exception {

	private static final long serialVersionUID = -4128963211516788215L;

	public TransferException(String message) {
		super(message);
	}

	public TransferException(String message, Throwable cause) {
		super(message, cause);
	}
}
