package com.tibco.as.io;

public interface IEvent {

	public enum Severity {
		DEBUG, INFO, WARN, ERROR
	}

	Throwable getException();

	String getMessage();

	Severity getSeverity();

	boolean isOK();

}
