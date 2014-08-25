package com.tibco.as.io;

public class Event implements IEvent {

	private Severity severity;
	private String message;
	private Throwable throwable;

	public Event(Severity severity, String message) {
		this.severity = severity;
		this.message = message;
	}

	public Event(Severity severity, Throwable throwable) {
		this(severity, throwable.getLocalizedMessage(), throwable);
	}

	public Event(Severity severity, String message, Throwable throwable) {
		this(severity, message);
		this.throwable = throwable;
	}

	@Override
	public Throwable getException() {
		return throwable;
	}

	@Override
	public String getMessage() {
		return message;
	}

	@Override
	public Severity getSeverity() {
		return severity;
	}

	@Override
	public boolean isOK() {
		return severity == Severity.DEBUG || severity == Severity.INFO;
	}

}
