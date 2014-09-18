package com.tibco.as.io.cli;

public class LogLevelConverter extends AbstractEnumConverter<LogLevel> {

	@Override
	protected LogLevel valueOf(String name) {
		return LogLevel.valueOf(name);
	}

	@Override
	protected LogLevel[] getValues() {
		return LogLevel.values();
	}

}
