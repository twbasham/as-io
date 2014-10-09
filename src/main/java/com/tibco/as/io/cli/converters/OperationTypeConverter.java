package com.tibco.as.io.cli.converters;

import com.tibco.as.io.OperationType;

public class OperationTypeConverter extends AbstractEnumConverter<OperationType> {

	@Override
	protected OperationType valueOf(String name) {
		return OperationType.valueOf(name);
	}

	@Override
	protected OperationType[] getValues() {
		return OperationType.values();
	}

}
