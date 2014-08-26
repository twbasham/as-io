package com.tibco.as.io.cli;

import com.tibco.as.io.Operation;

public class OperationTypeConverter extends AbstractEnumConverter<Operation> {

	@Override
	protected Operation valueOf(String name) {
		return Operation.valueOf(name);
	}

	@Override
	protected Operation[] getValues() {
		return Operation.values();
	}

}
