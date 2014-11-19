package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.DestinationImport;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.IDestinationTransfer;
import com.tibco.as.io.ImportConfig;
import com.tibco.as.io.OperationType;
import com.tibco.as.io.TransferConfig;
import com.tibco.as.io.cli.converters.DistributionRoleConverter;
import com.tibco.as.io.cli.converters.OperationTypeConverter;
import com.tibco.as.space.Member.DistributionRole;

public abstract class ImportCommand extends AbstractCommand {

	@Parameter(names = { "-space" }, description = "Space name")
	private String space;
	@Parameter(names = { "-distribution_role" }, description = "Distribution role (none, leech, seeder)", converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;
	@Parameter(names = { "-operation" }, description = "Space operation (get, load, none, partial, put, take)", converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;
	@Parameter(names = { "-wait_for_ready_timeout" }, description = "Wait for ready timeout")
	private Long waitForReadyTimeout;

	@Override
	protected void configure(TransferConfig config) {
		ImportConfig importConfig = (ImportConfig) config;
		if (distributionRole != null) {
			importConfig.setDistributionRole(distributionRole);
		}
		if (operation != null) {
			importConfig.setOperation(operation);
		}
		if (waitForReadyTimeout != null) {
			importConfig.setWaitForReadyTimeout(waitForReadyTimeout);
		}
		super.configure(config);
	}

	@Override
	protected IDestinationTransfer getTransfer(IDestination destination) {
		if (space != null) {
			destination.getSpaceDef().setName(space);
		}
		return new DestinationImport(destination);
	}

}
