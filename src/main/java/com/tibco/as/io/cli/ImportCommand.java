package com.tibco.as.io.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.tibco.as.io.ChannelImport;
import com.tibco.as.io.IChannel;
import com.tibco.as.io.IDestination;
import com.tibco.as.io.ImportConfig;
import com.tibco.as.io.OperationType;
import com.tibco.as.io.cli.converters.DistributionRoleConverter;
import com.tibco.as.io.cli.converters.OperationTypeConverter;
import com.tibco.as.space.Member.DistributionRole;

public abstract class ImportCommand extends AbstractCommand {

	@ParametersDelegate
	private Transfer transfer = new Transfer();
	@Parameter(names = { "-space" }, description = "Space name")
	private String space;
	@Parameter(names = { "-distribution_role" }, description = "Distribution role (none, leech, seeder)", converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;
	@Parameter(names = { "-operation" }, description = "Space operation (get, load, none, partial, put, take)", converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private OperationType operation;
	@Parameter(names = { "-wait_for_ready_timeout" }, description = "Wait for ready timeout")
	private Long waitForReadyTimeout;

	@Override
	protected void configure(IDestination destination) {
		if (space != null) {
			destination.setSpaceName(space);
		}
		ImportConfig config = destination.getImportConfig();
		transfer.configure(config);
		if (distributionRole != null) {
			config.setDistributionRole(distributionRole);
		}
		if (operation != null) {
			config.setOperation(operation);
		}
		if (waitForReadyTimeout != null) {
			config.setWaitForReadyTimeout(waitForReadyTimeout);
		}
	}

	@Override
	protected ChannelImport createTransfer(IChannel channel) {
		return channel.getImport();
	}
}
