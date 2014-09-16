package com.tibco.as.io.cli;

import java.text.MessageFormat;
import java.util.Collection;

import com.beust.jcommander.Parameter;
import com.tibco.as.io.ITransfer;
import com.tibco.as.io.AbstractImport;
import com.tibco.as.io.Operation;
import com.tibco.as.space.Member.DistributionRole;

public abstract class AbstractImportCommand extends AbstractCommand {

	@Parameter(description = "Distribution role (none, leech, seeder)", names = { "-distribution_role" }, converter = DistributionRoleConverter.class, validateWith = DistributionRoleConverter.class)
	private DistributionRole distributionRole;

	@Parameter(description = "Space operation (get, load, none, partial, put, take)", names = { "-operation" }, converter = OperationTypeConverter.class, validateWith = OperationTypeConverter.class)
	private Operation operation;

	@Parameter(description = "Wait for ready timeout", names = { "-wait_for_ready_timeout" })
	private Long waitForReadyTimeout;

	public void configure(AbstractImport transfer) {
		super.configure(transfer);
		transfer.setDistributionRole(distributionRole);
		transfer.setOperation(operation);
		transfer.setWaitForReadyTimeout(waitForReadyTimeout);
	}

	@Override
	protected String getOpenedMessage(ITransfer transfer) {
		return MessageFormat.format("Importing {0}", transfer.getInputStream()
				.getName());
	}

	@Override
	protected String getClosedMessage(ITransfer transfer) {
		return MessageFormat.format("Imported {0}", transfer.getInputStream()
				.getName());
	}

	@Override
	protected String getExecutingMessage(Collection<ITransfer> transfers) {
		return MessageFormat.format("Importing {0} space(s)", transfers.size());
	}

}
