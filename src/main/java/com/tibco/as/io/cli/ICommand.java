package com.tibco.as.io.cli;

import com.tibco.as.io.AbstractChannel;
import com.tibco.as.io.MetaspaceTransfer;

public interface ICommand {

	MetaspaceTransfer getTransfer(AbstractChannel channel) throws Exception;

}