package com.tibco.as.io.cli;

import java.util.Collection;

import com.tibco.as.io.DestinationConfig;

public interface ICommand {

	void configure(Collection<DestinationConfig> destinations);

}