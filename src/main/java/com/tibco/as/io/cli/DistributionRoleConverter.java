package com.tibco.as.io.cli;

import com.tibco.as.space.Member.DistributionRole;

public class DistributionRoleConverter extends AbstractEnumConverter<DistributionRole> {

	@Override
	protected DistributionRole valueOf(String name) {
		return DistributionRole.valueOf(name);
	}

	@Override
	protected DistributionRole[] getValues() {
		return DistributionRole.values();
	}

}
