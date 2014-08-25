package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.io.ICloseable;
import com.tibco.as.space.ASException;
import com.tibco.as.space.Member.DistributionRole;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public interface IOperation extends ICloseable {

	Tuple execute(Tuple tuple) throws ASException;

	SpaceResultList execute(Collection<Tuple> tuples);

	void close() throws ASException;

	void setDistributionRole(DistributionRole distributionRole);

	void setKeepSpaceOpen(boolean keepSpaceOpen);

	void setWaitForReadyTimeout(Long waitTimeout);

}