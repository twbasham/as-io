package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;

public interface IOperation {

	Tuple execute(Tuple tuple) throws ASException;

	SpaceResultList execute(Collection<Tuple> tuples);

	void open() throws ASException;

	void close() throws ASException;

}