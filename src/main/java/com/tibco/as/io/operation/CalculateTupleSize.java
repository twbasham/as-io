package com.tibco.as.io.operation;

import java.util.Collection;

import com.tibco.as.space.ASException;
import com.tibco.as.space.ASStatus;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.Space;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.SpaceResultList;
import com.tibco.as.space.Tuple;
import com.tibco.as.space.impl.data.ASBlob;
import com.tibco.as.space.impl.data.ASSpaceResult;
import com.tibco.as.space.impl.data.ASSpaceResultList;
import com.tibco.as.space.impl.data.ASTuple;
import com.tibco.as.space.impl.serializer.ASEncoder;
import com.tibco.as.space.impl.serializer.ASSerializerException;

public class CalculateTupleSize extends AbstractOperation {

	private static final String FIELD_LENGTH = "length";
	private static final String FIELD_BYTES = "bytes";
	private static final String FIELD_CAPACITY = "capacity";

	private ASEncoder encoder = new ASEncoder();

	public CalculateTupleSize(Metaspace metaspace, String spaceName) {
		super(metaspace, spaceName);
	}

	@Override
	protected Tuple execute(Space space, Tuple tuple) throws ASException {
		return getEncodedData(space.getSpaceDef(), tuple);
	}

	private Tuple getEncodedData(SpaceDef spaceDef, Tuple tuple)
			throws ASException {
		try {
			((ASTuple) tuple).serialize(encoder, true, spaceDef);
		} catch (ASSerializerException e) {
			throw new ASException(ASStatus.SYS_ERROR, e);
		}
		ASBlob data = encoder.getData();
		Tuple result = Tuple.create();
		result.put(FIELD_BYTES, data.getBytes());
		result.put(FIELD_CAPACITY, data.getCapacity());
		result.put(FIELD_LENGTH, data.getLength());
		return tuple;
	}

	@Override
	protected SpaceResultList execute(Space space, Collection<Tuple> tuples) {
		SpaceResultList resultList = new ASSpaceResultList();
		try {
			SpaceDef spaceDef = space.getSpaceDef();
			for (Tuple tuple : tuples) {
				Tuple result = getEncodedData(spaceDef, tuple);
				resultList.add(new ASSpaceResult((ASTuple) result, null));
			}
		} catch (ASException e) {
			e.printStackTrace();
		}
		return resultList;
	}

	@Override
	public String toString() {
		return "CalculateTupleSize";
	}

}
