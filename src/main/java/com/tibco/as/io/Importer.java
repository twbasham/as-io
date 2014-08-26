package com.tibco.as.io;

import java.util.ArrayList;
import java.util.Collection;

import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public abstract class Importer<T> extends MetaspaceTransfer<T, Tuple> {

	public Importer(Metaspace metaspace) {
		super(metaspace);
	}

	@Override
	protected Import createTransfer() {
		return new Import();
	}

	@Override
	protected SpaceDef getSpaceDef(Metaspace metaspace, Transfer transfer)
			throws Exception {
		Import config = (Import) transfer;
		String spaceName = getSpaceName(config);
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			spaceDef = createSpaceDef(spaceName, config);
			Collection<String> keys = spaceDef.getKeyDef().getFieldNames();
			if (keys.isEmpty()) {
				for (FieldDef fieldDef : spaceDef.getFieldDefs()) {
					keys.add(fieldDef.getName());
				}
			}
			metaspace.defineSpace(spaceDef);
		}
		return spaceDef;
	}

	protected abstract SpaceDef createSpaceDef(String spaceName, Import config) throws Exception;

	@Override
	protected IOutputStream<Tuple> getOutputStream(Metaspace metaspace,
			Transfer transfer, SpaceDef spaceDef) {
		Import config = (Import) transfer;
		return new SpaceOutputStream(metaspace, spaceDef.getName(), config);
	}

	protected String getSpaceName(Import config) {
		String spaceName = config.getSpaceName();
		if (spaceName == null) {
			return getInputSpaceName(config);
		}
		return spaceName;
	}

	protected abstract String getInputSpaceName(Import config);

	protected FieldDef[] getFieldDefs(String[] names, FieldType[] types) {
		int length = Math.max(names.length, types == null ? 0 : types.length);
		FieldDef[] fieldDefs = new FieldDef[length];
		for (int index = 0; index < fieldDefs.length; index++) {
			String name = get(names, index);
			FieldType type = get(types, index);
			if (type == null) {
				type = FieldType.STRING;
			}
			fieldDefs[index] = FieldDef.create(name, type);
		}
		return fieldDefs;
	}

	private <A> A get(A[] array, int index) {
		if (array == null) {
			return null;
		}
		if (array.length > index) {
			return array[index];
		}
		return null;
	}

	@Override
	protected Collection<Transfer> getTransfers(Metaspace metaspace) {
		return new ArrayList<Transfer>();
	}

}
