package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.space.FieldDef;
import com.tibco.as.space.FieldDef.FieldType;
import com.tibco.as.space.Metaspace;
import com.tibco.as.space.SpaceDef;
import com.tibco.as.space.Tuple;

public abstract class AbstractImporter<T> extends AbstractMetaspaceTransfer<T, Tuple> {

	public AbstractImporter(Metaspace metaspace) {
		super(metaspace);
	}

	@Override
	protected SpaceDef getSpaceDef(Metaspace metaspace, AbstractTransfer transfer)
			throws Exception {
		AbstractImport config = (AbstractImport) transfer;
		String spaceName = getSpaceName(config);
		SpaceDef spaceDef = metaspace.getSpaceDef(spaceName);
		if (spaceDef == null) {
			spaceDef = SpaceDef.create(spaceName);
			populateSpaceDef(spaceDef, config);
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

	protected abstract void populateSpaceDef(SpaceDef spaceDef, AbstractImport config)
			throws Exception;

	@Override
	protected IOutputStream<Tuple> getOutputStream(Metaspace metaspace,
			AbstractTransfer transfer, SpaceDef spaceDef) {
		AbstractImport config = (AbstractImport) transfer;
		return new SpaceOutputStream(metaspace, spaceDef.getName(), config);
	}

	protected String getSpaceName(AbstractImport config) {
		String spaceName = config.getSpaceName();
		if (spaceName == null) {
			return getInputSpaceName(config);
		}
		return spaceName;
	}

	protected abstract String getInputSpaceName(AbstractImport config);

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

}
