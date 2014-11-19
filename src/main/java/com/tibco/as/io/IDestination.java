package com.tibco.as.io;

import com.tibco.as.space.SpaceDef;
import com.tibco.as.util.convert.IAccessor;
import com.tibco.as.util.convert.IConverter;

public interface IDestination {

	IChannel getChannel();

	ExportConfig getExportConfig();

	ImportConfig getImportConfig();

	SpaceDef getSpaceDef();

	void setSpaceDef(SpaceDef spaceDef);

	String getName();

	IInputStream getInputStream();

	IOutputStream getOutputStream();

	IAccessor[] getObjectAccessors(TransferConfig transfer);

	IAccessor[] getTupleAccessors(TransferConfig transfer);

	IConverter[] getJavaConverters(TransferConfig transfer);

}
