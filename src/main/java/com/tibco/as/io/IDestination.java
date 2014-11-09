package com.tibco.as.io;

import java.util.Collection;

import com.tibco.as.convert.Settings;

public interface IDestination {

	Settings getSettings();

	ExportConfig getExportConfig();

	ImportConfig getImportConfig();

	void setSpaceName(String spaceName);

	void setFieldNames(Collection<String> fieldNames);

}
