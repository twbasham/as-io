package com.tibco.as.io;

import java.util.List;

public interface IOutputStream<T> extends ICloseable {

	void write(List<T> elements) throws Exception;

	void write(T element) throws Exception;

}