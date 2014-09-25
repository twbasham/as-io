package com.tibco.as.io;

import java.util.List;

public interface IOutputStream<T> {

	void open() throws Exception;

	void close() throws Exception;

	void write(List<T> elements) throws Exception;

	void write(T element) throws Exception;

}