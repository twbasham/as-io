package com.tibco.as.io.cli.converters;

import java.text.MessageFormat;
import java.util.Arrays;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.ParameterException;

public abstract class AbstractEnumConverter<T extends Enum<T>> implements
		IStringConverter<T>, IParameterValidator {

	@Override
	public T convert(String value) {
		return valueOf(value.toUpperCase());
	}

	protected abstract T valueOf(String name);

	@Override
	public void validate(String name, String value) throws ParameterException {
		try {
			convert(value);
		} catch (IllegalArgumentException e) {
			throw new ParameterException(MessageFormat.format(
					"No {0} named ''{1}''. Valid names are {2}", name, value,
					toString(getValues())));
		}
	}

	private String toString(T[] values) {
		String[] names = new String[values.length];
		for (int i = 0; i < names.length; i++) {
			names[i] = values[i].name().toLowerCase();
		}
		return Arrays.toString(names);
	}

	protected abstract T[] getValues();

}
