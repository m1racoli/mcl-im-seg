package util;

import org.apache.hadoop.fs.Path;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IStringConverterFactory;

/**
 * for Jcommander
 * 
 * @author Cedrik
 *
 */
public class PathConverter implements IStringConverter<Path> {

	@Override
	public Path convert(String value) {
		return new Path(value);
	}
	
	public static class Factory implements IStringConverterFactory {

		@SuppressWarnings({ "rawtypes", "unchecked" })
		@Override
		public Class<? extends IStringConverter<?>> getConverter(Class cls) {
			if(cls.equals(Path.class)) return PathConverter.class;
			return null;
		}

	}
}
