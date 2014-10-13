/**
 * 
 */
package io.file;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * @author Cedrik
 *
 */
public abstract class TextFormatWriter implements Flushable, Closeable {

	private final BufferedWriter writer;
	private final Map<String, CharSequence> map = new LinkedHashMap<String, CharSequence>();
	private boolean header = false;
	
	/**
	 * @param out
	 */
	public TextFormatWriter(Writer out) {
		this.writer = new BufferedWriter(out);
	}

	/**
	 * @param out
	 * @param sz
	 */
	public TextFormatWriter(Writer out, int sz) {
		this.writer = new BufferedWriter(out,sz);
	}
	
	public final TextFormatWriter write(String field, Locale locale, String format, Object value) {
		if(map.put(field, String.format(locale, format, value)) == null && header){
			throw new IllegalStateException("header is already written and the "+field+"is not part of it");
		}
		return this;
	}
	
	public final TextFormatWriter write(String field, String format, Object value) {
		return write(field, Locale.getDefault(), format, value);
//		if(map.put(field, String.format(format, value)) == null && header){
//			throw new IllegalStateException("header is already written and the "+field+"is not part of it");
//		}
	}
	
	public final TextFormatWriter write(String field, Object obj){
		return write(field, "%s", obj);
	}
	
	public final TextFormatWriter write(String field, int val) {
		return write(field, "%d", val);
	}
	
	public final TextFormatWriter write(String field, long val) {
		return write(field, "%d", val);
	}
	
	public final TextFormatWriter write(String field, float val) {
		return write(field, "%f", val);
	}
	
	public final TextFormatWriter write(String field, double val) {
		return write(field, "%f", val);
	}
	
	public final void writeLine() throws IllegalStateException, IOException {
		if(map.isEmpty()){
			throw new IllegalStateException("no values defined");
		}
		
		if(!header){
			header(writer, map.keySet());
			header = true;
		} else {
			writer.newLine();
		}
		
		final char del = delimiter();
		Iterator<CharSequence> it = map.values().iterator();
		writer.append(it.next());
		while(it.hasNext()){
			writer.append(del);
			writer.append(it.next());
		}	
	}
	
	protected void header(BufferedWriter out, Set<String> fields) throws IOException {
		
	}
	
	protected void footer(BufferedWriter out, Set<String> fields) throws IOException {
		
	}
	
	protected abstract char delimiter();
	
	protected final boolean headerWritten(){
		return header;
	}
	
	@Override
	public final void flush() throws IOException {
		writer.flush();
	}

	@Override
	public final void close() throws IOException {
		if(!map.isEmpty()){
			footer(writer, map.keySet());
		}
		writer.close();		
	}
}
