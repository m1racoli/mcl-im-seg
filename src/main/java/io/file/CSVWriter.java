/**
 * 
 */
package io.file;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Cedrik
 *
 */
public class CSVWriter extends TextFormatWriter {

	private static final char DEL = ',';
	
	/**
	 * @param out
	 */
	public CSVWriter(Writer out) {
		super(out);
	}

	/**
	 * @param out
	 * @param sz
	 */
	public CSVWriter(Writer out, int sz) {
		super(out, sz);
	}

	/* (non-Javadoc)
	 * @see io.file.TextFormatWriter#header(java.io.BufferedWriter, java.util.Set)
	 */
	@Override
	protected void header(BufferedWriter out, Set<String> fields)
			throws IOException {
		Iterator<String> it = fields.iterator();
		out.append(it.next());
		while(it.hasNext()){
			out.append(DEL);
			out.append(it.next());
		}
		out.newLine();
	}

	/* (non-Javadoc)
	 * @see io.file.TextFormatWriter#delimiter()
	 */
	@Override
	protected char delimiter() {
		return DEL;
	}

}
