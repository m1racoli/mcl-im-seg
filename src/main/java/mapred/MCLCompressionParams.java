/**
 * 
 */
package mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Lz4Codec;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class MCLCompressionParams implements Applyable {

	private static final String COMPRESS_MAP_CONF = "mapreduce.map.output.compress";
	private static final String COMPRESS_MAP_CODEC_CONF = "mapreduce.map.output.compress.codec";
	
	private static final String COMRESS_OUTPUT_CONF = "mapreduce.output.fileoutputformat.compress";
	private static final String COMRESS_OUTPUT_CODEC_CONF = "mapreduce.output.fileoutputformat.compress.codec";
	private static final String COMRESS_OUTPUT_TYPE_CONF = "mapreduce.output.fileoutputformat.compress.type";
	
	@Parameter(names = "-cm")
	private boolean compress_map_output = false;
	
	@Parameter(names = "-co")
	private boolean compress_output = false;
	
	private final CompressionCodec codec;
	private final CompressionType type;
	
	/**
	 * CompressionParameters with LZ4 as default codec and Block as default compression type
	 */
	public MCLCompressionParams() {
		this(new Lz4Codec(), CompressionType.BLOCK);
	}
	
	public MCLCompressionParams(boolean compress_map, boolean compress_output){
		this();
		this.compress_map_output = compress_map;
		this.compress_output = compress_output;
	}
	
	public MCLCompressionParams(CompressionCodec codec, CompressionType type){
		this.codec = codec;
		this.type = type;
	}
	
	@Override
	public void apply(Configuration conf) {
		conf.setBoolean(COMPRESS_MAP_CONF, compress_map_output);
		conf.setClass(COMPRESS_MAP_CODEC_CONF, codec.getClass(), CompressionCodec.class);
		conf.setBoolean(COMRESS_OUTPUT_CONF, compress_output);
		conf.setClass(COMRESS_OUTPUT_CODEC_CONF, codec.getClass(), CompressionCodec.class);
		conf.setEnum(COMRESS_OUTPUT_TYPE_CONF, type);
	}

}
