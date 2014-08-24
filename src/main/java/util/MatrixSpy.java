/**
 * 
 */
package util;

import java.awt.Transparency;
import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.awt.image.ComponentColorModel;
import java.awt.image.DataBuffer;
import java.awt.image.DataBufferByte;
import java.awt.image.Raster;
import java.awt.image.SampleModel;
import java.awt.image.WritableRaster;
import java.io.File;

import javax.imageio.ImageIO;

import io.writables.MCLMatrixSlice;
import io.writables.MatrixMeta;
import io.writables.SliceId;
import io.writables.MCLMatrixSlice.MatrixEntry;
import mapred.MCLConfigHelper;
import mapred.MCLContext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.util.Options.PathOption;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;

/**
 * @author Cedrik
 *
 */
public class MatrixSpy<M extends MCLMatrixSlice<M>> extends AbstractUtil {

	private static final Logger logger = LoggerFactory.getLogger(MatrixSpy.class);
	
	@Parameter(names = "-n", required = true)
	private int n;
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MatrixSpy(), args));
	}

	@SuppressWarnings("unchecked")
	@Override
	protected int run(Path input, Path output, boolean hdfsOutput) throws Exception {
		
		Configuration conf = getConf();
		MatrixMeta meta = MatrixMeta.load(conf, input);
		
		if (meta == null) {
			return -1;
		}
		
		meta.apply(conf);
		FileSystem fs = input.getFileSystem(conf);
		
		PathFilter filter = new PathFilter() {
			
			@Override
			public boolean accept(Path path) {
				String name = path.getName();
				return !name.startsWith("_") && !name.startsWith(".");
			}
		};
		
		SliceId id = new SliceId();
		M m = null;
		int cnt = 0;
		long m_n = meta.getN();
		int nsub = meta.getNSub();
		double[] spy = new double[n*n];
		
		logger.info("resolution: {} entries/pixel", Math.pow((double) m_n/n,2.0));
		
		for(FileStatus fileStatus : fs.listStatus(input,filter)) {
			Option pathOption = SequenceFile.Reader.file(fileStatus.getPath());
			SequenceFile.Reader reader = new SequenceFile.Reader(conf, pathOption);
			
			if(m == null) {
				MCLConfigHelper.setMatrixSliceClass(conf, (Class<? extends MCLMatrixSlice<?>>) reader.getValueClass());
				m = MCLContext.getMatrixSliceInstance(conf);
			}
			
			while (reader.next(id, m)) {
				for(MatrixEntry e : m.dump()) {
					long j = ((long) id.getSliceId()*(long) nsub) + e.col;
					long i = e.row;
					int spy_i = (int) ((i * n)/m_n);
					int spy_j = (int) ((j * n)/m_n);
					spy[spy_j + (n* spy_i)] += 1.0;//e.val;
					cnt++;
				}
			}
			
			reader.close();
		}
		
		double max = 0.0f;
		for (int i = spy.length-1; i >= 0; --i){
			double val = spy[i];
			if(max < val) max = val;
		}
		
		for (int i = spy.length-1; i >= 0; --i){
			spy[i] /= max;
		}
		
		BufferedImage image = getGrayScale(n, spy);
		//TODO FileSystem outFS = hdfsOutput ? output.getFileSystem(conf) : FileSystem.getLocal(conf);
		File file = new File("spy.png");
		ImageIO.write(image, "png", file);
		
		logger.info("count: {}",cnt);
		return 0;
	}
	
	public static BufferedImage getGrayScale(int width, double[] vals) {
		int height = vals.length / width;
		byte[] buffer = new byte[vals.length];
	    for (int i = buffer.length - 1; i >= 0; --i) {
	    	buffer[i] = vals[i] == 0.0 ? (byte) 255 : (byte) (255 * (0.9 - 0.9 *vals[i]));
	    }
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
	    int[] nBits = { 8 };
	    ColorModel cm = new ComponentColorModel(cs, nBits, false, true,
	            Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
	    SampleModel sm = cm.createCompatibleSampleModel(width, height);
	    DataBufferByte db = new DataBufferByte(buffer, width * height);
	    WritableRaster raster = Raster.createWritableRaster(sm, db, null);
	    BufferedImage result = new BufferedImage(cm, raster, false, null);
	    
	    return result;
	}
	
	public static BufferedImage getRGBScale(int width, double[] vals) {
		int height = vals.length / width;
		byte[] buffer = new byte[vals.length];
	    for (int i = buffer.length - 1; i >= 0; --i) {
	    	buffer[i] = (byte) (255 * vals[i]);
	    }
		ColorSpace cs = ColorSpace.getInstance(ColorSpace.CS_GRAY);
	    int[] nBits = { 8 };
	    ColorModel cm = new ComponentColorModel(cs, nBits, false, true,
	            Transparency.OPAQUE, DataBuffer.TYPE_BYTE);
	    SampleModel sm = cm.createCompatibleSampleModel(width, height);
	    DataBufferByte db = new DataBufferByte(buffer, width * height);
	    WritableRaster raster = Raster.createWritableRaster(sm, db, null);
	    BufferedImage result = new BufferedImage(cm, raster, false, null);
	    
	    return result;
	}

}
