/**
 * 
 */
package io.input;

import java.awt.image.RenderedImage;
import java.io.IOException;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author Cedrik
 *
 */
public class ImageRecordReader extends RecordReader<Path,RenderedImage> {

	private FSDataInputStream fileIn;
	private ImageReader in;
	private Path file;
	private RenderedImage image;
	
	/**
	 * 
	 */
	public ImageRecordReader() {}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#close()
	 */
	@Override
	public void close() throws IOException {
		if(fileIn != null) {
			fileIn.close();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
	 */
	@Override
	public Path getCurrentKey() throws IOException, InterruptedException {
		return file;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
	 */
	@Override
	public RenderedImage getCurrentValue() throws IOException, InterruptedException {
		return image;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		return image == null ? 0.0f : 1.0f;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#initialize(org.apache.hadoop.mapreduce.InputSplit, org.apache.hadoop.mapreduce.TaskAttemptContext)
	 */
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
		
		Configuration conf = context.getConfiguration();
		
		final Path file = fileSplit.getPath();
		final FileSystem fs = file.getFileSystem(conf);
		
		fileIn = fs.open(file);
		//TODO
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(image == null){
			image = ImageIO.read(fileIn);
			return image != null;			
		}
		//image.getTile(0, 0).
		return false;
	}

}
