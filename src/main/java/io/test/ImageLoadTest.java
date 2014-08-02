package io.test;

import java.awt.color.ColorSpace;
import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;

public class ImageLoadTest {

	public static final String[] SUFFIXES = {".jpg",".png",".tif"};
	public static final FileFilter filter = new FileFilter() {
		
		@Override
		public boolean accept(File pathname) {
			
			if(pathname.isDirectory())
				return false;
			
			final String filename = pathname.getName();
			
			for(String suffix : SUFFIXES){
				if(filename.toLowerCase().endsWith(suffix))
					return true;
			}
			
			return false;
		}
	};
	
	public static void main(String[] args) throws IOException {
		File dir = new File("C:\\Users\\Cedrik\\Documents\\TU-Berlin\\mcl-im-seg\\src\\main\\resources\\io\\test");
		
		File[] files = dir.listFiles(filter);
		for(File file : files){
			System.out.println(file);
			ImageInputStream inputStream = ImageIO.createImageInputStream(file);
			System.out.println(inputStream);			
			Iterator<ImageReader> readers = ImageIO.getImageReaders(inputStream);
			while(readers.hasNext()){
				ImageReader reader = readers.next();
				reader.setInput(inputStream);
				System.out.println(reader);
				System.out.printf("ratio: %f, h: %d, w: %d\n",
						reader.getAspectRatio(0),
						reader.getHeight(0),
						reader.getWidth(0));
				BufferedImage image = reader.read(0);
				int rgb = image.getRGB(0, 0);
				byte[] bytes = ByteBuffer.allocate(4).putInt(rgb).array();
				ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
				DataInputStream dataInputStream = new DataInputStream(stream);
				System.out.printf("First pixel: %d %d %d %d\n",
						dataInputStream.readUnsignedByte(),
						dataInputStream.readUnsignedByte(),
						dataInputStream.readUnsignedByte(),
						dataInputStream.readUnsignedByte());
				System.out.println(reader.getFormatName());
				IIOMetadata meta = reader.getImageMetadata(0);
				System.out.println(meta.getNativeMetadataFormatName());
				ColorModel colorModel = image.getColorModel();
				System.out.println("Color model: "+colorModel);
				System.out.println("Color model class: "+colorModel.getClass());
				ColorSpace colorSpace = colorModel.getColorSpace();
				System.out.println(colorSpace.isCS_sRGB());
				System.out.println(colorSpace.getType());
				for(int i = 0; i<colorSpace.getNumComponents();i++){
					System.out.println(colorSpace.getName(i));
				}
			}
			System.out.println();
		}
	}
	
}
