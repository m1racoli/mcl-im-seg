package io.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

public class VLongTest {
	
	private static boolean is_const = false;
	
	public static void main(String[] args) throws IOException {

		final int n = 32*1024*1024;
		final int long_size = 8;
		
		ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream(n*long_size);
		DataOutputStream out = new DataOutputStream(arrayOutputStream);
		long[] output = new long[n];
		
		long tic = System.nanoTime();
		
			for(long i = 0; i < n; i++)
				if(is_const) out.writeLong(i);
				else WritableUtils.writeVLong(out, i);

		
		long toc = System.nanoTime() - tic;
		
		System.out.printf("written: bytes/item: %f, nanos/item: %f\n",(double) arrayOutputStream.size()/n,(double) toc/n);
		
		ByteArrayInputStream arrayInputStream = new ByteArrayInputStream(arrayOutputStream.toByteArray());
		DataInputStream in = new DataInputStream(arrayInputStream);
		
		tic = System.nanoTime();
		
			for(int i = 0; i < n; i++)
				if(is_const) output[i] = in.readLong();
				else output[i] = WritableUtils.readVLong(in);
			
		toc = System.nanoTime() - tic;
		
		System.out.printf("   read: bytes/item: %f, nanos/item: %f\n",(double) arrayOutputStream.size()/n,(double) toc/n);
	}

}
