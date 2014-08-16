package io.test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import io.writables.CSCSlice;
import io.writables.SliceId;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import mapred.MCLContext;

public class CSCSliceTest extends MCLContext {

	public static void main(String[] args) throws IOException {
		
		setN(9);
		setNSub(3);
		setKMax(9);
		setPrintMatrix(PrintMatrix.ALL);
		Logger.getLogger(CSCSlice.class).setLevel(Level.DEBUG);
		
		int[] col = new int[] {1,0,1,0,1,2,2,2};
		long[] row = new long[] {0,1,2,4,5,0,1,4};
		float[] val = new float[] {1.0f,0.5f,1.0f,3.0f,2.0f,1,2,3};
		
		CSCSlice slice = new CSCSlice();
		slice.fill(col,row,val);
		System.out.println(slice);
		SliceId id = new SliceId();
		for(CSCSlice m : rewrite(slice).getSubBlocks(id)){
			CSCSlice o = rewrite(m);
			
			System.out.println(o);
			System.out.println(o.multipliedBy(slice, null));
		}
	}
	
	public static CSCSlice rewrite(CSCSlice m) throws IOException {
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		m.write(new DataOutputStream(outputStream));
		CSCSlice o = new CSCSlice();
		ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
		o.readFields(new DataInputStream(inputStream));
		return o;
	}

}
