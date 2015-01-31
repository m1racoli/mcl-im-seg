package io.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeTest {

	public static void main(String[] args) {
		System.out.println("java.library.path: "+ System.getProperty("java.library.path"));
		
		try{
			System.loadLibrary("nativetest");
		} catch (Throwable e){
			System.err.println("could not load libary 'nativetest'");
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		System.out.println("libary 'nativetest' successfully loaded");
		
		ByteBuffer bb = ByteBuffer.allocateDirect(32);
		bb.order(ByteOrder.nativeOrder());
		int i = 1;
		double d = 3.14;
		bb.putInt(0,i);
		bb.putDouble(8,d);
		System.out.printf("JAVA: put int %d, put double %f\n",i,d);
		NativeTest.hello(bb);
		i = bb.getInt(0);
		d = bb.getDouble(8);
		System.out.printf("JAVA: get int %d, get double %f\n",i,d);
	}

	private static native void hello(ByteBuffer bb);
}
