package io.test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeTest {

	public static final String NATIVE_TEST_LIB_NAME = "mclnative";
	
	public static void main(String[] args) {
		System.out.println("java.library.path: "+ System.getProperty("java.library.path"));
		
		try{
			System.loadLibrary(NATIVE_TEST_LIB_NAME);
		} catch (Throwable e){
			System.err.printf("could not load libary '$s'\n",NATIVE_TEST_LIB_NAME);
			System.err.println(e.getMessage());
			System.exit(1);
		}
		
		System.out.printf("libary '%s' successfully loaded\n",NATIVE_TEST_LIB_NAME);
		
		ByteBuffer bb = ByteBuffer.allocateDirect(32);
		bb.order(ByteOrder.nativeOrder());
		
		System.out.printf("bb has array: %s\n",bb.hasArray());
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
