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
		bb.putInt(0,1);
		bb.putDouble(8,3.14);
		NativeTest.hello(bb);
		System.out.println("java int: "+bb.getInt(0));
		System.out.println("java int: "+bb.getDouble(8));
	}

	private static native void hello(ByteBuffer bb);
}
