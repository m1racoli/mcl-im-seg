package io.test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class NativeTest {

	public static void main(String[] args) {
		System.out.println(new File(System.getProperty("java.library.path")).getAbsolutePath());
		System.loadLibrary("nativetest");
		
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
