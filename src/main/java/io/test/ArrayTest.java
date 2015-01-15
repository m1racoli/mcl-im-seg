package io.test;

public class ArrayTest {

	public static void main(String[] args) {
		int w = 35;
		int h = 4;
		System.out.printf("a=int[%d][%d]\n",w,h);
		
		int[][] a = new int[w][h];
		System.out.printf("a.len = %d\n",a.length);
		System.out.printf("a[0].len = %d\n",a[0].length);
		

	}

}
