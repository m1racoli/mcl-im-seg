package io.test;

import java.util.PriorityQueue;
import java.util.Queue;

public class BufferTest {

	public static void main(String[] args) {
		Queue<Integer> buffer = new PriorityQueue<Integer>();
		int[] vals = new int[]{4,2,5,1,5,7,2};
		for(int val : vals){
			buffer.add(val);
		}
		
		final int size = buffer.size();
		for(int i = 0; i<size;i++){
			System.out.println(buffer.remove());
		}
	}

}
