package io.cluster;

import iterators.ReadOnlyIterator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * array backed cluster set
 * 
 * @author Cedrik
 *
 */
public class ArrayClustering extends AbstractSet<Cluster<Integer>> implements Clustering<Integer> {

	private static final Pattern PATTERN = Pattern.compile("\\t");
	private final int l;
	private int[] clPtr = new int[32];
	private int[] clElements = new int[32];
	private int[] clIdx = new int[32];
	
	public ArrayClustering(File file) throws IOException {
		l = read(new FileReader(file), 0);		
	}
	
	private int read(Reader in, int pos) throws IOException {
		BufferedReader reader = new BufferedReader(in);
		int l = clPtr[pos];
		for(String line = reader.readLine(); line != null; line = reader.readLine()){			
			final String[] split = PATTERN.split(line);			
			for(int i = 0; i < split.length; i++){
				final int index = Integer.parseInt(split[i]);
				set(clElements,l++,index);
				set(clIdx,index,pos);
			}
			set(clPtr,++pos,l);
		}
		reader.close();
		return pos;
	}
	
	private static void set(int[] ar, int pos, int val){
		if(ar.length <= pos) grow(ar, pos);
		ar[pos] = val;
	}

	private static int[] grow(int[] ar, int minCapacity){
		// this doesn't handle overflows or OOM errors
		int oldCapacity = ar.length;
        int newCapacity = oldCapacity + (oldCapacity >> 1);
        if (newCapacity - minCapacity < 0)
            newCapacity = minCapacity;
		return Arrays.copyOf(ar, newCapacity);
	}
	
	@Override
	public int size() {
		return l;
	}
	
	@Override
	public Iterator<Cluster<Integer>> iterator() {		
		return new ReadOnlyIterator<Cluster<Integer>>() {
			private int i = 0;
			
			@Override
			public boolean hasNext() {
				return i < l;
			}

			@Override
			public Cluster<Integer> next() {
				return new ArrayCluster(i);
			}
		};
	}

	private class ArrayCluster extends AbstractSet<Integer> implements Cluster<Integer> {

		final int id;
		final int s;
		final int t;
		
		ArrayCluster(int id){
			this.id = id;
			this.s = clPtr[id];
			this.t = clPtr[id+1];
		}
		
		@Override
		public Iterator<Integer> iterator() {
			return new ReadOnlyIterator<Integer>() {
				private int i = s;
				
				@Override
				public boolean hasNext() {
					return i < t;
				}

				@Override
				public Integer next() {
					return clElements[i++];
				}
			};
		}

		@Override
		public int size() {
			return t-s;
		}
		
		@Override
		public boolean contains(Object o) {
			if(o instanceof Integer){
				int val = (Integer) o;
				return val >= 0 && val < clIdx.length && clIdx[val] == id;
			}
			return false;
		}		
	}
	
}
