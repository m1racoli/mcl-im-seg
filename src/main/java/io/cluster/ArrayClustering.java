package io.cluster;

import iterators.ConcatenatedIterator;
import iterators.IntArrayIterator;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * array backed cluster set
 * 
 * @author Cedrik
 *
 */
public class ArrayClustering extends AbstractSet<Cluster<Integer>> implements Clustering<Integer> {

	private static final Logger logger = LoggerFactory.getLogger(ArrayClustering.class);
	
	private static final Pattern PATTERN = Pattern.compile("\\t");
	private final int l;
	private final int end;
	private int[] clPtr = new int[32];
	private int[] clElements = new int[32];
	private int[] clIdx = new int[32];
	
	public ArrayClustering(File file) throws IOException {
		logger.debug("open {}",file);
		l = read(new FileReader(file), 0);
		end = clPtr[l];
		logger.debug("{} clusters read",l);
	}
	
	private int read(Reader in, int pos) throws IOException {
		BufferedReader reader = new BufferedReader(in);
		int l = clPtr[pos];
		for(String line = reader.readLine(); line != null; line = reader.readLine()){			
			final String[] split = PATTERN.split(line);			
			for(int i = 0; i < split.length; i++){
				final int index = Integer.parseInt(split[i]);
				clElements = set(clElements,l++,index);
				clIdx = set(clIdx,index,pos);
			}
			clPtr = set(clPtr,++pos,l);
		}
		reader.close();
		return pos;
	}
	
	private static int[] set(int[] ar, int pos, int val){
		if(ar.length <= pos) ar = grow(ar, pos+1);
		ar[pos] = val;
		return ar;
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
	public Cluster<Integer> getCluster(Integer e) {
		return new ArrayCluster(clIdx[e]);
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
				return new ArrayCluster(i++);
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
			return new IntArrayIterator(clElements, s, t);
		}

		@Override
		public int size() {
			return t-s;
		}
		
		@Override
		public boolean contains(Object o) {
			if(o == null || !(o instanceof Integer))
				return false;
			
			int val = (Integer) o;
			return val >= 0 && val < end && clIdx[val] == id;
		}
		
		@Override
		public int hashCode() {
			return id;
		}
		
		@Override
		public boolean equals(Object o) {
			if(o instanceof ArrayCluster){
				ArrayCluster other = (ArrayCluster) o;
				return id == other.id;
				//TODO check if same clustering
			}
			return false;
		}

		@Override
		public Cluster<Integer> not() {
			return new InverseArrayCluster(id);
		}

	}
	
	private class InverseArrayCluster extends ArrayCluster {
		
		InverseArrayCluster(int id) {
			super(id);
		}
		
		@Override
		public Iterator<Integer> iterator() {
			return new ConcatenatedIterator<Integer>(
					new IntArrayIterator(clElements, 0, s),
					new IntArrayIterator(clElements, t, end));
		}
		
		@Override
		public int size() {
			return end - super.size();
		}
		
		@Override
		public boolean contains(Object o) {
			if(o == null || !(o instanceof Integer))
				return false;
			
			int val = (Integer) o;
			return val >= 0 && val < end && clIdx[val] != id;
		}

		@Override
		public boolean equals(Object o) {
			if(o instanceof InverseArrayCluster){
				InverseArrayCluster other = (InverseArrayCluster) o;
				return id == other.id;
				//TODO check if same clustering
			}
			return false;
		}
		
		@Override
		public Cluster<Integer> not() {
			return new ArrayCluster(id);
		}
		
	}
	
}
