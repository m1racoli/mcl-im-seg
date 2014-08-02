/**
 * 
 */
package io.writables;

import java.util.Iterator;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;

import util.ReadOnlyIterator;

/**
 * @author Cedrik
 *
 */
@SuppressWarnings("rawtypes")
public abstract class MCLSingleColumnMatrixSlice<V extends MCLSingleColumnMatrixSlice> extends MCLMatrixSlice<V,FloatWritable> {
	
	public void init(final Index idx, final Iterable<Feature> features){
		clear();
		contruct(idx.row, new Iterable<Float>(){

			@Override
			public Iterator<Float> iterator() {
				return new ValueIterator(idx,features.iterator());
			}
			
		});
	}
	
	private final class ValueIterator extends ReadOnlyIterator<Float>{

		private final Index idx;
		private final Iterator<Feature> iter;
		
		private ValueIterator(Index idx, Iterator<Feature> iter){
			this.idx = idx;
			this.iter = iter;
		}
		
		@Override
		public boolean hasNext() {
			return iter.hasNext();
		}

		@SuppressWarnings("unchecked")
		@Override
		public Float next() {
			
			if(idx.isDiagonal()){
				iter.next();
				return 1.0f;
			}
			
			return iter.next().dist(iter.next()); //TODO other comparator
		}
		
	}
	
	public abstract void contruct(LongWritable row, Iterable<Float> values);

}
