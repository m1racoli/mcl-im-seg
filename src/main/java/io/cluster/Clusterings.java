/**
 * 
 */
package io.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * utilities for clusterings
 * 
 * @author Cedrik
 *
 */
public class Clusterings {

	public static double avgSize(Clustering<?> clustering) {
		double sum = 0.0;
		for(Cluster<?> cluster : clustering){
			sum += cluster.size();
		}
		return sum / clustering.size();
	}
	
	@SuppressWarnings("unused")
	public static void testInverse(Clustering<?> clustering){
		Integer total = null;
		for(Cluster<?> cluster : clustering){
			int size = cluster.size();
			int cnt = 0;
			for(Object e : cluster){
				cnt++;
			}
			if(cnt != size){
				System.err.printf("size != cnt: %d != %d\n",size,cnt);
			}
			
			Cluster<?> inv = cluster.not();
			int inv_size = inv.size();
			int inv_cnt = 0;
			for(Object e : inv){
				inv_cnt++;
			}
			if(inv_cnt != inv_size){
				System.err.printf("inv_size != inv_cnt: %d != %d\n",inv_size,inv_cnt);
			}
			
			if(total == null){
				total = size + inv_size;
			} else {
				if(total.compareTo(size + inv_size) != 0){
					System.err.printf("total inconsistent: %d != %d\n",total,size + inv_size);
				}
			}
		}
	}
	
	public static double nCut(Clustering<Integer> clustering, final Map<Integer,Map<Integer,Double>> m,int te){
		ExecutorService executorService = Executors.newFixedThreadPool(te);
		List<Future<Double>> results = new ArrayList<Future<Double>>(clustering.size());
		
		for(final Cluster<Integer> cl : clustering){
			results.add(executorService.submit(new Callable<Double>() {

				@Override
				public Double call() throws Exception {
					return nassoc(cl, m);
				}
			}));
		}
		
		double ncut = 0.0;
		for(Future<Double> result : results){
			try {
				ncut += result.get();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(1);
			}
		}	
		
		return clustering.size() - ncut;
	}
	
	public static double nCut(Clustering<Integer> clustering, Map<Integer,Map<Integer,Double>> m){
		double ncut = 0.0;
		for(Cluster<Integer> cl : clustering){
//			ncut += nCut(cl, raster);
			ncut += nassoc(cl, m);
		}
		return clustering.size() - ncut;
	}
	
	public static double nassoc(Cluster<Integer> cl, Map<Integer,Map<Integer,Double>> m){
		return cut(cl, cl, m)/cut(cl, m);
	}
	
	public static double nCut(Cluster<Integer> cl, Map<Integer,Map<Integer,Double>> m){
		System.err.println("buggy ncut version!!!");
		//TODO
		return cut(cl, cl.not(), m)/cut(cl, m);
	}
	
	public static double cut(Cluster<Integer> cl, Map<Integer,Map<Integer,Double>> m){
		double cut = 0.0;
		for(Integer p : cl)
			cut += cut(p, m);
		return cut;
	}
	
	public static double cut(Cluster<Integer> cl1, Cluster<Integer> cl2, Map<Integer,Map<Integer,Double>> m){
		double cut = 0.0;
		for(Integer p : cl2){
			cut += cut(p, cl1, m);
		}
		return cut;
	}
	
	public static double cut(Integer p, Map<Integer,Map<Integer,Double>> m){
		double cut = 0.0;
		Map<Integer,Double> map = m.get(p);
		if(map == null){
			return 0.0;
		}
		for(Double v : map.values())
			cut += v;		
		return cut;
	}	
	
	public static double cut(Integer p, Cluster<Integer> cl, Map<Integer,Map<Integer,Double>> m){
		double cut = 0.0;
		Map<Integer,Double> map = m.get(p);
		if(map == null){
			return 0.0;
		}
		for(Integer p2 : cl){
			Double v = map.get(p2);
			if(v != null)
				cut += v;
		}
		return cut;
	}
	
}
