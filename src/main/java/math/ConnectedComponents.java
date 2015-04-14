/**
 * 
 */
package math;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * computes weak connected components
 * 
 * @author Cedrik
 *
 */
public class ConnectedComponents {
	
	private final Map<Integer,List<Integer>> edge_set;
	private final Set<Integer> visited;
	
	public ConnectedComponents(Map<Integer,Integer> edges){
		edge_set = new LinkedHashMap<>(edges.size()*2);
		for(Entry<Integer,Integer> e : edges.entrySet()){
			addEdge(e.getKey(), e.getValue(), true);
		}
		
		visited = new HashSet<>(edge_set.keySet().size());
	}

	public Map<Integer,List<Integer>> compute(){
		Map<Integer,List<Integer>> results = new LinkedHashMap<Integer, List<Integer>>();
		
		for(Integer node : edge_set.keySet()){
			if(visited.contains(node)){
				continue;
			}
			List<Integer> list = depthFirstSearch(node);
			results.put(node, list);
		}
		
		return results;
	}
	
	private List<Integer> depthFirstSearch(Integer node){
		visited.add(node);
		List<Integer> result = new LinkedList<>();
		
		for(Integer child : edge_set.get(node)){
			if(visited.contains(child)){
				continue;
			}
			
			result.add(child);
			result.addAll(depthFirstSearch(child));
		}		
		
		return result;
	}
	
	private void addEdge(Integer i1, Integer i2, boolean symmetric){
		if(symmetric){
			addEdge(i2, i1, false);
		}
		List<Integer> list = edge_set.get(i1);
		if(list == null){
			list = new LinkedList<>();
			edge_set.put(i1, list);
		}
		list.add(i2);
	}
	
}
