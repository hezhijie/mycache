package net.mycache.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class RouteSet {
    private int[] weights;
	private final static int DEFAULT_READ_WEIGHT = 1;
	private List<ServerInfo> infos;
	private List<RouteData> routes;
	
	private void initWeights() {
		weights = new int[infos.size()];
		int prev = 0;
        for (int i = 0; i < infos.size(); i++) {
            weights[i] = prev + DEFAULT_READ_WEIGHT;
            prev = weights[i];
        }
	}
	
	public RouteSet(List<ServerInfo> infos, List<RouteData> routes) {
		this.infos = infos;
		this.routes = routes;
		this.weights = new int[infos.size()];
		initWeights();
	}
	
	public RouteSet(List<ServerInfo> infos) {
		this.infos = infos;
		routes = new ArrayList<RouteData> ();
		for (ServerInfo info : infos) {
			routes.add(new RouteData(info));
		}
		
		weights = new int[infos.size()];
		initWeights();
	}
	
	public void removeRouteData(RouteData routeData) {
		int index = 0;
		Iterator<RouteData> iter = routes.iterator();  
		while (iter.hasNext()) {  
			if (iter.next().equals(routeData)) {
		        iter.remove(); 
		        break;
			}
			index++;
		} 
		
		infos.remove(index);
		
		int[] newWeights = new int[weights.length - 1];
		for (int i = 0, j = 0; i < weights.length; i++) {
			if (i == index) {
				continue;
			} else {
				newWeights[j] = weights[i];
				j++;
			}
		}
		weights = newWeights;
		
		routeData.destroy();
	}
	
    public int[] getWeights() {
		return weights;
	}

	public void setWeights(int[] weights) {
		this.weights = weights;
	}
	
	public List<RouteData> getRoutes() {
		return routes;
	}
	
	public void destroy() {
		if (routes != null) {
			for (RouteData route : routes) {
				route.destroy();
			}
		}
	}

	public List<ServerInfo> getInfos() {
		return infos;
	}

	public void setInfos(List<ServerInfo> infos) {
		this.infos = infos;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
    	builder.append("{");
		builder.append("weights:");
		builder.append(Arrays.toString(weights));
		builder.append(" serverInfos:");
		builder.append(infos);
		builder.append(",routeDatas:");
		builder.append(routes);
    	builder.append("}");
		return builder.toString();
	}
}
