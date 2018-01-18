/**
 * FILE: GeometryByPolygonJudgementUsingIndex.java
 * PATH: org.datasyslab.geospark.joinJudgement.GeometryByPolygonJudgementUsingIndex.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import com.google.gson.Gson;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import com.vividsolutions.jts.io.WKTReader;

import scala.Tuple2;

/**
 * The Class GeometryByPolygonJudgementUsingIndex.
 */
public class DBGeometryByPolygonJudgementUsingIndex implements PairFlatMapFunction<Iterator<Tuple2<Integer, Object>>, Polygon, HashSet<Geometry>>, Serializable{
	
	/** The consider boundary intersection. */
	boolean considerBoundaryIntersection=false;
	private int DBNum;
	
	/**
	 * Instantiates a new geometry by polygon judgement using index.
	 *
	 * @param considerBoundaryIntersection the consider boundary intersection
	 */
	public DBGeometryByPolygonJudgementUsingIndex(boolean considerBoundaryIntersection,int DBNum)
	{
		this.considerBoundaryIntersection = considerBoundaryIntersection;
		this.DBNum = DBNum;
	}
	/* (non-Javadoc)
	 * @see org.apache.spark.api.java.function.PairFlatMapFunction#call(java.lang.Object)
	 */
	@Override
	public Iterator<Tuple2<Polygon, HashSet<Geometry>>> call(Iterator<Tuple2<Integer, Object>> cogroup) throws Exception {
		HashSet<Tuple2<Polygon, HashSet<Geometry>>> result = new HashSet<Tuple2<Polygon, HashSet<Geometry>>>();
        //Iterator<SpatialIndex> iteratorTree=cogroup._2()._1().iterator();

//        if(!iteratorTree.hasNext())
//        {
//        	return result.iterator();
//        }
//        SpatialIndex treeIndex = iteratorTree.next();
//        if(treeIndex instanceof STRtree)
//        {
//        	treeIndex = (STRtree)treeIndex;
//        }
//        else
//        {
//        	treeIndex = (Quadtree)treeIndex;
//        }
		HashMap<Integer, Connection> conMap = new HashMap<Integer, Connection>();
        while(cogroup.hasNext()) {
        	Connection con = null;
        	Tuple2<Integer, Object> element = cogroup.next();
        	int route = element._1%DBNum;
        	
        	if(conMap.get(route) != null) {
        		con = conMap.get(route);
        	} else {
        		con = DriverManager.getConnection("jdbc:neo4j:bolt://"+"DBSRV"+route, "spark", "25519173");
        		conMap.put(route, con);
        		        	}
        	Polygon window=(Polygon)element._2;  
            List<Geometry> queryResult=new ArrayList<Geometry>();
            //queryResult=treeIndex.query(window.getEnvelopeInternal());
            //DB Query
            String queryStr = window.toText();
			String query = "CALL spatial.intersects('"+element._1()+"','" + queryStr
					+ "') YIELD node RETURN node";
			try (PreparedStatement stmt = con.prepareStatement(query)) {
				con.setAutoCommit(false);
				try (ResultSet rs = stmt.executeQuery()) {
					con.commit();
					while(rs.next()) {
						HashMap<String, String> node = (HashMap<String, String>) rs.getObject(1);
						String json = node.get("MBRRDDKey");
						WKTReader reader = new WKTReader();
						Geometry obj = reader.read(node.get("wkt"));
						MBRRDDKey key = Gson.class.newInstance().fromJson(json, MBRRDDKey.class);

						obj.setUserData(key);
						
						queryResult.add(obj);
						
					}
				} catch(Exception e) {
					e.printStackTrace();
				} finally{
					con.setAutoCommit(true);
				}
			}
            
            
            if(queryResult.size()==0) continue;
            HashSet<Geometry> objectHashSet = new HashSet<Geometry>();
            
            for(Geometry spatialObject:queryResult)
            {
				// Refine phase. Use the real polygon (instead of its MBR) to recheck the spatial relation.
            	if(considerBoundaryIntersection)
            	{
            		//if (window.intersects(spatialObject)) {
            		if (true) {
            			objectHashSet.add(spatialObject);
            		}
            	}
            	else{
            		if (window.covers(spatialObject)) {
            			objectHashSet.add(spatialObject);
            		}
            	}
            }
            if(objectHashSet.size()==0) continue;
            result.add(new Tuple2<Polygon, HashSet<Geometry>>(window, objectHashSet));   
        }
        for(Connection conn:conMap.values()) {
        	conn.close();
        }
        return result.iterator();
    }
}
