/**
 * FILE: SpatialRDD.java
 * PATH: org.datasyslab.geospark.spatialRDD.SpatialRDD.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialPartitioning.EqualPartitioning;
import org.datasyslab.geospark.spatialPartitioning.HilbertPartitioning;
import org.datasyslab.geospark.spatialPartitioning.PartitionJudgement;
import org.datasyslab.geospark.spatialPartitioning.RtreePartitioning;
import org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner;
import org.datasyslab.geospark.spatialPartitioning.VoronoiPartitioning;
import org.datasyslab.geospark.utils.RDDSampleUtils;
import org.datasyslab.geospark.utils.XMaxComparator;
import org.datasyslab.geospark.utils.XMinComparator;
import org.datasyslab.geospark.utils.YMaxComparator;
import org.datasyslab.geospark.utils.YMinComparator;
import org.wololo.geojson.GeoJSON;
import org.wololo.jts2geojson.GeoJSONWriter;

import com.google.gson.Gson;
import com.hqkang.SparkApp.core.CommonHelper;
import com.hqkang.SparkApp.core.DBHelper;
import com.hqkang.SparkApp.geom.MBR;
import com.hqkang.SparkApp.geom.MBRKey;
import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;

import scala.Tuple2;

/**
 * The Class SpatialRDD.
 */
public abstract class DBSpatialRDD extends SpatialRDD implements Serializable{
	
	/** The Constant logger. */
	final static Logger logger = Logger.getLogger(DBSpatialRDD.class);
    
    /** The total number of records. */
    public long totalNumberOfRecords=-1;
    
    /** The boundary. */
    public Double[] boundary = new Double[4];
    
    /** The boundary envelope. */
    public Envelope boundaryEnvelope = null;
    
    /** The spatial partitioned RDD. */
    public JavaPairRDD<Integer, Object> spatialPartitionedRDD;
    
    /** The indexed RDD. */
    public JavaPairRDD<Integer, SpatialIndex> indexedRDD;
    
    /** The indexed raw RDD. */
    public JavaRDD<Object> indexedRawRDD;
    
    /** The raw spatial RDD. */
    public JavaRDD<Object> rawSpatialRDD;

	/** The grids. */
    public List<Envelope> grids;
    
    public HashMap<Integer, Connection> serverList = new HashMap<Integer, Connection>();
    
    public int serverNumber;
    
	/**
	 * Spatial partitioning.
	 *
	 * @param gridType the grid type
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean spatialPartitioning(GridType gridType,int srvNum) throws Exception
	{
		
		
        int numPartitions = this.rawSpatialRDD.rdd().partitions().length;;
		if(this.boundaryEnvelope==null)
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD boundary is null. Please call boundary() first.");
        }
        if(this.totalNumberOfRecords==-1)
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] SpatialRDD volume is unkown. Please call count() first.");
        }
		//Calculate the number of samples we need to take.
        int sampleNumberOfRecords = RDDSampleUtils.getSampleNumbers(numPartitions, this.totalNumberOfRecords);
        //Take Sample
        ArrayList objectSampleList = new ArrayList(this.rawSpatialRDD.takeSample(false, sampleNumberOfRecords));
        //Sort
        if(gridType == GridType.EQUALGRID)
        {
        	EqualPartitioning EqualPartitioning=new EqualPartitioning(this.boundaryEnvelope,numPartitions);
        	grids=EqualPartitioning.getGrids();
        }
        else if(gridType == GridType.HILBERT)
        {
        	HilbertPartitioning hilbertPartitioning=new HilbertPartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=hilbertPartitioning.getGrids();
        }
        else if(gridType == GridType.RTREE)
        {
        	RtreePartitioning rtreePartitioning=new RtreePartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=rtreePartitioning.getGrids();
        }
        else if(gridType == GridType.VORONOI)
        {
        	VoronoiPartitioning voronoiPartitioning=new VoronoiPartitioning(objectSampleList,this.boundaryEnvelope,numPartitions);
        	grids=voronoiPartitioning.getGrids();
        }
        else
        {
        	throw new Exception("[AbstractSpatialRDD][spatialPartitioning] Unsupported spatial partitioning method.");
        }
        JavaPairRDD<Integer, Object> spatialNumberingRDD = this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<Object, Integer, Object>() {
                    @Override
                    public Iterator<Tuple2<Integer, Object>> call(Object spatialObject) throws Exception {
                    	return PartitionJudgement.getPartitionID(grids,spatialObject);
                    }
                }
        );
        
        
        
       
        List<Integer> keys = spatialNumberingRDD.keys().distinct().collect();
        
        
        
        
		SparkContext sc = spatialNumberingRDD.context();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

		final Broadcast<Integer> BsrvNum = jsc.broadcast(srvNum);
        this.spatialPartitionedRDD = spatialNumberingRDD.partitionBy(new SpatialPartitioner(grids.size()));
        /*
        List<Integer> layerList = spatialPartitionedRDD.keys().distinct().collect();
        
        for(int k=0;k<srvNum;k++) {
			try{
			Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://"+"DBSRV"+k, "neo4j", "25519173");
        	serverList.put(k, con);
			} catch(Exception e) {
				e.printStackTrace();
				
			}
        	
        }
        
		for(Integer i:layerList) {
			int cal =i;
		
			

			Connection con = serverList.get(cal%srvNum);
			

		
			
			String addLq = "call spatial.addWKTLayer('"+cal+"','wkt')";
			
			DBHelper.retry(0,3,con,addLq);
			
		}
		for(Connection conn:serverList.values()) {
			try{
			conn.close();
			} catch(Exception e) {
				e.printStackTrace();
				
			}
        	
        }
		*/
		
		
		
        
        spatialPartitionedRDD.keys().distinct().foreachPartition(new VoidFunction<Iterator<Integer>>() {
        	

			@Override
			public void call(Iterator<Integer> t) throws Exception {
				// TODO Auto-generated method stub
				Integer srvNum = BsrvNum.getValue();
				
				for(int k=0;k<srvNum;k++) {
					try{
					Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://"+"DBSRV"+k, "spark", "25519173");
		        	serverList.put(k, con);
					} catch(Exception e) {
						e.printStackTrace();
						
					}
		        	
		        }
				while(t.hasNext()) {
					int cal = t.next();
				
					

					Connection con = serverList.get(cal%srvNum);
					//Polygon pol = ele.shape();
					
					

				
					
					String addLq = "call spatial.addWKTLayer('"+cal+"','wkt')";
					
					//String query = "CALL spatial.addWKTWithProperties('"+tu._1()+"','" + pol.toText() + "',"
					//		+ CommonHelper.conString(property) + "," + CommonHelper.conString(propertyField) + ")";
					
					
					DBHelper.retry(0,3,con,addLq);
					//DBHelper.retry(0,3,con,query);
					
					/*
					try{
					con.setAutoCommit(false);;
					PreparedStatement stmtAddL = con.prepareStatement(addLq);
					stmtAddL.execute();
					con.commit();
					} catch(Exception e) {
						e.printStackTrace();
						if(e.getMessage().contains("existing layer"))
						{
							System.err.println("existing layer");
						} else {
							e.printStackTrace();
							try{
							con.commit();
							} catch(Exception e2){
								e.printStackTrace();
							}
						}
						//con.rollback();
						
						
					} finally{con.setAutoCommit(true);}
					/*
					try{
					con.setAutoCommit(false);
					PreparedStatement stmt = con.prepareStatement(query);

					stmt.execute();
					con.commit();
					} 
					
					 catch(Exception e) {
						e.printStackTrace();
						con.rollback();
					} finally{con.setAutoCommit(true);}
					*/ 
				}
				
				for(Connection conn:serverList.values()) {
					try{
					conn.close();
					} catch(Exception e) {
						e.printStackTrace();
						
					}
		        	
		        }
			

			}
				
		});
		
        
        spatialPartitionedRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Object>>>() {
        	

			@Override
			public void call(Iterator<Tuple2<Integer, Object>> t) throws Exception {
				// TODO Auto-generated method stub
				Integer srvNum = BsrvNum.getValue();
				
				for(int k=0;k<srvNum;k++) {
					try{
					Connection con = DriverManager.getConnection("jdbc:neo4j:bolt://"+"DBSRV"+k, "spark", "25519173");
		        	serverList.put(k, con);
					} catch(Exception e) {
						e.printStackTrace();
						
					}
		        	
		        }
				while(t.hasNext()) {
					Tuple2<Integer, Object> tu = t.next();
					int cal = tu._1;
				
					Geometry ele = (Geometry) tu._2;
					MBRRDDKey key = (MBRRDDKey) ele.getUserData();
					

					Connection con = serverList.get(tu._1%srvNum);
					//Polygon pol = ele.shape();
					Polygon pol = (Polygon) ele;
					String[] property = { "MBRRDDKey" };
					

					String json = Gson.class.newInstance().toJson(key);
					String[] propertyField = {json};
					//String addLq = "call spatial.addWKTLayer('"+tu._1()+"','wkt')";
					
					String query = "CALL spatial.addWKTWithProperties('"+tu._1()+"','" + pol.toText() + "',"
							+ CommonHelper.conString(property) + "," + CommonHelper.conString(propertyField) + ")";
					
					
				//	DBHelper.retry(0,3,con,addLq);
					DBHelper.retry(0,3,con,query);

					/*
					try{
					con.setAutoCommit(false);;
					PreparedStatement stmtAddL = con.prepareStatement(addLq);
					stmtAddL.execute();
					con.commit();
					} catch(Exception e) {
						//e.printStackTrace();
						if(e.getMessage().contains("layer"))
						{
							System.err.println("existing layer");
						} else {
							e.printStackTrace();
							try{
							con.commit();
							} catch(Exception e2){
								e.printStackTrace();
							}
						}
						//con.rollback();
						
						
					} finally{con.setAutoCommit(true);}
					
					try{
					con.setAutoCommit(false);
					PreparedStatement stmt = con.prepareStatement(query);

					stmt.execute();
					con.commit();
					} 
					
					 catch(Exception e) {
						e.printStackTrace();
						con.rollback();
					} finally{con.setAutoCommit(true);}
					*/
				}
				
				for(Connection conn:serverList.values()) {
					try{
					conn.close();
					} catch(Exception e) {
						e.printStackTrace();
						
					}
		        	
		        }
			
				
			}
				
		});
       // this.spatialPartitionedRDD = spatialNumberingRDD;
        /*
         * INSERT to DB
         * */
         
        return true;
	}
	
	/**
	 * Spatial partitioning.
	 *
	 * @param otherGrids the other grids
	 * @return true, if successful
	 * @throws Exception the exception
	 */
	public boolean spatialPartitioning(final List<Envelope> otherGrids) throws Exception
	{
        JavaPairRDD<Integer, Object> spatialNumberingRDD = this.rawSpatialRDD.flatMapToPair(
                new PairFlatMapFunction<Object, Integer, Object>() {
                    @Override
                    public Iterator<Tuple2<Integer, Object>> call(Object spatialObject) throws Exception {
                    	return PartitionJudgement.getPartitionID(otherGrids,spatialObject);
                    }
                }
        );
        this.grids = otherGrids;
        this.spatialPartitionedRDD = spatialNumberingRDD.partitionBy(new SpatialPartitioner(grids.size()));
        return true;
	}
	
	/**
	 * Count without duplicates.
	 *
	 * @return the long
	 */
	public long countWithoutDuplicates()
	{

		List collectedResult = this.rawSpatialRDD.collect();
		HashSet resultWithoutDuplicates = new HashSet();
		for(int i=0;i<collectedResult.size();i++)
		{
			resultWithoutDuplicates.add(collectedResult.get(i));
		}
		return resultWithoutDuplicates.size();
	}
	
	/**
	 * Count without duplicates SPRDD.
	 *
	 * @return the long
	 */
	public long countWithoutDuplicatesSPRDD()
	{
		JavaRDD cleanedRDD = this.spatialPartitionedRDD.map(new Function<Tuple2<Integer,Object>,Object>()
		{

			@Override
			public Object call(Tuple2<Integer, Object> spatialObjectWithId) throws Exception {
				return spatialObjectWithId._2();
			}
		});
		List collectedResult = cleanedRDD.collect();
		HashSet resultWithoutDuplicates = new HashSet();
		for(int i=0;i<collectedResult.size();i++)
		{
			resultWithoutDuplicates.add(collectedResult.get(i));
		}
		return resultWithoutDuplicates.size();
	}
	
	/**
	 * Builds the index.
	 *
	 * @param indexType the index type
	 * @param buildIndexOnSpatialPartitionedRDD the build index on spatial partitioned RDD
	 * @throws Exception the exception
	 */
	public void buildIndex(final IndexType indexType,boolean buildIndexOnSpatialPartitionedRDD) throws Exception {
	      if (buildIndexOnSpatialPartitionedRDD==false) {
	    	  //This index is built on top of unpartitioned SRDD
	    	  this.indexedRawRDD =  this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>,Object>()
	    	  {
	    		  @Override
	        	  public Iterator<Object> call(Iterator<Object> spatialObjects) throws Exception {
	        		  if(indexType == IndexType.RTREE)
	        		  {
		        		  STRtree rt = new STRtree();
		        		  while(spatialObjects.hasNext()){
		        			  Geometry spatialObject = (Geometry)spatialObjects.next();
		        			  rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
		        		  }
		        		  HashSet<Object> result = new HashSet<Object>();
		        		  rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        		  result.add(rt);
		        		  return result.iterator();
	        		  }
	        		  else
	        		  {
	        			  Quadtree rt = new Quadtree();
		        		  while(spatialObjects.hasNext()){
		        			  Geometry spatialObject = (Geometry)spatialObjects.next();
		        			  rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
		        		  }
		        		  HashSet<Object> result = new HashSet<Object>();
		        		  rt.query(new Envelope(0.0,0.0,0.0,0.0));
		        		  result.add(rt);
		        		  return result.iterator();
	        		  }

	        	  }
	          	}); 
	        }
	        else
	        {
	        	if(this.spatialPartitionedRDD==null)
	        	{
  				  throw new Exception("[AbstractSpatialRDD][buildIndex] spatialPartitionedRDD is null. Please do spatial partitioning before build index.");
	        	}
	        	JavaPairRDD<Integer, Iterable<Object>> groupBySpatialPartitionedRDD = this.spatialPartitionedRDD.groupByKey();       
//	        	this.indexedRDD = groupBySpatialPartitionedRDD.flatMapValues(new Function<Iterable<Object>, Iterable<SpatialIndex>>() {
//	        		@Override
//	        		public Iterable<SpatialIndex> call(Iterable<Object> spatialObjects) throws Exception {
//	        			if(indexType == IndexType.RTREE)
//	        			{
//		        			STRtree rt = new STRtree();
//		        			Iterator<Object> iterator=spatialObjects.iterator();
//		        			while(iterator.hasNext()){
//		        				Geometry spatialObject = (Geometry)iterator.next();
//		        				rt.insert(spatialObject.getEnvelopeInternal(), spatialObject);
//		        			}
//		        			HashSet<SpatialIndex> result = new HashSet<SpatialIndex>();
//		        			rt.query(new Envelope(0.0,0.0,0.0,0.0));
//		        			result.add(rt);
//		        			return result;
//	        			}
//	        			else
//	        			{
//		        			Quadtree rt = new Quadtree();
//		        			Iterator<Object> iterator=spatialObjects.iterator();
//		        			while(iterator.hasNext()){
//		        				Geometry spatialObject = (Geometry) iterator.next();
//		        				Geometry castedSpatialObject = (Geometry) spatialObject;
//		        				rt.insert(castedSpatialObject.getEnvelopeInternal(), castedSpatialObject);
//		        			}
//		        			HashSet<SpatialIndex> result = new HashSet<SpatialIndex>();
//		        			rt.query(new Envelope(0.0,0.0,0.0,0.0));
//		        			result.add(rt);
//		        			return result;
//	        			}
//	        		}
//	        	}
//	        	);
	        }
	}
	
    /**
     * Boundary.
     *
     * @return the envelope
     */
    public Envelope boundary() {
    	Object minXEnvelope = this.rawSpatialRDD.min(new XMinComparator());
    	Object minYEnvelope = this.rawSpatialRDD.min(new YMinComparator());
    	Object maxXEnvelope = this.rawSpatialRDD.max(new XMaxComparator());
    	Object maxYEnvelope = this.rawSpatialRDD.max(new YMaxComparator());    	
    	this.boundary[0] = ((Geometry) minXEnvelope).getEnvelopeInternal().getMinX();
    	this.boundary[1] = ((Geometry) minYEnvelope).getEnvelopeInternal().getMinY();
    	this.boundary[2] = ((Geometry) maxXEnvelope).getEnvelopeInternal().getMaxX();
    	this.boundary[3] = ((Geometry) maxYEnvelope).getEnvelopeInternal().getMaxY();
        this.boundaryEnvelope =  new Envelope(boundary[0],boundary[2],boundary[1],boundary[3]);
        return this.boundaryEnvelope;
    }
	
	/**
	 * Gets the raw spatial RDD.
	 *
	 * @return the raw spatial RDD
	 */
	public JavaRDD<Object> getRawSpatialRDD() {
		return rawSpatialRDD;
	}

	/**
	 * Sets the raw spatial RDD.
	 *
	 * @param rawSpatialRDD the new raw spatial RDD
	 */
	public void setRawSpatialRDD(JavaRDD<Object> rawSpatialRDD) {
		this.rawSpatialRDD = rawSpatialRDD;
	}
	
	/**
	 * Analyze.
	 *
	 * @param newLevel the new level
	 * @return true, if successful
	 */
	public boolean analyze(StorageLevel newLevel)
	{
		this.rawSpatialRDD.persist(newLevel);
		this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
        return true;
	}
	
	/**
	 * Analyze.
	 *
	 * @return true, if successful
	 */
	public boolean analyze()
	{
		this.boundary();
        this.totalNumberOfRecords = this.rawSpatialRDD.count();
        return true;
	}
	
    /**
     * Save as geo JSON.
     *
     * @param outputLocation the output location
     */
    public void saveAsGeoJSON(String outputLocation) {
        this.rawSpatialRDD.mapPartitions(new FlatMapFunction<Iterator<Object>, String>() {
            @Override
            public Iterator<String> call(Iterator<Object> iterator) throws Exception {
                ArrayList<String> result = new ArrayList<String>();
                GeoJSONWriter writer = new GeoJSONWriter();
                while (iterator.hasNext()) {
                	Geometry spatialObject = (Geometry)iterator.next();
                    GeoJSON json = writer.write(spatialObject);
                    String jsonstring = json.toString();
                    result.add(jsonstring);
                }
                return result.iterator();
            }
        }).saveAsTextFile(outputLocation);
    }
    
    /**
     * Minimum bounding rectangle.
     *
     * @return the rectangle RDD
     */
    @Deprecated
    public RectangleRDD MinimumBoundingRectangle() {
        JavaRDD<Polygon> rectangleRDD = this.rawSpatialRDD.map(new Function<Object, Polygon>() {
            public Polygon call(Object spatialObject) {
        		Double x1,x2,y1,y2;
                LinearRing linear;
                Coordinate[] coordinates = new Coordinate[5];
                GeometryFactory fact = new GeometryFactory();
            	x1 = ((Geometry) spatialObject).getEnvelopeInternal().getMinX();
				x2 = ((Geometry) spatialObject).getEnvelopeInternal().getMaxX();
				y1 = ((Geometry) spatialObject).getEnvelopeInternal().getMinY();
				y2 = ((Geometry) spatialObject).getEnvelopeInternal().getMaxY();
		        coordinates[0]=new Coordinate(x1,y1);
		        coordinates[1]=new Coordinate(x1,y2);
		        coordinates[2]=new Coordinate(x2,y2);
		        coordinates[3]=new Coordinate(x2,y1);
		        coordinates[4]=coordinates[0];
                linear = fact.createLinearRing(coordinates);
                Polygon polygonObject = new Polygon(linear, null, fact);
                return polygonObject;
            }
        });
        return new RectangleRDD(rectangleRDD);
    }
}
