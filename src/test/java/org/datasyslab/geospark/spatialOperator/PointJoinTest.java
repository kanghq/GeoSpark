/**
 * FILE: PointJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PointJoinTest.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * 
 * @author Arizona State University DataSystems Lab
 *
 */

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;

import scala.Tuple2;


// TODO: Auto-generated Javadoc
/**
 * The Class PointJoinTest.
 */
public class PointJoinTest {
    
    /** The sc. */
    public static JavaSparkContext sc;
    
    /** The prop. */
    static Properties prop;
    
    /** The input. */
    static InputStream input;
    
    /** The Input location. */
    static String InputLocation;
    
    /** The Input location query window. */
    static String InputLocationQueryWindow;
    
    /** The Input location query polygon. */
    static String InputLocationQueryPolygon;
    
    /** The offset. */
    static Integer offset;
    
    /** The splitter. */
    static FileDataSplitter splitter;
    
    /** The grid type. */
    static GridType gridType;
    
    /** The index type. */
    static IndexType indexType;
    
    /** The num partitions. */
    static Integer numPartitions;

    /** The conf. */
    static SparkConf conf;
    
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	conf = new SparkConf().setAppName("PointJoin").setMaster("local[2]");
        sc = new JavaSparkContext(conf);
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);
        prop = new Properties();
        input = PointJoinTest.class.getClassLoader().getResourceAsStream("point.test.properties");
        InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource("primaryroads.csv").getPath();
        offset = 0;
        splitter = null;
        gridType = null;
        indexType = null;
        numPartitions = 0;

        try {
            // load a properties file
            prop.load(input);          
            InputLocation = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("inputLocation")).getPath();
            InputLocationQueryWindow = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("queryWindowSet")).getPath();
            InputLocationQueryPolygon = "file://"+PointJoinTest.class.getClassLoader().getResource(prop.getProperty("queryPolygonSet")).getPath();
            offset = Integer.parseInt(prop.getProperty("offset"));
            splitter = FileDataSplitter.getFileDataSplitter(prop.getProperty("splitter"));
            gridType = GridType.getGridType(prop.getProperty("gridType"));
            indexType = IndexType.getIndexType(prop.getProperty("indexType"));
            numPartitions = Integer.parseInt(prop.getProperty("numPartitions"));

        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    
    
    /**
     * Test spatial join query with rectangle RDD.
     *
     * @throws Exception the exception
     */
    /*
    @Test(expected = NullPointerException.class)
    public void testSpatialJoinQueryUsingIndexException() throws Exception {
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Envelope, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }

    }
    */
    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDD() throws Exception {
    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDD() throws Exception {

        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }
    
    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDDUsingRtreeIndex() throws Exception {
    	
        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.RTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingRTreeIndex() throws Exception {
    	
        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.RTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithRectangleRDDUsingQuadtreeIndex() throws Exception {

        RectangleRDD queryRDD = new RectangleRDD(sc, InputLocationQueryWindow, offset, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
  
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSpatialJoinQueryWithPolygonRDDUsingQuadTreeIndex() throws Exception {
    	
        PolygonRDD queryRDD = new PolygonRDD(sc, InputLocationQueryPolygon, splitter, true, numPartitions);

        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true, numPartitions);
        
        spatialRDD.spatialPartitioning(gridType);
        
        spatialRDD.buildIndex(IndexType.QUADTREE, true);
        
        queryRDD.spatialPartitioning(spatialRDD.grids);
        
        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();
        
        assert result.get(1)._1().getUserData()!=null;
        for(int i=0;i<result.size();i++)
        {
        	if(result.get(i)._2().size()!=0)
        	{
        		assert result.get(i)._2().iterator().next().getUserData()!=null;
        	}
        }
    }
    
    
}