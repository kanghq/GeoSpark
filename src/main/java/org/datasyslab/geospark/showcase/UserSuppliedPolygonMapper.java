/**
 * FILE: UserSuppliedPolygonMapper.java
 * PATH: org.datasyslab.geospark.showcase.UserSuppliedPolygonMapper.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.showcase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.WKTReader;

/**
 * The Class UserSuppliedPolygonMapper.
 */
public class UserSuppliedPolygonMapper implements FlatMapFunction<String, Geometry>{
	
	/** The result. */
	List result= new ArrayList<Polygon>();
    
    /** The spatial object. */
    Geometry spatialObject = null;
    
    /** The multi spatial objects. */
    MultiPolygon multiSpatialObjects = null;
    
    /** The fact. */
    GeometryFactory fact = new GeometryFactory();
    
    /** The line split list. */
    List<String> lineSplitList;
    
    /** The coordinates list. */
    ArrayList<Coordinate> coordinatesList;
    
    /** The coordinates. */
    Coordinate[] coordinates;
    
    /** The linear. */
    LinearRing linear;
    
    /** The actual end offset. */
    int actualEndOffset;
    
    /* (non-Javadoc)
     * @see org.apache.spark.api.java.function.FlatMapFunction#call(java.lang.Object)
     */
    public Iterator call(String line) throws Exception {
        List result= new ArrayList<Polygon>();
        Geometry spatialObject = null;
        MultiPolygon multiSpatialObjects = null;
        List<String> lineSplitList;
        lineSplitList=Arrays.asList(line.split("\t"));
        String newLine = lineSplitList.get(0).replace("\"", "");
        WKTReader wktreader = new WKTReader();
        spatialObject = wktreader.read(newLine);
        if(spatialObject instanceof MultiPolygon)
        {
        	multiSpatialObjects = (MultiPolygon) spatialObject;
        	for(int i=0;i<multiSpatialObjects.getNumGeometries();i++)
        	{
                		spatialObject = multiSpatialObjects.getGeometryN(i);
                		result.add((Polygon) spatialObject);
                	}
        }
        else
        {
        	result.add((Polygon)spatialObject);
        }
        return result.iterator();
    }

}
