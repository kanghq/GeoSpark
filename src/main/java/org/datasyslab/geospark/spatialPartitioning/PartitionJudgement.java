/**
 * FILE: PartitionJudgement.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.PartitionJudgement.java
 * Copyright (c) 2017 Arizona State University Data Systems Lab
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import com.hqkang.SparkApp.geom.MBRRDDKey;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;


import scala.Tuple2;

// TODO: Auto-generated Javadoc
/**
 * The Class PartitionJudgement.
 */
public class PartitionJudgement implements Serializable{
	
	
	/**
	 * Gets the partition ID.
	 *
	 * @param grids the grids
	 * @param spatialObject the spatial object
	 * @return the partition ID
	 * @throws Exception the exception
	 */
	public static Iterator<Tuple2<Integer, Object>> getPartitionID(List<Envelope> grids,Object spatialObject) throws Exception
	{
		HashSet<Tuple2<Integer, Object>> result = new HashSet<Tuple2<Integer, Object>>();
		int overflowContainerID=grids.size();
		boolean containFlag=false;
		int startTime = ((MBRRDDKey)((Geometry) spatialObject).getUserData()).getStartTime();
		int endTime = ((MBRRDDKey)((Geometry) spatialObject).getUserData()).getEndTime();
		for (int datePrefix = startTime; datePrefix <= endTime; datePrefix++) {
			for (int gridId = 0; gridId < grids.size(); gridId++) {
				if (grids.get(gridId).covers(((Geometry) spatialObject).getEnvelopeInternal())) {
					int newGridId = (String.valueOf(gridId).length()*10)*datePrefix+gridId;
					result.add(new Tuple2<Integer, Object>(newGridId, spatialObject));
					containFlag = true;
				} else if (grids.get(gridId).intersects(((Geometry) spatialObject).getEnvelopeInternal())
						|| ((Geometry) spatialObject).getEnvelopeInternal().covers(grids.get(gridId))) {
					int newGridId = (String.valueOf(gridId).length()*10)*datePrefix+gridId;

					result.add(new Tuple2<Integer, Object>(newGridId, spatialObject));
					// containFlag=true;
				}
			}
			if (containFlag == false) {
				int newOverflowContainerID = (String.valueOf(overflowContainerID).length()*10)*datePrefix+overflowContainerID;
				result.add(new Tuple2<Integer, Object>(newOverflowContainerID, spatialObject));
			}
		}
		return result.iterator();
	}
}
