package com.innovation.spark_anti_patterns;

import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
//import org.apache.spark.sql.SparkSession;
//import scala.collection.JavaConversions;

import java.util.HashSet;
import java.util.Set;

public class DuplicateDAGListener extends SparkListener {
	private Set<Integer> seenPlanDescriptions = new HashSet<>();
	
	@Override
	public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
		System.out.println("Application started: " + applicationStart.appId());
	}
	
	@Override
	public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
		System.out.println("Application ended: " + applicationEnd.hashCode());
	}
	
	@Override
	public void onJobStart(SparkListenerJobStart jobStart) {
		System.out.println("Job started: " + jobStart.jobId());
	}
	
	@Override
	public void onJobEnd(SparkListenerJobEnd jobEnd) {
		System.out.println("Job ended: " + jobEnd.jobId());
	}
	
	@Override
	public void onStageCompleted(SparkListenerStageCompleted stageCompleted) {
		String planDescription = stageCompleted.stageInfo().name();
		String planDetails = stageCompleted.stageInfo().details();
		
		int planHash = planDetails.hashCode();
		
		// Detect duplicate plan description
		if (seenPlanDescriptions.contains(planHash)) {
			System.out.print("Duplicate DAG detected: " + planDescription);
			System.out.println(", plan hash: " + planHash);
		}
		else {
			seenPlanDescriptions.add(planHash);
		}
	}
}
