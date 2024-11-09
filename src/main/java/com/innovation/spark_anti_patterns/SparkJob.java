package com.innovation.spark_anti_patterns;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SparkJob {
	public static void main(String args[]) {
		SparkSession spark = SparkSession.builder()
				.appName("DuplicateDAGExample")
				.master("local[*]")
				.getOrCreate();
		spark.sparkContext().addSparkListener(new DuplicateDAGListener());
		
		Dataset<Row> df = spark.createDataFrame(
				java.util.Arrays.asList(
						new Person("Alice", 34),
						new Person("Bob", 35),
						new Person("Chad", 36)
					), Person.class
				);
		
		Dataset<Row> filteredDf1 = df.filter("age > 30");
		System.out.println("Number of people above 30: " + filteredDf1.count());
		

		Dataset<Row> filteredDf2 = df.filter("age > 30");
		System.out.println("Number of people above 30 (second): " + filteredDf2.count());
		
		spark.stop();
	}
	
	public static class Person implements java.io.Serializable {
		private static final long serialVersionUID = 1L;
		private String name;
		private int age;
		
		public Person(String name, int age) {
			this.name = name;
			this.age = age;
		}
		
		public String getName() {
			return name;
		}
		
		public int getAge() {
			return age;
		}
	}

}
