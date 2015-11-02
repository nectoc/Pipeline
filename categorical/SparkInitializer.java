package pipeline.test.categorical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 10/30/15.
 */
public class SparkInitializer {

	private static SparkInitializer instance = null;

	private SparkInitializer(){

	}

	public static SparkInitializer getInstance(){

		if(instance == null) {
			instance = new SparkInitializer();
		}
		return instance;
	}
/*
	public static SparkConf initializer() {

		SparkConf conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		//JavaSparkContext scn = new JavaSparkContext(conf);

		//Initializing SQL Context
		//SQLContext sqlContext;
		//sqlContext = new SQLContext(scn);

		return conf;
	}*/

	public JavaSparkContext initialize(){
		SparkConf conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		JavaSparkContext scn = new JavaSparkContext(conf);

		return scn;
	}

}
