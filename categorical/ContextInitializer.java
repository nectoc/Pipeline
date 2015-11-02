package pipeline.test.categorical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 11/1/15.
 */
public class ContextInitializer {

	private static SparkConf sc;
	private static JavaSparkContext jsc;
	private static  SQLContext sql;

	static{
		sc = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		jsc = new JavaSparkContext(sc);
		sql =  new SQLContext(jsc);
	}

	public static SparkConf getSc() {
		return sc;
	}

	public static JavaSparkContext getJsc() {
		return jsc;
	}

	public static SQLContext getSql() {
		return sql;
	}
}
