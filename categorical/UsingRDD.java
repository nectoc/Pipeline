package pipeline.test.categorical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 10/30/15.
 */
public class UsingRDD extends Transformer {

	//Contains regex patterns and labels
	Map<String, String> regMap;
	String defaultLabel;
	//String featureName;
	//String[] wordList;
	int index;
	//Map<String,String>map;

	@Override public DataFrame transform(DataFrame dataFrame) {

		//SparkConf conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		//JavaSparkContext scn = new JavaSparkContext(conf);

		//Initializing SQL Context
		//SparkInitializer si = SparkInitializer.getInstance();
		//JavaSparkContext scn = si.initialize();
		//SQLContext sqlContext = new SQLContext(scn);

		System.out.println("Enterrr");
		StructType schema = dataFrame.schema();
		JavaRDD<Row> rowRDD = dataFrame.javaRDD();
		final Map<String, String>regMap = new HashMap<String, String>();
		regMap.put(".*?\\bTHEFT\\b.*?", "theft");
		defaultLabel = "default";
		index = 2;

		JavaRDD<Row>changedRows = rowRDD.map(new Function<Row, String[]>() {
			public String[] call(Row row) throws Exception {
				String input = row.toString();
				String[] output = input.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
				return output;
			}
		}).map(new Function<String[], Row>() {

			public Row call(String[] wordList) throws Exception {

				boolean isLabeled = false;
				for (Map.Entry<String, String> entry : regMap.entrySet()) {
					Pattern pattern = Pattern.compile(entry.getKey());
					Matcher matcher = pattern.matcher(wordList[index]);
					System.out.println("ABCD");

					if (wordList[index] != null) {
						System.out.println("Entering");

						if (matcher.find()) {
							System.out.println("Found");
							isLabeled = true;
							wordList[index] = entry.getValue();
							System.out.println("Changed : "+ wordList[index]);
						}
						System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
				}
				if(isLabeled ==false) {
					wordList[index] = defaultLabel;
				}
				return RowFactory.create((Object[]) wordList);
			}
		});

		//convert row rdd to a data frame
		changedRows.count();

		System.out.println("After adding dataframe");
		DataFrame frame = ContextInitializer.getSql().createDataFrame(changedRows, schema);
		frame.show();

		return frame;
	}

	@Override public StructType transformSchema(StructType structType)
	{
		System.out.println("Schema transformation");
		return structType;
	}

	public Transformer inputVals(Map<String, String> regMap,int index){
		this.regMap = regMap;
		this.index = index;
		return this;
	}

	@Override public Transformer copy(ParamMap paramMap) {
		return null;
	}

	public String uid()
	{
		return String.valueOf(Math.random());
	}
}
