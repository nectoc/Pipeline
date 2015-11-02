package pipeline.test.categorical;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 10/19/15.
 */
public class ReplaceStage extends Transformer {

	//Contains regex patterns and labels
	Map<String, String> regMap;
	String defaultLabel;
	//String featureName;
	//String[] wordList;
	int index;

	/*
	//Label list, Regex list, column name,default label
	public ReplaceStage(Map<String, String> map, String dL ,int index){

		this.regMap = map;
		this.defaultLabel = dL;
		//this.featureName = featureName;
		this.index = index;
	}*/

	@Override public DataFrame transform(DataFrame dataFrame) {

		/*
		StructType schema = dataFrame.schema();
		JavaRDD<Row>rowRDD = dataFrame.javaRDD();

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

					if (wordList[index] != null) {
						System.out.println("Entering");

						if (matcher.find()) {
							isLabeled = true;
							wordList[index] = entry.getValue();
						}
						System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					}
				}
				if(isLabeled ==false) {
					wordList[index] = defaultLabel;
				}
				return RowFactory.create((Object[])wordList);
			}
		});

		//convert row rdd to a data frame

		Column column = dataFrame.col("Category").rlike("ARREST").as("ABC");
		SQLContext sqlContext = dataFrame.sqlContext();
		DataFrame df = sqlContext.createDataFrame(changedRows,schema);
		*/
				/*
		Row []row = dataFrame.collect();
		String []arr = row.toString().split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
		for(String val : arr) {
			System.out.println(val);
		} */


		DataFrame frame;
		Row []rows = dataFrame.collect();
		int count = dataFrame.collect().length;
		defaultLabel  = "default";

		for (Row row : rows) {
			System.out.println("ROW: "+row.toString());
		}

		String[]wordList = new String[rows.length];

		Map<String,String>regMap = new HashMap<String, String>();
		regMap.put(".*?\\bWARRANT\\b.*?","warrant");
		index = 2;

		for(int x = 0; x <rows.length ; x++) {
			wordList = rows[x].toString().split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");

			for(String s : wordList){
				System.out.println("Word List : "  + s);
			}

			boolean isLabeled = false;

			for (Map.Entry<String, String> entry : regMap.entrySet()) {
				Pattern pattern = Pattern.compile(entry.getKey());
				Matcher matcher = pattern.matcher(wordList[index]);

				if (wordList[index] != null) {

					if (matcher.find()) {
						System.out.println("-------------Entering-------------------");
						isLabeled = true;
						wordList[index] = entry.getValue();
						System.out.println("Changed :"+wordList[index]);
					}
					System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
					System.out.println("Row -------" + rows[x]);
				}
			}
			if(isLabeled ==false) {
				wordList[index] = defaultLabel;
				System.out.println("------------------default--------------------");
				System.out.println(wordList[index] );
			}
			System.out.println(rows[x]);


			int val = 0;
			String line = " ";
			for(String s : wordList){

				if(val < wordList.length-1){
					line+= s + ",";

				}else {
					line+= s;
				}
				val ++;
			}
			System.out.println("Words Line :" + line.trim());

			//Find a way to convert a string(line) to a row


		}
		return null;
	}

/*
	public Transformer passData(Map<String, String> regMap, String defaultLabel, int index){

		this.regMap = regMap;
		this.defaultLabel = defaultLabel;
		this.index = index;
		return this;
	} */

	@Override public StructType transformSchema(StructType structType) {

		return structType;
	}

	@Override public Transformer copy(ParamMap paramMap) {
		return null;
	}

	public String uid() {
		return String.valueOf(Math.random());
	}
}
