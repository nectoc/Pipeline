package pipeline.test.categorical;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 10/30/15.
 */

public class RowToFrame extends Transformer {

	Map<String, String> regMap;
	String defaultLabel;
	//String featureName;
	//String[] wordList;
	int index;

	@Override public DataFrame transform(DataFrame dataFrame) {

		DataFrame frame;
		Row[]rows = dataFrame.collect();
		int count = dataFrame.collect().length;
		defaultLabel  = "default";

		for (Row row : rows) {
			System.out.println("ROW: "+row.toString());
		}

		String[]wordList = new String[rows.length];

		Map<String,String> regMap = new HashMap<String, String>();
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

	@Override public StructType transformSchema(StructType structType) {
		return null;
	}

	@Override public Transformer copy(ParamMap paramMap) {
		return null;
	}

	public String uid() {
		return null;
	}
}
