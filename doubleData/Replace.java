package pipeline.test.doubleData;

import org.apache.spark.api.java.function.Function;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by root on 9/24/15.
 */
public class Replace implements Function<String[], String[]> {

	static int index;
	static String dLabel;
	static Map<String, String> map;

	public Replace(Map<String, String> m, int i, String d) {
		map = m;
		index = i;
		dLabel = d;
	}

	public String[] call(String[] words) throws Exception {

		boolean isLabeled = false;

		for (Map.Entry<String, String> entry : map.entrySet()) {
			Pattern pattern = Pattern.compile(entry.getKey());
			Matcher matcher = pattern.matcher(words[index]);

			if (words[index] != null) {

				if (matcher.find()) {
					isLabeled = true;
					words[index] = entry.getValue();
				}
				System.out.println("Key : " + entry.getKey() + " Value : " + entry.getValue());
			}
		}
		if(isLabeled ==false) {
			words[index] = dLabel;
		}

		return words;
	}
}