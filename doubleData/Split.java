package pipeline.test.doubleData;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;

/**
 * Created by root on 9/24/15.
 */
public class Split extends Transformer implements Function<String, String[]> {

	public String[] call(String element) throws Exception {

		String delimiter= ",(?=(?:[^\"]|\"[^\"]*\")*$)";

		//Split data
		System.out.println("Words");
		String[]words = element.split(delimiter);

		return words;
	}

	private String colName;

	@Override public DataFrame transform(DataFrame dataFrame) {
		//Drop a column
		DataFrame df = dataFrame.drop(colName);
		//Prints remaining
		df.printSchema();
		df.show();
		System.out.println("Transform");
		return df;
	}

	//Returns schema
	@Override public StructType transformSchema(StructType structType) {

		//System.out.println("Field Names");

		/*List<StructField>sf = new ArrayList<StructField>();

		for (StructField s : structType.fields()){
			if(!s.name().equals(colName)){
				sf.add(s);
			}
		}
		return DataTypes.createStructType(sf); */
		System.out.println("Transform Schema");
		return structType;
	}


	@Override public Transformer copy(ParamMap paramMap) {
		return null;

	}

	public Transformer columnName(String name){
		this.colName = name;
		return this;
	}

	//Unique ID
	public String uid() {
		return String.valueOf(Math.random());
	}
}
