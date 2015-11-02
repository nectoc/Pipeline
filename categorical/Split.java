package pipeline.test.categorical;

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
		String[]words = element.split(delimiter);

		return words;
	}

	private String colName;
	private StructType schema;

	@Override public DataFrame transform(DataFrame dataFrame) {
		DataFrame x =  dataFrame.drop(colName);
		//x.schema.show or juxt
		//return x
		return x;
	}

	public Transformer colName(String name) {
		this.colName = name;
		return this;
	}

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
