package pipeline.test.doubleData;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;

/**
 * Created by root on 10/16/15.
 */
public class Stage2 extends Transformer {

	private String Label;
	private String regex;

	@Override public DataFrame transform(DataFrame dataFrame) {
		DataFrame df = dataFrame.filter(Label);
		return null;
	}

	@Override public StructType transformSchema(StructType structType) {
		return null;
	}

	@Override public Transformer copy(ParamMap paramMap) {
		return null;
	}
	public Transformer getVal(String val){
		this.Label = val;
		return this;
	}

	public String uid() {
		return null;
	}
}
