package pipeline.test.doubleData;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StructType;

/**
 * Created by root on 10/16/15.
 */
public class Test extends Transformer {
	@Override public DataFrame transform(DataFrame dataFrame) {
		dataFrame.show();
		return dataFrame;
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
