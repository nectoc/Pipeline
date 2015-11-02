package pipeline.test.categorical;

import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.runtime.BoxedUnit;

import java.util.function.Function;

/**
 * Created by root on 10/29/15.
 */
public class TestStage extends Transformer {

	@Override public DataFrame transform(DataFrame dataFrame) {
		// 1. Can't use it since it requires a condition
		/*
		DataFrame test = dataFrame.filter(".*?\\bWARRANT\\b.*?");
		test.show();
		return test;*/

		//2.foreach - Can't identify what boxedUnit is
		/*DataFrame df = dataFrame.foreach(new Function<Row, Row>() {
			public Row apply(Row row) {
				return null;
			}

			public <V> Function<V, Row> compose(Function<? super V, ? extends Row> before) {
				return null;
			}

			public <V> Function<Row, V> andThen(Function<? super Row, ? extends V> after) {
				return null;
			}
		}); */

		//dataFrame.foreach(new Function);

		//3.Column approach - Can't find a way to iterate (in rdds they provide a single row at a time an so on so it's easy to
		//split values and do the task but it simply applies to all the rows )
		//Column c = dataFrame.col("category");

		//4.Column approach 2
		/*
		String []columns = dataFrame.columns();
		for(String s : columns){
			if(s.matches("Category")){

			}
			System.out.println(s);
		}*/

		System.out.println("Transform");
		return null;

	}

	@Override public StructType transformSchema(StructType structType) {
		System.out.println("Schema");
		return null;
	}

	@Override public Transformer copy(ParamMap paramMap) {
		return null;
	}

	public String uid() {
		System.out.println("uid");
		return String.valueOf(Math.random());
	}
}
