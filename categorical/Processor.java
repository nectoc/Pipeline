package pipeline.test.categorical;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by root on 10/19/15.
 */
public class Processor {
	public static void main(String[] args) {

		//Initialize spark context
		//SparkConf conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		//JavaSparkContext scn = new JavaSparkContext(conf);
		//ContextInitializer ci = new ContextInitializer();


		//Initializing SQL Context
		SQLContext sqlContext = ContextInitializer.getSql();

		//Adding csv file to a JavaRDD
		JavaRDD<String> text = ContextInitializer.getJsc().textFile("pipeline.csv").cache();

		//Returns the heading of file since it returns the first line
		final String schemaString = text.first();

		//Split feature names by ,
		//String [] featureNames = schemaString.split(",");
		String [] splitArr = schemaString.split(",");
		String [] featureNames = new String[splitArr.length];

		for(int i=0; i<splitArr.length;i++){
			featureNames[i] = splitArr[i].trim();
			System.out.println(featureNames[i]);
		}


		List<StructField> fields = new ArrayList<StructField>();

		//Add field names to a Struct Field type List
		for (String fieldName: featureNames) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.DoubleType, true));
		}

		//Add structFields to a structType object (creates schema to create the data frame)
		StructType schema = DataTypes.createStructType(fields);

		//Convert records of the RDD to Rows.
		JavaRDD<Row> rowRDD = text.filter(new Function<String, Boolean>() {
			public Boolean call(String val) throws Exception {
				if (schemaString.equals(val)) {
					return false;
				}
				return true;
			}
		}).map(
				new Function<String, Row>() {
					public Row call(String record) throws Exception {
						String[] fields = record.split(",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))");
						Double[] fieldValues = new Double[fields.length];
						int i = 0;
						for (String s : fields) {
							fieldValues[i] = Double.parseDouble(s);
							i++;
						}
						return RowFactory.create((Object[]) fieldValues);
					}
				});


		DataFrame frame = sqlContext.createDataFrame(rowRDD,schema);
		frame.show();

		//RegexTokenizer rt = new RegexTokenizer();
		//rt.setPattern(",");
		Map<String,String>regMap = new HashMap<String, String>();
		regMap.put(".*?\\bTHEFT\\b.*?", "theft");
		//map.put("ARREST","arrest");

		//ReplaceStage rs = new ReplaceStage();
		//rs.transform(frame);
		//rs.passData(map,"default",2);
		//DataFrame ds = rs.transform(frame);

		TestStage ts = new TestStage();
		//ts.transform(frame);

		UsingRDD ur = new UsingRDD();
		//ur.transform(frame);


		//Merge multiple columns to a vector column known as features
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(featureNames).setOutputCol("features");

		//Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {rt, vectorAssembler,lr });
		//PipelineModel model = pipeline.fit(frame);
		//System.out.println();

		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {ur,vectorAssembler});
		PipelineModel model = pipeline.fit(frame);
	}
}
