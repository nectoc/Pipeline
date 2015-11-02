package pipeline.test.doubleData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;

/**
 * Created by root on 10/7/15.
 */
public class Processor {
	public static void main(String[] args) {

		//Initialize spark context
		SparkConf conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local[4]");
		JavaSparkContext scn = new JavaSparkContext(conf);

		//Initializing SQL Context
		SQLContext sqlContext = new SQLContext(scn);

		//Adding csv file to a JavaRDD
		JavaRDD<String> text = scn.textFile("IndiansDiabetes.csv").cache();

		//Returns the heading of file since it returns the first line
		final String schemaString = text.first();

		//Split feature names by ,
		String [] splitArr = schemaString.split(",");
		String [] featureNames = new String[splitArr.length];

		//Dropped features
		String [] dropped = new String[featureNames.length -1];
		//Remaining features
		String [] remaining = new String[featureNames.length -1];

		int i = 0;
		for(String s : splitArr){
			featureNames[i] = s;
			i++;
		}
		for(String a : featureNames){
			System.out.println(a);
		}

		int x = 0;
		int y = 0;
		for(String s : featureNames){

			if(s.equals("PG2")) {
				dropped[x] = s;
				x++;
			}else{
				remaining[y] = s;
				y++;
			}

		}

		List<StructField> fields = new ArrayList<StructField>();

		//Add field names to a Struct Field type List
		for (String fieldName: schemaString.split(",")) {
				fields.add(DataTypes.createStructField(fieldName, DataTypes.DoubleType, true));
		}
		//Add structFields to a structType object (creates schema)
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD to Rows.
		JavaRDD<Row> rowRDD = text.filter(new Function<String, Boolean>() {
			public Boolean call(String s) throws Exception {
				if (schemaString.equals(s)) {
					return false;
				}
				return true;
			}
		}).map(new Function<String, Row>() {
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

		//Pass rowRDD and struct type schema to data frame
		DataFrame trainDataFrame = sqlContext.createDataFrame(rowRDD, schema);

		trainDataFrame.show();
		//trainDataFrame.registerTempTable("Data");
		//trainDataFrame.write().parquet("Data.parquet")\
		//New pipeline stage
		Split split = new Split();
		split.columnName("PG2");



		//Merge multiple columns to a vector column known as features
		VectorAssembler vectorAssembler = new VectorAssembler();
		vectorAssembler.setInputCols(remaining).setOutputCol("features");
		//OneHotEncoder encoder =new OneHotEncoder().setInputCol("Category").setOutputCol("cat");


		// Configuring ML pipeline
		LinearRegression lr = new LinearRegression().setMaxIter(10).setRegParam(0.001).setLabelCol("Age");
		Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {split, vectorAssembler,lr });
		PipelineModel model = pipeline.fit(trainDataFrame);



		}
	}
