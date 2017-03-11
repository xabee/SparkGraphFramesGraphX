import java.io.File;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import exercise_1.Exercise_1;
import exercise_2.Exercise_2;
import exercise_3.Exercise_3;


public class UPCSchool_Spark {

	static String HADOOP_COMMON_PATH = "G:\\winutils";
	
	public static void main(String[] args) throws Exception {
		System.setProperty("hadoop.home.dir", HADOOP_COMMON_PATH);
		
		SparkConf conf = new SparkConf().setAppName("UPCSchool-Spark").setMaster("local[2]");
		JavaSparkContext ctx = new JavaSparkContext(conf);
		SQLContext sqlctx = new SQLContext(ctx);
		LogManager.getRootLogger().setLevel(Level.ERROR);
		
		//Exercise_1.warmup(ctx,sqlctx);
		//Exercise_2.basicGraphframes(ctx,sqlctx);
		Exercise_3.wikipedia(ctx,sqlctx);
		

	}

}
