package exercise_1;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.examples.*;
import org.graphframes.lib.PageRank;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import scala.Function;
import scala.Tuple3;

public class Exercise_1 {

	
	public static void warmup(JavaSparkContext ctx, SQLContext sqlCtx) {
		java.util.List<Row> vertices_list = new ArrayList<Row>();		
		vertices_list.add(RowFactory.create("a", "Alice", 34));
		vertices_list.add(RowFactory.create("b", "Bob", 36));
		vertices_list.add(RowFactory.create("c", "Charlie", 30));
		vertices_list.add(RowFactory.create("d", "David", 29));
		vertices_list.add(RowFactory.create("e", "Esther", 32));
		vertices_list.add(RowFactory.create("f", "Fanny", 36));
		vertices_list.add(RowFactory.create("g", "Gabby", 60));		
		
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		
		
		StructType vertices_schema = new StructType(new StructField[]{
			new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("age", DataTypes.IntegerType, true, new MetadataBuilder().build())
		});
		
		DataFrame vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
		
		
		
		// edges creation
		java.util.List<Row> edges_list = new ArrayList<Row>();
		
		edges_list.add(RowFactory.create("a", "b", "friend"));
		edges_list.add(RowFactory.create("b", "c", "follow"));
		edges_list.add(RowFactory.create("c", "b", "follow"));
		edges_list.add(RowFactory.create("f", "c", "follow"));
		edges_list.add(RowFactory.create("e", "f", "follow"));
		edges_list.add(RowFactory.create("e", "d", "friend"));
		edges_list.add(RowFactory.create("d", "a", "friend"));
		edges_list.add(RowFactory.create("a", "e", "friend"));
		
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
			
		
		StructType edges_schema = new StructType(new StructField[]{
			new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("relationship", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		
		DataFrame edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		
		
		GraphFrame gf = new GraphFrame(vertices, edges);
		
		System.out.println(gf);
	
		gf.edges().show();
		gf.vertices().show();
	}
	
}
