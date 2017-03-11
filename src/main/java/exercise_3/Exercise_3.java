package exercise_3;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;

public class Exercise_3 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

//////////// LOAD VERTICES ---------------------------------------------------------

		java.util.List<Row> vertices_list = new ArrayList<Row>();
		
        String fileName = "/wiki-vertices.txt";
        String line = null;

        try {
	            int i=0;
        		FileReader fileReader =  new FileReader(fileName);
	            BufferedReader bufferedReader = new BufferedReader(fileReader);
	            while((line = bufferedReader.readLine()) != null) {						// omit 1st row

	                String parsedline[] = line.split("\\s", 2);
	                //System.out.println(parsedline.toString());
	                //System.out.println("N: "+i+ " " + line.toString());
	                //System.out.println(parsedline[0]);
	                //System.out.println(parsedline[1]);
	                // System.out.println(parsedline[2]);
	                vertices_list.add(RowFactory.create(parsedline[0], parsedline[1]));
	                		
	                		  
	                i++;
	            }   
                
	            bufferedReader.close();         
        	}
        catch(FileNotFoundException ex) {
            	System.out.println("Unable to open file '" + fileName + "'");                
        	}
        catch(IOException ex) {
            	System.out.println("Error reading file '"  + fileName + "'");                  
        	}
        
		JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);
		
		StructType vertices_schema = new StructType(new StructField[]{
				new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build()),
			});
		
		DataFrame vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
        	
//////////// LOAD EDGES ---------------------------------------------------------
		
		java.util.List<Row> edges_list = new ArrayList<Row>();
		
        // The name of the file to open.
        fileName = "/wiki-edges.txt";

        // This will reference one line at a time
        line = null;

        try {
	            int i=0;
        		FileReader fileReader =  new FileReader(fileName);
	            BufferedReader bufferedReader = new BufferedReader(fileReader);
	            while((line = bufferedReader.readLine()) != null) {						// omit 1st row
	            	// System.out.println(line);
	                String parsedline[] = line.split("\\s");
	                // System.out.println(parsedline.toString());
	               // System.out.println("Nº "+i);
	               // System.out.println(parsedline[0]);
	                //System.out.println(parsedline[1]);
	                //System.out.println(parsedline[2]);
	                
	                edges_list.add(RowFactory.create(parsedline[0], parsedline[1]));
	                		
	                i++;
	            }   
                
	            bufferedReader.close();         
        	}
        catch(FileNotFoundException ex) {
            	System.out.println("Unable to open file '" + fileName + "'");                
        	}
        catch(IOException ex) {
            	System.out.println("Error reading file '"  + fileName + "'");                  
        	}
		
		JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);
		
		StructType edges_schema = new StructType(new StructField[]{
			new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
			new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
		});
		
		DataFrame edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		
		// Graph
		GraphFrame G = new GraphFrame(vertices,edges);
		
		// System.out.println(G);
	
		// G.vertices().show();
		// G.edges().show();
		
		// pageRank calculation, 10 iterations
				Row[] rank = G.pageRank().resetProbability(0.15).maxIter(10).run().vertices().orderBy(new Column("pagerank").desc()).take(10);
				System.out.println("\n10 most relevant articles:\n");
				int i=0;
				for(Row row:rank){
					i++;
					System.out.println(Integer.toString(i) +": " + row.get(1)+" ----> pageRank: " + row.get(2));
				}
	
	}
	
}
