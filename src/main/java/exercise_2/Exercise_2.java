package exercise_2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.BR;
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

public class Exercise_2 {
	
	public static void basicGraphframes(JavaSparkContext ctx, SQLContext sqlCtx) {
		
//////////
/*************************************************
*************** 2.1 Load Graph ******************
*************************************************/
		
//////////// LOAD VERTICES ---------------------------------------------------------

		java.util.List<Row> vertices_list = new ArrayList<Row>();
		
        String fileName = "/people.txt";
        String line = null;

        try {
	            int i=0;
        		FileReader fileReader =  new FileReader(fileName);
	            BufferedReader bufferedReader = new BufferedReader(fileReader);
	            while((line = bufferedReader.readLine()) != null) {						// omit 1st row
	                if (i!=0) {
				            	// System.out.println(line);
				                String parsedline[] = line.split(",");
				                // System.out.println(parsedline.toString());
				                // System.out.println("Nº "+i);
				                // System.out.println(parsedline[0]);
				                // System.out.println(parsedline[1]);
				                // System.out.println(parsedline[2]);
		                		vertices_list.add(RowFactory.create(parsedline[0], parsedline[1], Integer.parseInt(parsedline[2])));
	                		  }
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
				new StructField("name", DataTypes.StringType, true, new MetadataBuilder().build()),
				new StructField("age", DataTypes.IntegerType, true, new MetadataBuilder().build())
			});
		
		DataFrame vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);
        	
		//////////// LOAD EDGES ---------------------------------------------------------
		
		java.util.List<Row> edges_list = new ArrayList<Row>();
		
        fileName = "/likes.txt";

        line = null;

        try {
	            int i=0;
        		FileReader fileReader =  new FileReader(fileName);
	            BufferedReader bufferedReader = new BufferedReader(fileReader);
	            while((line = bufferedReader.readLine()) != null) {						// omit 1st row
	                if (i!=0) {										// omit 1st line, header
				            	// System.out.println(line);
				                String parsedline[] = line.split(",");
				                // System.out.println(parsedline.toString());
				               // System.out.println("Nº "+i);
				               // System.out.println(parsedline[0]);
				                //System.out.println(parsedline[1]);
				                //System.out.println(parsedline[2]);
				                edges_list.add(RowFactory.create(parsedline[0], parsedline[1], parsedline[2]));
	                		  }
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
			new StructField("likes", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		
		DataFrame edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);
		
		GraphFrame G = new GraphFrame(vertices,edges);
		
		System.out.println(G);
	
		G.edges().show();
		G.vertices().show();

	 
/*************************************************
 ************** 2.2 Graph Views ******************
 *************************************************/
				
		// names of the users that are at least 30
		DataFrame filteredvertices = vertices.filter("age>=30");
		filteredvertices.show();
		
		Row[] triplets = G.triplets().collect();
		
		//G.triplets().show();
		
		System.out.println("\n'likes'/'loves' from triplets:\n");
		System.out.println("\n");
		for(Row row:triplets){
		    if(Integer.parseInt(row.getStruct(0).getString(2))>5)
			      {
			        System.out.println(row.getStruct(1).get(1)+ " loves "+ row.getStruct(2).get(1));
			      }
		    else
				  {
		    	    System.out.println(row.getStruct(1).get(1)+ " likes "+ row.getStruct(2).get(1));	
				  }
		}
				 
/*************************************************
 *********** 2.3 Querying the Graph **************
 *************************************************/
		
		System.out.println("\nYounger node: \n" + G.vertices().orderBy("age").first());
		
		System.out.println("\n in 'likes' (different) per node: \n");
		G.inDegrees().show();
		
		System.out.println("\n given 'likes' (different) per node\n");
		G.outDegrees().show();
				 
/*************************************************
 ************* 2.4 Motif Finding *****************
 *************************************************/
		
		// triplets of users A,B,C where A follows B and B follows C, but A does not follow C
		System.out.println("\n triplets of users A,B,C where A follows B and B follows C, but A does not follow C\n");        
		G.find("(a)-[e]->(b);(b)-[e1]->(c); !(a)-[]->(c) ").show();
		
		System.out.println("\n Find the subgraph of people with 3 likes or more");
		G.find("(a)-[e]->(b)").filter("e.likes >= 3").show();

	}
	
}
