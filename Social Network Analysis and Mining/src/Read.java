
import java.awt.Dimension;
import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import javax.swing.JFrame;

import agape.tools.Components;
import edu.uci.ics.jung.algorithms.layout.CircleLayout;
import edu.uci.ics.jung.algorithms.layout.Layout;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
// import java.util.regex.Matcher;
// import java.util.regex.Pattern;
import edu.uci.ics.jung.graph.Graph;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;
import edu.uci.ics.jung.graph.util.EdgeType;
import edu.uci.ics.jung.visualization.BasicVisualizationServer;



public class Read {
   // JDBC driver name and database URL
   static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";  
   static final String DB_URL = "jdbc:mysql://localhost:3306/twitter";

   //  Database credentials
   static final String USER = "root";
   static final String PASS = "77#481122bB?";
   
   public static void main(String[] args) {
   Connection conn = null;
   Statement stmt = null;
   try{
      //STEP 2: Register JDBC driver
      Class.forName("com.mysql.jdbc.Driver");

      //STEP 3: Open a connection
      System.out.println("Connecting to database...");
      conn = DriverManager.getConnection(DB_URL,USER,PASS);

      //STEP 4: Execute a query
      System.out.println("Creating statement...");
      stmt = conn.createStatement();
      String sql;
      sql = "SELECT text FROM tweet";
      ResultSet rs = stmt.executeQuery(sql);
      
      Graph<String, String> g = new  DirectedSparseGraph<String, String>();
      HashSet<String> secondset = new HashSet<String>();
      //STEP 5: Extract data from result set
      
      int times = 0;
      while(rs.next()){
    	  String last = rs.getString("text");
    	  
    	  
    	  // add the vertices
    	  for (String token: last.replaceAll("[^0-9A-Za-z]"," ").split("\\s+")){
    	  if(!g.containsVertex(token) && !token.isEmpty() ){
				g.addVertex((String) token);
			}
          }
    	  
    	  // add the edges 
    	  String unique[] = last.replaceAll("[^0-9A-Za-z]"," ").split("\\s+");
    	  int treesize = (unique.length);
    	  System.out.println(last);
    	  for(Integer i = 0 ; i < treesize; i++){
  			for (Integer j = 0 ; j< treesize; j++){
  				if (!unique[i].isEmpty()  && !unique[j].isEmpty()) {
  					String Edge = unique[i] + "-" + unique[j];
  					String AltEdge = unique[j] + "-" + unique[i];
  					if (!g.containsEdge(Edge) && !g.containsEdge(AltEdge) && unique[i] != unique[j]) {
  						if (i < j ){
  		  					g.addEdge(Edge, unique[i], unique[j], EdgeType.DIRECTED);}
  						else{
  		  					g.addEdge(Edge, unique[j], unique[i], EdgeType.DIRECTED);}
  						}
  					}
  				}
  			} 
    	  times += 1;
    	  if (times > 100){ //only read the first 100 tweets.
    		 break;}
      }//end of 1st while loop for global vertices adding
      
      //ArrayList<Set<Object>> k = getAllStronglyConnectedComponent(g);
      //System.out.println(k);
      
      
      /* print out the graph for fun
       * 
      Layout<Integer, String> layout = new CircleLayout(graph);
      BasicVisualizationServer<Integer,String> vv = new BasicVisualizationServer<Integer,String>(layout);
      JFrame frame = new JFrame("Simple Graph View");
      frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
      frame.getContentPane().add(vv);
      frame.pack();
      frame.setVisible(true);
      */
      
      //STEP 6: Clean-up environment
      rs.close();
      stmt.close();
      conn.close();
   }
 
   catch(Exception e){
      //Handle errors for Class.forName
      System.out.println("error");
	   e.printStackTrace();
	   System.exit(1);
   }finally{
      //finally block used to close resources
      try{
         if(stmt!=null)
            stmt.close();
      }catch(SQLException se2){
      }// nothing we can do
      try{
         if(conn!=null)
            conn.close();
      }catch(SQLException se){
         se.printStackTrace();
      }//end finally try
   }//end try
   System.out.println("Goodbye!");
}//end main

	   public static <V,E> java.util.ArrayList<java.util.Set<V>> getAllStronglyConnectedComponent(edu.uci.ics.jung.graph.Graph<String, String> graph) {
	        return (ArrayList<Set<V>>) graph ;
	    }

   /*
	public static Graph<String, String> getAllStronglyConnectedComponent(Graph<String, String> graph){
		static Graph graph = getAllStronglyConnectedComponent(Graph<String, String> graph);
		
		return graph;
	}
   */
}//end FirstExample
