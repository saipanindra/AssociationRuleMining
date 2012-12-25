import java.sql.*;
import java.io.*;
import java.util.*;
public class arm {
	
	static Connection con;
	static String inputFile = "system.in";
	public static void main(String[] args) throws Exception{
		
		File f = new File(inputFile);
		if(f.exists()){
			Scanner input = new Scanner(f);
			String credentials = input.nextLine();
			input.close();
			String uname = credentials.split(",")[0].split("=")[1];
			String pword = credentials.split(",")[1].split("=")[1];
			
            con = connectToDb(uname,pword);
            if(con!=null){
			System.out.println("Connected to Database:"+con);
            loadData(uname,pword);
           task1();
           task2();
            task3();
            task4();
			con.close();
		}
            else
            	System.out.println("Connection Error");
		}
		else
			System.out.println("Input file system.in not found");
	}
	static void task1() throws FileNotFoundException,IOException,SQLException
	{
		//Read Input
		System.out.println("Task1 Started...");
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		br.readLine();
		String task1Input = br.readLine();
		br.close();
		double supportPercent = Double.parseDouble(task1Input.split(":")[1].split("=")[1].split("%")[0].trim());
		if(supportPercent >=0){
		System.out.println("Task1 Support Percent :" + supportPercent);
		//Prepare query
		String task1Sql = "select  temp.iname,(temp.counttrans/temp2.uniquetrans)*100 as percent"+
                          " from (select i.itemname iname,count(t.transid) counttrans from trans t, items i"+
                          " where i.itemid = t.itemid group by i.itemname having count(t.transid)>=(select count(distinct transid)*"+supportPercent/100+
                          " from trans)"+
                          ") temp , (select count(distinct transid) uniquetrans from trans) temp2 order by percent";
		
		
		PreparedStatement selTask1 = con.prepareStatement(task1Sql);
		ResultSet rsTask1 = selTask1.executeQuery();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("system.out.1")); 
		while(rsTask1.next())
		{
			bw.write("{"+rsTask1.getString(1)+"}, s="+rsTask1.getDouble(2)+"%");
			bw.newLine();
		}
		rsTask1.close();
		bw.close();
	    System.out.println("Task1 Completed...\n");
		}
		else
			System.out.println("Support percent should be a positive number");
		
	}
	static void task2() throws FileNotFoundException,IOException,SQLException
	{
		System.out.println("Task2 Started...");
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		br.readLine();
		br.readLine();
		String task2Input = br.readLine();
		br.close();
		double supportPercent = Double.parseDouble(task2Input.split(":")[1].split("=")[1].split("%")[0].trim());
		if(supportPercent >=0){
		System.out.println("Task2 Support percent:"+supportPercent);
		try{
		PreparedStatement dropView = con.prepareStatement("drop materialized view trans1");
		dropView.executeUpdate();}
		catch(SQLException e){
			}
		//Creating materilalized view to filter out transactions as per apriori rule
		String sqlTransView = "create materialized view trans1(transid,itemid) as select * from trans where itemid in"+
				" (select itemid from trans group by itemid having count(*)>=(select count(distinct(transid))*"+supportPercent/100+" from trans))";
		
		
		PreparedStatement createView = con.prepareStatement(sqlTransView);
		
		
		createView.executeUpdate();
		createView.close();
        //Using SQL from task1 to retrieve item sets of size 1.
		String task1Sql = "select  temp.iname,(temp.counttrans/temp2.uniquetrans)*100 as percent"+
                " from (select i.itemname iname,count(t.transid) counttrans from trans t, items i"+
                " where i.itemid = t.itemid group by i.itemname having count(t.transid)>=(select count(distinct transid)*"+supportPercent/100+
                " from trans)"+
                ") temp , (select count(distinct transid) uniquetrans from trans) temp2 order by percent";

		PreparedStatement selTask1 = con.prepareStatement(task1Sql);
		ResultSet rsTask1 = selTask1.executeQuery();
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("system.out.2")); 
		while(rsTask1.next())
		{
			bw.write("{"+rsTask1.getString(1)+"}, s="+rsTask1.getDouble(2)+"%");
			bw.newLine();
		}
		rsTask1.close();
		selTask1.close();
		
		
		
		String task2Sql = "select  temp.iname1,temp.iname2,(temp.counttrans/temp2.uniquetrans)*100 as percent"+
								   " from(select t1.itemid,t2.itemid,i1.itemname iname1,i2.itemname iname2,count(*) counttrans"+
								   " from trans1 t1,trans1 t2,items i1,items i2 where t1.transid = t2.transid and t1.itemid = i1.itemid"+
								   " and t2.itemid = i2.itemid and t1.itemid < t2.itemid group by t1.itemid,t2.itemid,i1.itemname,i2.itemname"+
								   " having count(*)>=(select count(distinct transid)*"+supportPercent/100+" from trans))  temp , (select count(distinct transid) uniquetrans from trans) temp2 order by percent";
		
		
		PreparedStatement selTask2 = con.prepareStatement(task2Sql);
		ResultSet rsTask2 = selTask2.executeQuery();
		
		while(rsTask2.next())
		{
			bw.write("{"+rsTask2.getString(1)+", "+rsTask2.getString(2)+"}, s="+rsTask2.getDouble(3)+"%");
			bw.newLine();
		}
		rsTask2.close();
		selTask2.close();
		bw.close();
		System.out.println("Task2 Completed...\n");
		}
		else
			System.out.println("Support percent should be a positive number");
		
	}
	
	static void task3() throws FileNotFoundException,IOException,InterruptedException,SQLException
	{
		System.out.println("Task3 Started..");
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		br.readLine();
		br.readLine();
		br.readLine();
		String task3Input = br.readLine();
		br.close();
		double supportPercent = Double.parseDouble(task3Input.split(":")[1].split(",")[0].split("=")[1].split("%")[0].trim());
		int size = Integer.parseInt(task3Input.split(":")[1].split(",")[1].split("=")[1].trim());
		if(supportPercent>=0 && size >0){
		System.out.println("Task3 Size : "+ size);
		System.out.println("Task3 Support Percent: "+supportPercent);
		BufferedWriter bw = new BufferedWriter(new FileWriter("system.out.3"));
		for(int i=1;i<=size;i++)
		{
			CallableStatement cstmt = con.prepareCall("{CALL GenerateFI(?,?)}");
			cstmt.setInt(1, i);
			cstmt.setDouble(2,supportPercent/100);
			cstmt.executeQuery();
			
			
			String sqlTask3 = "select i.itemname,f.percent from FISet f ,items i where f.itemid = i.itemid order by f.percent,f.isetid";
			//String sqlTask3 = "select i.itemname,f.percent from FISet f ,items i where f.itemid = i.itemid order by f.isetid";
			PreparedStatement selTask3 = con.prepareStatement(sqlTask3);
			ResultSet rsTask3 =  selTask3.executeQuery(sqlTask3);
			
			int j =1;
			String res= "";
			j=1;
			while(rsTask3.next()){
				
				if(j==1){
					  
					  res += "{";
				  }
				  res = res + rsTask3.getString(1);
				  if(j==i){
					  res+= "}, s="+rsTask3.getDouble(2)+"%";
					  bw.write(res);
					  bw.newLine();
					  j=1;
					  res ="";
				  }
				  else
				  {
					  res+=", ";
					  j++;
				  }
				  
				 
			}
			rsTask3.close();
			selTask3.close();
			
		}
		  bw.close();
		  System.out.println("Task3 Completed...\n");
		}
		else
		{
			System.out.println("Support percent should a postive number and size should be a positive integer.");
		}
	}
		
	
	static Connection connectToDb(String uname,String pword) throws SQLException{
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		Connection con = DriverManager.getConnection("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",uname.trim(), pword.trim());
		return con;
	}
	
	static void task4() throws FileNotFoundException,IOException,InterruptedException,SQLException
	{
		System.out.println("Task4 Started...");
		BufferedReader br = new BufferedReader(new FileReader(inputFile));
		br.readLine();
		br.readLine();
		br.readLine();
		br.readLine();
		String task4Input = br.readLine();
		br.close();
		double supportPercent = Double.parseDouble(task4Input.split(":")[1].split(",")[0].split("=")[1].split("%")[0].trim());
		double confidence = Double.parseDouble(task4Input.split(":")[1].split(",")[1].split("=")[1].split("%")[0].trim());
		int size = Integer.parseInt(task4Input.split(":")[1].split(",")[2].split("=")[1].trim());
		if(supportPercent>=0 && size>=2 && confidence>=0){
		System.out.println("Task4 Confidence Percent: "+confidence);
		System.out.println("Task4 Support Percent: "+supportPercent);
		System.out.println("Task4 Size : "+ size);
		
		BufferedWriter bw = new BufferedWriter(new FileWriter("system.out.4"));
		for(int i=2;i<=size;i++)
		{
			CallableStatement cstmt = con.prepareCall("{CALL GenerateAR(?,?,?)}");
			cstmt.setDouble(1, confidence/100);
			cstmt.setInt(2,i);
			cstmt.setDouble(3,supportPercent/100);
			cstmt.executeQuery();
			
			
			String sqlTask4 = "select * from artable order by ruleid,confidence";
			PreparedStatement selTask4 = con.prepareStatement(sqlTask4);
			ResultSet rsTask4 =  selTask4.executeQuery(sqlTask4);
			
			String leftSet = "";
			String rightSet = "";
			Boolean resultSetExhausted = false;
			String itemName;
			String isLeft;
			Double support;
			Double conf;
			if(rsTask4.next()){
			while(true && !resultSetExhausted){
				
				
				leftSet = "{";
				rightSet= "{";
				for(int j=0;j<i;j++){
					itemName = rsTask4.getString(3);
					isLeft = rsTask4.getString(5);
					//support = rsTask4.getBigDecimal(6);
					//conf = rsTask4.getBigDecimal(4);
					if(j==0){
						
						if(isLeft.equals("Y"))
						 leftSet =leftSet+itemName;
						else
						  rightSet = rightSet+itemName;
						if(!rsTask4.next()) {resultSetExhausted = true;break; }
					}
					if(j==i-1){
						if(isLeft.equals("Y")){
							if(leftSet.equals("{")) leftSet =leftSet+itemName;
							 else leftSet =leftSet+","+itemName;	
						}
  					   else{
  						 if(rightSet.equals("{")) rightSet = rightSet + itemName;
							else rightSet = rightSet + ", "+itemName;
  					   }
						  
						leftSet = leftSet+"}";
						rightSet  = rightSet+"}";
						String resultString = "{"+leftSet+" - > "+rightSet+"} s="+rsTask4.getDouble(6)+"%, c="+rsTask4.getDouble(4)+"%";
						bw.write(resultString);
						bw.newLine();
						leftSet = "{";
						rightSet = "{";
						if(!rsTask4.next()) {resultSetExhausted = true;break; }
					}
					
					if(j>0 && j<i-1){
						
						if(isLeft.equals("Y")){
							 if(leftSet.equals("{")) leftSet =leftSet+itemName;
							 else leftSet =leftSet+", "+itemName;
						}
						else{
							if(rightSet.equals("{")) rightSet = rightSet + itemName;
							else rightSet = rightSet + ", "+itemName;
						}
						if(!rsTask4.next()) {resultSetExhausted = true;break; }
					   }
					
					}
				}
				
			}			
		}
		  bw.close();  	
	      System.out.println("Task4 Completed...\n");
		}
		else
			 System.out.println("For Task4, supportPercent and confidence should be a positive numbers and size should be greater than or equal to 2");
			
	}
	
	
	static void loadData(String uname,String pword) throws Exception
	{
		System.out.println("Data load started..\n");
		PreparedStatement dropTrans = con.prepareStatement("drop table Trans");
		PreparedStatement dropItems = con.prepareStatement("drop table Items");
		PreparedStatement dropFISet = con.prepareStatement("drop table FISet");
		PreparedStatement dropPairs = con.prepareStatement("drop table pairs");
		PreparedStatement dropTempFISet = con.prepareStatement("drop table TempFISet");
		PreparedStatement dropTemp = con.prepareStatement("drop table temp");
		PreparedStatement dropTempset = con.prepareStatement("drop table tempset");
		PreparedStatement dropTempright = con.prepareStatement("drop table tempright");
		PreparedStatement dropArtable = con.prepareStatement("drop table artable");
		PreparedStatement createTrans = con.prepareStatement("CREATE TABLE TRANS (TRANSID NUMBER,ITEMID NUMBER)");
		PreparedStatement createItems = con.prepareStatement("CREATE TABLE ITEMS(ITEMID NUMBER,ITEMNAME VARCHAR2(100))");
		PreparedStatement createFISet = con.prepareStatement("CREATE TABLE FISet(ISetID NUMBER,ITEMID NUMBER,percent NUMBER)");
		PreparedStatement createPairs = con.prepareStatement("create table pairs(id1 number,id2 number)");
		PreparedStatement createTempFISet = con.prepareStatement("CREATE TABLE TempFISet(ISetid number,itemid number,percent number)");
		PreparedStatement createTemp = con.prepareStatement("CREATE TABLE Temp (ITEMID NUMBER)");
		PreparedStatement createTempset = con.prepareStatement("CREATE TABLE tempset (ITEMID NUMBER)");
		PreparedStatement createTempright = con.prepareStatement("CREATE TABLE tempright (ITEMID NUMBER)");
		PreparedStatement createArtable = con.prepareStatement("CREATE TABLE artable(ruleid NUMBER,itemid NUMBER,itemname VARCHAR2(100 BYTE),confidence NUMBER,isleft CHAR(1 BYTE),support NUMBER)");
		
		try{
		      dropTrans.executeUpdate();
		}
		catch(Exception e){
			
		}
		try{

		      dropItems.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropFISet.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropPairs.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropTempset.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropTempright.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropArtable.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropTempFISet.executeUpdate();
		}
		catch(Exception e){
			
		}
        try{
		      dropTemp.executeUpdate();
		}
		catch(Exception e){
			
		}
        con.commit();
        
		createTrans.executeUpdate();
		createItems.executeUpdate();
		createFISet.executeUpdate();
		createPairs.executeUpdate();
		createTempFISet.executeUpdate();
		createTemp.executeUpdate();
		createTempset.executeUpdate();
		createTempright.executeUpdate();
		createArtable.executeUpdate();
		writeItemsControlFile();
		writeTransControlFile();
		String loadItemsString = "sqlldr "+uname.trim()+"/"+pword.trim()+"@orcl control=items.ctl";
		String loadTransString = "sqlldr "+uname.trim()+"/"+pword.trim()+"@orcl control=trans.ctl";
		
		Process p1 = Runtime.getRuntime().exec(loadItemsString);
		p1.waitFor();
		Process p2 = Runtime.getRuntime().exec(loadTransString);
		p2.waitFor();
                File f = new File("arm.sql");
                if(f.exists()){
		 Process p3 = Runtime.getRuntime().exec("sqlplus "+uname.trim()+"@orcl/"+pword.trim()+" @arm.sql");
		 p3.waitFor();
		 con.commit();
		 System.out.println("Data Load Completed...\n");
                }
                else
                {
                  System.out.println("The arm.sql file which has the stored procedures forthe project is not in the current directory");
                  System.exit(0);
                }
     }
	public static void writeItemsControlFile() throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("items.ctl"));
		bw.write("LOAD DATA");
		bw.newLine();
		bw.write("INFILE items.dat");
		bw.newLine();
		bw.write("INTO TABLE items");
		bw.newLine();
		bw.write("FIELDS TERMINATED BY ',' optionally enclosed by X'27'");
		bw.newLine();
		bw.write("(itemid,itemname)");
		bw.close();
	}
	public static void writeTransControlFile() throws IOException{
		BufferedWriter bw = new BufferedWriter(new FileWriter("trans.ctl"));
		bw.write("LOAD DATA");
		bw.newLine();
		bw.write("INFILE trans.dat");
		bw.newLine();
		bw.write("INTO TABLE trans");
		bw.newLine();
		bw.write("FIELDS TERMINATED BY ','");
		bw.newLine();
		bw.write("(transid,itemid)");
		bw.close();
	}


}

