import java.sql.*;
import java.io.*;
import java.util.*;

class Proc {
	String id ;
	String oproc;
	String sproc;
	int columns[];
	boolean skip;
	
	Proc(String arg) {
		StringTokenizer tokens = new StringTokenizer(arg,"|");

		id = tokens.nextToken().trim();
		if (id.startsWith("S"))  skip=true;
		else  skip=false;
		
		sproc=tokens.nextToken().trim();
		oproc = tokens.nextToken().trim();
		if  (tokens.hasMoreTokens()) {
			String col = tokens.nextToken();
			StringTokenizer coltoken = new StringTokenizer(col,",");
			if  (coltoken.countTokens() > 0) {
				columns = new int[coltoken.countTokens()];
			}
			int i= 0;
			while (coltoken.hasMoreTokens()) {
				columns[i] = Integer.parseInt(coltoken.nextToken());
				i++;
			}
		} else columns = new int[0];
		System.out.println("Processing.. "+toString());
	}
	
	public String toString() {
		StringBuffer stb = new StringBuffer("");
		if (columns != null)  {
			for (int i=0; i< columns.length; i++) {
				if (i < (columns.length - 1))  {
					stb.append(columns[i]).append(",");
				}	else stb.append(columns[i]);
			}
		}
		return "Case id=["+id+"],Proc(sy/ora)=["+sproc+"/"+oproc+"],SumCol=["+stb+"]";
	}
}