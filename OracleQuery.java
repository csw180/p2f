import java.sql.*;
import java.io.*;
import java.util.*;

public class OracleQuery {
	
	static final String TPA_URL = "jdbc:oracle:thin:@//138.240.38.31:1523/DBDTPA";
	static final String TPA_id = "TPAPRC";
	static final String TPA_pass = "12dec*ta*d";

	static final String KPI_URL = "jdbc:oracle:thin:@//138.240.38.31:1523/DBDKPI";
	static final String KPI_id = "KPIPRC";
	static final String KPI_pass = "12dec*ki*d";
	
	String   target;
	Connection  conn;
	Statement   stmt;
	ResultSet   rs;
	
	OracleQuery(String t) {
		target = t;
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		connect();
	}
	
	private void connect() {
		try {
			if  (target.equals("TPA"))  {
				conn = DriverManager.getConnection(TPA_URL,TPA_id,TPA_pass);
			} else if (target.equals("KPI"))  {
				conn = DriverManager.getConnection(KPI_URL,KPI_id,KPI_pass);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void execute(File f)  throws FileNotFoundException, IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		PrintWriter pw = new PrintWriter(new FileWriter(new File(".//odata//result_"+target+".dat")));
		String line;
		String result;
		int   cnt =0;
		while (  (line = br.readLine()) != null) {
			cnt++;
			Proc proc = new Proc(line);
			if  (cnt > 50) {
				release();
				connect();
				cnt = 0;
			}
			if  (!proc.skip)  result = doQuery(proc.id,proc.oproc,proc.columns);
			else result = proc.id+"|"+proc.oproc+"|SKIPPED";
			pw.println(result);
		}
		br.close();
		pw.close();
	}

	public String doQuery(String id, String sql, int[] sumcols) {  // 파일이름에 사용하는 case id, 프로시져호출문, sum 값을 구할 칼럼의 순서값 배열
		String result_msg = null;
		PrintWriter pw = null;
		try {
			File outf = new File(".//odata//"+id+".dat");
			if (!outf.exists()) {
				outf.createNewFile();
			}

			FileWriter fw = new FileWriter(outf);
			pw = new PrintWriter(fw);
			
			String lsql = "{call "+sql+"}";
			CallableStatement stmt = conn.prepareCall(lsql);
			stmt.registerOutParameter(1,oracle.jdbc.OracleTypes.CURSOR);
			long start = System.currentTimeMillis();
			stmt.execute();
			long end = System.currentTimeMillis();
			
			ResultSet  rs = (ResultSet)stmt.getObject(1);

			ResultSetMetaData meta = rs.getMetaData();
			int cnt  =  meta.getColumnCount();
			StringBuffer colnameBuf = new StringBuffer();
			for (int i=1; i<=cnt-1;i++) {
				if ( contains(sumcols,i))
					colnameBuf.append(String.valueOf(i)+"."+meta.getColumnName(i)).append('|');
				else
					colnameBuf.append(meta.getColumnName(i)).append('|');
			}
			
			if ( contains(sumcols,cnt) )
				colnameBuf.append(String.valueOf(cnt)+"."+meta.getColumnName(cnt));
			else
				colnameBuf.append(meta.getColumnName(cnt));

			pw.println(colnameBuf);

			StringBuffer buf;
			long rec_cnt = 0;
			double[] sums = new double[sumcols.length];

			while ( rs.next()) {
				buf = new StringBuffer();

				for(int j = 0; j<sumcols.length; j++) {
					sums[j] += rs.getDouble(sumcols[j]);
				}
				rec_cnt++;
				for(int i=1; i<cnt;i++) {
					buf.append(rs.getString(i)).append("|");
				}
				buf.append(rs.getString(cnt));
				pw.println(buf);
			}
			StringBuffer sums_buf = new StringBuffer("");
			if ( sums.length > 0) {
				for(int i=0; i<sums.length; i++) {
					if (i<sums.length-1) sums_buf.append(String.valueOf(sumcols[i])).append('|').append(String.valueOf(sums[i])).append('|');
					else sums_buf.append(String.valueOf(sumcols[i])).append('|').append(String.valueOf(sums[i]));
				}
			}
			result_msg = id+"| |"+sql+"|"+String.valueOf( (end-start)/1000.0)+"|"+String.valueOf(rec_cnt)+"|"+sums_buf.toString();
		} catch (SQLException e) {
			System.out.println(e.getMessage());
			result_msg = id+"| |"+sql+"|ERROR";
		} catch (IOException e) {
			e.printStackTrace();
			result_msg = id+"| |"+sql+"|IOERROR";
		} finally {
			pw.close();
			try { if (rs!=null) rs.close(); } catch (SQLException e) {}
			try { if (stmt!=null) stmt.close(); } catch (SQLException e) {}
			if  ( result_msg == null ) result_msg = id+"| |"+sql;
			return result_msg;
		}
	}

	public void release() {
		try { if (conn!=null) conn.close(); } catch (SQLException e) {}
	}

	public static boolean contains(int[] arr, int num) {
		for (int i=0;i<arr.length;i++) {
			if  (arr[i]==num) return true;
		}
		return false;
	}
	
	public  static void main(String args[]) {
		if  (args.length != 1) {
			System.out.println("Usage: OracleQuery [TPA | KPI]");
			return;
		}
		
		OracleQuery app =  new OracleQuery(args[0]);
		try {
			app.execute(new File(".//procs.dat"));
		} catch (FileNotFoundException e ) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		app.release();
	}

}