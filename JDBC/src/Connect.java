import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.Scanner;

public class Connect 
{
	public static void delete_tables(java.sql.Statement stmt) throws SQLException
	{
		stmt.execute("TRUNCATE TABLE history");
		stmt.execute("SET FOREIGN_KEY_CHECKS = 0;");
		stmt.execute("SET UNIQUE_CHECKS = 0;");
		stmt.execute("SET sql_log_bin = 0");
	}
	
	public static int Kontostand_TX(java.sql.Statement stmt, int ACCID) throws SQLException
	{
		ResultSet rs = stmt.executeQuery("select balance from accounts where accid="+ACCID+";");
		rs.first();
		return rs.getInt(1);
	}
	
	public static int Einzahlungs_TX(java.sql.Statement stmt, int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException
	{
		stmt.executeUpdate("update branches set balance=balance+"+DELTA+" where branchid="+BRANCHID+";");
		stmt.executeUpdate("update tellers set balance=balance+"+DELTA+" where tellerid="+TELLERID+";");
		stmt.executeUpdate("update accounts set balance=balance+"+DELTA+" where accid="+ACCID+";");
		ResultSet rs = stmt.executeQuery("select balance from accounts where accid="+ACCID+";");
		stmt.setQueryTimeout(998493834);
		rs.first();
		int newbalance = rs.getInt(1) + DELTA;
		stmt.executeUpdate("insert into history values ("+ ACCID +","+ TELLERID +","+ DELTA +","+ BRANCHID +","+ newbalance +",'Einzahlung')");
		
		return newbalance;
	}
	
	public static int Analyse_TX(java.sql.Statement stmt, int DELTA) throws SQLException
	{
		ResultSet rs = stmt.executeQuery("select count(accid) from benchmark.history where delta="+ DELTA +";");
		rs.first();
		return rs.getInt(1);
	}
	
	private static int rand()
	{
		Random rnd = new Random();
		int x = rnd.nextInt(100) + 1;
		if(x<= 50)
			return 1;
		if(x<=85)
			return 2;
		return 3;
	}
	
	public static void main(String[] args) throws SQLException
	{
		int iTxCount = 0;
		long tnow, tend, tbeg;
		java.sql.Statement stmt = null;
		
		Scanner scr = new Scanner(System.in);
		final int iGesamtZeit = 60000;
		final int iEinschwingZeit = 24000;
		final int iAusschwingZeit = 54000;
		String user = "root";
		String pass = "janbe2013";
		String DB = "benchmark";
		System.out.print("DB-Server: ");
		String ConnectionName = "jdbc:mysql://"+scr.nextLine()+"/" + DB;
		scr.close();
		
		Connection con = DriverManager.getConnection(ConnectionName, user, pass);
		stmt = con.createStatement();
		
		//Commit der Datenbank erst am Ende der Operationen
		//con.setAutoCommit(false);
		//create_table(stmt);
		delete_tables(stmt);
		Random r = new Random();
		tend = System.currentTimeMillis() + iGesamtZeit;
		tnow = System.currentTimeMillis();
		tbeg = tnow;
		
		while(tnow < tend )
		{
			switch(rand())
			{
				case 1:
					Kontostand_TX(stmt, r.nextInt(10000)+1);
					tnow = System.currentTimeMillis(); 
					if((tnow - tbeg) > iEinschwingZeit && (tnow - tbeg) < iAusschwingZeit)
						iTxCount++;
					break;
				case 2:
					Einzahlungs_TX(stmt, r.nextInt(10000)+1, r.nextInt(10000)+1, r.nextInt(10000)+1, r.nextInt(10000)+1);
					if((tnow - tbeg) > iEinschwingZeit && (tnow - tbeg) < iAusschwingZeit)
						iTxCount++;
					break;
				case 3:
					Analyse_TX(stmt, r.nextInt(10000)+1);
					if((tnow - tbeg) > iEinschwingZeit && (tnow - tbeg) < iAusschwingZeit)
						iTxCount++;
					break;
			}
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("Tx  : " + iTxCount);
		System.out.println("TxPS: "+ java.lang.Math.round(iTxCount/((iAusschwingZeit-iEinschwingZeit)/1000)));
		stmt.close();
		con.close();
	}
}
