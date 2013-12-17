import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

public class Connect implements Runnable
{
	//Global
	private Boolean bRun;
	private Thread runner;
	private ServerSocket ss;
	private List<Socket> Clients;
	private List<ObjectOutputStream> Outputs;
	private List<ObjectInputStream> Inputs;
	
	
	public Connect() throws SQLException, ClassNotFoundException, IOException
	{
		bRun = true;
		runner = new Thread(this);
		Clients = new ArrayList<Socket>();
		Outputs = new ArrayList<ObjectOutputStream>();
		Inputs = new ArrayList<ObjectInputStream>();
		doSome();
	}
	
	public void doSome() throws SQLException, IOException, ClassNotFoundException
	{
		int iTxCount = 0;
		 int iPercent = 0;
		long tnow, tend, tbeg;
		Boolean bIsServer = false;
		java.sql.Statement stmt = null;
		Socket Client = null;
		
		Scanner scr = new Scanner(System.in);
		final int iGesamtZeit = 60000;
		final int iEinschwingZeit = 24000;
		final int iAusschwingZeit = 54000;
		String user = "root";
		String pass = "janbe2013";
		String DB = "benchmark";
		System.out.print("DB-Server  : ");
		String ConnectionName = "jdbc:mysql://"+scr.nextLine()+"/" + DB;
		System.out.print("Data-Server: ");
		try
		{
			Client = new Socket(scr.nextLine(), 1235);
			System.out.println("Connect as client");
		}
		catch(Exception e)
		{
			System.out.println("Connect as server");
			bIsServer = true;
			ss = new ServerSocket(1235);
			runner.start();
		}
		
		
		if(bIsServer)
		{
			System.out.println("Press enter to start...");
			System.in.read();
			bRun = false;
			for(int i=0;i<Outputs.size();i++)
			{
				Outputs.get(i).writeObject("Go");
			}
		}
		else
		{
			ObjectInputStream ois = new ObjectInputStream(Client.getInputStream());
			ois.readObject();
			System.out.println("\n");
		}
		System.out.println("Server sended go");
		scr.close();
		Connection con = DriverManager.getConnection(ConnectionName, user, pass);
		stmt = con.createStatement();
		
		System.out.print("|1%_____________________50%____________________100%|\n|");
		delete_tables(stmt);
		Random r = new Random();
		tend = System.currentTimeMillis() + iGesamtZeit;
		tnow = System.currentTimeMillis();
		tbeg = tnow;
		
		while(tnow < tend )
		{
			tnow = System.currentTimeMillis(); 
			switch(rand())
			{
				case 1:
					Kontostand_TX(stmt, r.nextInt(10000)+1);
					break;
				case 2:
					Einzahlungs_TX(stmt, r.nextInt(10000)+1, r.nextInt(10000)+1, r.nextInt(10000)+1, r.nextInt(10000)+1);
					break;
				case 3:
					Analyse_TX(stmt, r.nextInt(10000)+1);
					break;
			}
			if((tnow - tbeg) > iEinschwingZeit && (tnow - tbeg) < iAusschwingZeit)
				iTxCount++;
			int xx = (iGesamtZeit - (int)(tend-tnow))*100/iGesamtZeit;
			if(xx > iPercent)
            {
                    if(xx%2 == 0)
                            System.out.print("=");
                    iPercent = xx;
            }
			try 
			{
				Thread.sleep(50);
			}
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
		System.out.println("|\n\n");
		if(bIsServer)
		{
			int txCount = 0;
			
			try 
			{
				Thread.sleep(1000);
			}
			catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
			for(ObjectInputStream is : Inputs)
			{
				DataInputStream dit = new DataInputStream(is);
				txCount += dit.readInt();
				dit.close();
				is.close();
			}
			for(ObjectOutputStream os : Outputs)
			{
				os.close();
			}
			for(Socket s : Clients)
			{
				s.close();
			}
			
			System.out.println("Tx  : " + (iTxCount + txCount));
			System.out.println("TxPS: "+  ((iTxCount + txCount) / ((iAusschwingZeit-iEinschwingZeit)/1000))   );
		}
		else
		{
			ObjectOutputStream oss = new ObjectOutputStream(Client.getOutputStream());
			DataOutputStream dot = new DataOutputStream(oss);
			dot.writeInt(iTxCount);
			dot.close();
			oss.close();
			System.out.println("Ended");
		}

		
		for(int i=0;i<Clients.size();i++)
		{
			Outputs.get(i).close();
			Inputs.get(i).close();
			Clients.get(i).close();
		}
		stmt.close();
		con.close();
	}
	
	public void run()
	{
		while(bRun)
		{
			try 
			{
				Clients.add(ss.accept());
				ObjectOutputStream os = new ObjectOutputStream(Clients.get(Clients.size()-1).getOutputStream());
				Outputs.add(os);
				ObjectInputStream is = new ObjectInputStream(Clients.get(Clients.size()-1).getInputStream());
				Inputs.add(is);
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}
	
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
		ResultSet rs = stmt.executeQuery("SELECT Einzahlungs_TX("+ACCID+","+TELLERID+","+BRANCHID+","+DELTA+");");
		rs.first();
		return rs.getInt(1) + DELTA;
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
		try {
			@SuppressWarnings("unused")
			Connect conc = new Connect();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	}
}
