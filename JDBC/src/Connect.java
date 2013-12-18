import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
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

/**
 * Dies ist der Load-Driver zur DBI-Praktikumsaufgbe 9
 * Es enhält den Clienten und Server
 * 
 * @author Benedikt Oppenberg, Jannik Ewers
 * @version 1.0
 */


public class Connect implements Runnable
{
	//Global
	private Boolean bRun;
	private Thread runner;
	private ServerSocket ss;
	private List<Socket> Clients;
	private List<InputStreamReader> Inputs;
	private List<PrintStream> Outputs;
	


    /** 
     * Konstruktor
     */
	public Connect() throws SQLException, ClassNotFoundException, IOException
	{
		//Init
		bRun = true;
		runner = new Thread(this);
		Clients = new ArrayList<Socket>();
		Inputs = new ArrayList<InputStreamReader>();
		Outputs = new ArrayList<PrintStream>();
		doSome();
	}
	
	/** 
     * Steuerung des Load-Drivers
     * Erzueugung des Clienten und/oder Servers
     */
	public void doSome() throws SQLException, IOException, ClassNotFoundException
	{
		//Init local vars
		int iTxCount = 0;
		int iPercent = 0;
		long tnow, tend, tbeg;
		Boolean bIsServer = false;
		java.sql.Statement stmt = null;
		Socket Client = null;
		Scanner scr = new Scanner(System.in);
		//Init final vars 
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
			//Versucht eine Verbindung zu dem Messdaten-Server aufzubauen
			Client = new Socket(scr.nextLine(), 1235);
			System.out.println("Connect as client");
		}
		catch(Exception e)
		{
			//Falls keine Verbindung hergestellt werden kann wird ein Server erstellt
			System.out.println("Connect as server");
			bIsServer = true;
			ss = new ServerSocket(1235);
			//Starte Thread
			runner.start();
		}
		
		
		if(bIsServer)
		{
			//Warten bis alle Clients verbunden sind
			System.out.println("Press enter to start...");
			System.in.read();
			//Thread schließen
			bRun = false;
			new Socket(ss.getInetAddress(), ss.getLocalPort()).close();
			//Allen Clienten das Startsignal senden
			for(PrintStream ps : Outputs)
			{
				ps.print("Go");
			}
		}
		else
		{
			//Auf das Startsignal warten
			DataInputStream ois = new DataInputStream(Client.getInputStream());
			ois.read();
			System.out.println("\n");
		}
		System.out.println("Server sended go");
		
		//Verbindung zum DB-Server herstellen
		Connection con = DriverManager.getConnection(ConnectionName, user, pass);
		stmt = con.createStatement();
		
		System.out.print("|1%_____________________50%____________________100%|\n|");
		//Tabelle history leeren
		trunc_table(stmt);
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
			int progress = (iGesamtZeit - (int)(tend-tnow))*100/iGesamtZeit;
			if(progress > iPercent)
            {
                    if(progress%2 == 0)
                            System.out.print("=");
                    iPercent = progress;
            }
			//Nachdenkzeit
			try {Thread.sleep(50);}catch (InterruptedException e) {e.printStackTrace();}
		}
		System.out.println("|\n\n");
		
		if(bIsServer)
		{
			//Auf die Clienten Warten
			try {Thread.sleep(5000);}catch (InterruptedException e){e.printStackTrace();}
			int txCount = 0;
			//Daten von den Clienten holen
			for(InputStreamReader dis : Inputs)
			{
				BufferedReader bufferedReader = new BufferedReader(dis);
				char[] buffer = new char[20];
		        int anzahlZeichen = bufferedReader.read(buffer, 0, 20);
		        String nachricht = new String(buffer, 0, anzahlZeichen);
		        txCount += Integer.parseInt(nachricht);
			}
			
			System.out.println("Cleints: " + (Clients.size()+1));
			System.out.println("Tx     : " + (iTxCount + txCount));
			System.out.println("TxPS   : "+  ((iTxCount + txCount) / ((iAusschwingZeit-iEinschwingZeit)/1000))   );
		}
		else
		{
			//Daten an den Server senden
			PrintStream dot = new PrintStream(Client.getOutputStream());
			dot.print(iTxCount);
			System.out.println("Ended");
			scr.next();
		}
		scr.close();
		stmt.close();
		con.close();
	}
	
	/** 
    * Wartet auf neue Clienten.
    * Wenn eine verbindung zu einem Clienten hergestellt wurde,
    * wird er und seine Streams in separate Listen eingereiht 
    */
	public void run()
	{
		while(bRun)
		{
			try 
			{
				Clients.add(Clients.size(), ss.accept());
				if(bRun)
				{
					Inputs.add(Inputs.size(), new InputStreamReader( Clients.get(Clients.size()-1).getInputStream()));				
					Outputs.add(Outputs.size(), new PrintStream( Clients.get(Clients.size()-1).getOutputStream()));
					System.out.println("Client connected " + Clients.size());
				}
				else
				{
					Clients.remove(Clients.size()-1);
				}
			} 
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	/** 
	* Leert die Tabelle history und setzt Systemvariablen im DBMS
	* 
	* @param stmt Das SQL-Statement, was mit dem DB-Server verbunden ist
	*/
	public static void trunc_table(java.sql.Statement stmt) throws SQLException
	{
		stmt.execute("TRUNCATE TABLE history");
		stmt.execute("SET FOREIGN_KEY_CHECKS = 0;");
		stmt.execute("SET UNIQUE_CHECKS = 0;");
		stmt.execute("SET sql_log_bin = 0");
	}
	
	/** 
	* Holt den Kontostand des Accounts von der DB
	* 
	* @param stmt Das SQL-Statement, was mit dem DB-Server verbunden ist
	* @param ACCID Die accid, von dem der kontostand geholt werden soll
	* @return Kontostand des Accounts
	* @throws SQLException Fehler beim Verarbeiten
	*/
	public static int Kontostand_TX(java.sql.Statement stmt, int ACCID) throws SQLException
	{
		ResultSet rs = stmt.executeQuery("select balance from accounts where accid="+ACCID+";");
		rs.first();
		return rs.getInt(1);
	}
	
	/** 
	* Zahlt in ein konto ein
	* 
	* @param stmt Das SQL-Statement, was mit dem DB-Server verbunden ist
	* @param ACCID Der Account, von dem der Kontostand geholt werden soll
	* @param TELLERID  ID des Geldautomaten
	* @param BRANCHID ID der Zweigstelle
	* @param DELTA Betrag der Einzahlung
	* @return Kontostand des Accounts
	* @throws SQLException Fehler beim Verarbeiten
	*/
	public static int Einzahlungs_TX(java.sql.Statement stmt, int ACCID, int TELLERID, int BRANCHID, int DELTA) throws SQLException
	{
		ResultSet rs = stmt.executeQuery("SELECT Einzahlungs_TX("+ACCID+","+TELLERID+","+BRANCHID+","+DELTA+");");
		rs.first();
		return rs.getInt(1) + DELTA;
	}
	
	/** 
	* Holt die Anzahl der Eintrage in history mit dem gegebene DELTA von der DB
	* 
	* @param stmt Das SQL-Statement, was mit dem DB-Server verbunden ist
	* @param DELTA Das DELTA, welches in history gesucht werden soll
	* @return Anzahl der gefundenen Ergebnisse
	* @throws SQLException Fehler beim Verarbeiten
	*/
	public static int Analyse_TX(java.sql.Statement stmt, int DELTA) throws SQLException
	{
		ResultSet rs = stmt.executeQuery("select count(accid) from benchmark.history where delta="+ DELTA +";");
		rs.first();
		return rs.getInt(1);
	}
	
	/**
	 * Diese Methode gibt eine Gewichtet Zufallszahl zwischen 1 und 3 zurück,
	 * mit einer Gewichtung von (35 : 50 : 15)
	 * 
	 * @return Gewichtete Zufallszahl
	 */
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
	
	/**
	 * Erzeugt ein neues Object von der Connect Klasse
	 * 
	 * @param args	Kommandozeilenparameter
	 * @throws SQLException Fehler beim Verarbeiten
	 */
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
