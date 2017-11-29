package de.htwberlin.mauterhebung;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import de.htwberlin.exceptions.AlreadyCruisedException;
import de.htwberlin.exceptions.DataException;
import de.htwberlin.exceptions.InvalidVehicleDataException;
import de.htwberlin.exceptions.UnkownVehicleException;

/**
 * Die Klasse realisiert den AusleiheService.
 * 
 * @author Patrick Dohmeier
 */
public class MauterServiceImpl implements IMauterhebung {

	private static final Logger L = LoggerFactory.getLogger(MauterServiceImpl.class);
	private Connection connection;

	@Override
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new DataException("Connection not set");
		}
		return connection;
	}

	@Override
	public float berechneMaut(int mautAbschnitt, int achszahl, String kennzeichen)
			throws UnkownVehicleException, InvalidVehicleDataException, AlreadyCruisedException {
		if(!isVehicleRegistered(kennzeichen)){
			throw new UnkownVehicleException();
		}
		if(!isVehicleDataRight(achszahl,kennzeichen, mautAbschnitt)){
			throw new InvalidVehicleDataException();
		}
		if(isVehicleInManual(kennzeichen)){
			//Manual Ablauf
			if(!isRouteAlreadyCruised(kennzeichen,mautAbschnitt)) {
				throw new AlreadyCruisedException();
			}
			updateCategory(kennzeichen);
			
		}
		if(isVehicleInAutomatic(kennzeichen)){
			// Automatic Ablauf
		}
		
		
		return 0;
	}


	/**
	 * prueft, ob Fahrzeug vorhanden ist  
	 * @author KingKoolKai
	 * @param kennzeichen Das Kennzeichen des Fahrzeugs das geprüft wird.
	 * @return true oder false 
	 */
	
	public boolean isVehicleRegistered(String kennzeichen){

		// Resultset.count() anwenden um testen ob positiv oder negativ ist, Ergebnis vorhanden oder nicht
		PreparedStatement preparedState = null;
		ResultSet result = null;
		boolean check = false;
		String query = "SELECT SUM(Anzahl) AS ANZAHL_FZ	FROM(SELECT count(F.FZ_ID) as Anzahl FROM FAHRZEUG F JOIN FAHRZEUGGERAT FZG ON F.FZ_ID = FZG.FZ_ID WHERE F.KENNZEICHEN = ?  AND F.ABMELDEDATUM IS NULL AND FZG.STATUS = 'active' union SELECT count(KENNZEICHEN) AS Anzahl FROM BUCHUNG WHERE b_ID = 1 and Kennzeichen = ?)";
		
		try {
			preparedState = getConnection().prepareStatement(query);
			preparedState.setString(1, kennzeichen);
			preparedState.setString(2, kennzeichen);
			result = preparedState.executeQuery();
			if(result.next()){
			
			return result.getInt("ANZAHL_FZ")>0;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return check;
	}
	public boolean isVehicleDataRight(int Achsen, String Kennzeichen, int abschnitt) {
		PreparedStatement preparedState = null;
		ResultSet result = null;
		boolean check = false;
		String queryAM = "SELECT COUNT(ACHSEN) AS Anzahl FROM FAHRZEUG WHERE KENNZEICHEN = ? AND ACHSEN = ?";
		String queryMA = "SELECT M.ACHSZAHL FROM BUCHUNG INNER JOIN MAUTKATEGORIE M ON M.KATEGORIE_ID = BUCHUNG.KATEGORIE_ID WHERE BUCHUNG.KENNZEICHEN = ? AND BUCHUNG.ABSCHNITTS_ID = ? AND BUCHUNG.BEFAHRUNGSDATUM IS NULL";
		
		try {
			preparedState = getConnection().prepareStatement(queryAM);
			preparedState.setString(1, Kennzeichen);
			preparedState.setInt(2, Achsen);
			result = preparedState.executeQuery();
			if(result.next()){
				if( result.getInt("Anzahl") == 0) {
					//Wenn Automatische Verfahren Prüfung negativ ist, Manuelle Prüfung durchführen (Achszahl von der Buchungs mautkategorie mit Achszahl prüfen)
					preparedState = getConnection().prepareStatement(queryMA);
					preparedState.setString(1, Kennzeichen);
					preparedState.setInt(2, abschnitt);
					result = preparedState.executeQuery();
					if(result.next()){
						String achszahlDB = result.getString(1);
						int achszahlDBzahl = Integer.parseInt(achszahlDB.substring(achszahlDB.length()-1));
						if(achszahlDB.contains(">=")) {
							return Achsen >= achszahlDBzahl;
						}
						if(achszahlDB.contains("=")) {
							System.out.println("hier1");
							return Achsen == achszahlDBzahl;
						}
						
					}
					
			} else {
				return result.getInt("Anzahl") > 0;
			}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("Hier");

		return check;
	}
	public boolean isVehicleInManual(String Kennzeichen){
		PreparedStatement preparedState = null;
		ResultSet result = null;
		boolean check = false;
		String query = "SELECT COUNT(BS.STATUS) AS Anzahl FROM BUCHUNGSTATUS BS INNER JOIN BUCHUNG B ON B.B_ID = BS.B_ID WHERE B.KENNZEICHEN = ? AND BS.STATUS='offen'";
		
		try {
			preparedState = getConnection().prepareStatement(query);
			preparedState.setString(1, Kennzeichen);
			result = preparedState.executeQuery();
			if(result.next()){
			return result.getInt("Anzahl")>0;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return check;
	}
	private boolean isVehicleInAutomatic(String Kennzeichen) {
		PreparedStatement preparedState = null;
		ResultSet result = null;
		boolean check = false;
		String query = "SELECT COUNT(FG.STATUS) AS Anzahl FROM FAHRZEUGGERAT FG INNER JOIN FAHRZEUG F ON F.FZ_ID = FG.FZ_ID WHERE F.KENNZEICHEN= ? AND FG.STATUS = 'active'";
		
		try {
			preparedState = getConnection().prepareStatement(query);
			preparedState.setString(1, Kennzeichen);
			result = preparedState.executeQuery();
			if(result.next()){
			return result.getInt("Anzahl")>0;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return check;
	}
	private boolean isRouteAlreadyCruised(String Kennzeichen, int Mautabschnitt) {
		PreparedStatement preparedState = null;
		ResultSet result = null;
		boolean check = false;
		String query = "SELECT COUNT(BS.STATUS) AS Anzahl FROM BUCHUNGSTATUS BS INNER JOIN BUCHUNG B ON B.B_ID = BS.B_ID WHERE B.KENNZEICHEN = ? AND B.ABSCHNITTS_ID = ? AND BS.STATUS = 'offen'";
		
		try {
			preparedState = getConnection().prepareStatement(query);
			preparedState.setString(1, Kennzeichen);
			preparedState.setInt(2, Mautabschnitt);
			result = preparedState.executeQuery();
			if(result.next()){
			return result.getInt("Anzahl")>0;
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return check;
	}
//	private boolean checkTollCategory(int Mautabschnitt, int Achszahl) {
//		
//	}
	private void updateCategory(String Kennzeichen) {
		
		// TO DO FINISH UPDATE WITH INNER JOIN
		PreparedStatement preparedState = null;
		boolean check = false;
				//UPDATE BUCHUNGSTATUS SET BUCHUNGSTATUS.STATUS = 'abgeschlossen' WHERE BUCHUNGSTATUS.STATUS = 'offen' AND EXISTS(SELECT * FROM BUCHUNG WHERE BEFAHRUNGSDATUM IS NULL AND KENNZEICHEN = ?) KRASSE QUERY
		String queryUPSTATUS = "UPDATE BUCHUNG SET BUCHUNG.B_ID = 3 WHERE BUCHUNG.KENNZEICHEN= ? AND BUCHUNG.BEFAHRUNGSDATUM IS NULL AND BUCHUNG.B_ID = 1";
		String queryUPBEFAHRUNGSDATUM = "UPDATE BUCHUNG  SET BUCHUNG.BEFAHRUNGSDATUM = CURRENT_DATE WHERE BUCHUNG.kennzeichen = ? AND BUCHUNG.BEFAHRUNGSDATUM IS NULL";
		try {
			preparedState = getConnection().prepareStatement(queryUPSTATUS);
			preparedState.setString(1, Kennzeichen);
			int anzahl = preparedState.executeUpdate();
			if(anzahl > 0) {
				preparedState = getConnection().prepareStatement(queryUPBEFAHRUNGSDATUM);
				preparedState.setString(1, Kennzeichen);
				anzahl = preparedState.executeUpdate();
			}


		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
}
