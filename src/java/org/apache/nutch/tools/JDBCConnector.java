package org.apache.nutch.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.log.Log;

public class JDBCConnector {
	private static Configuration conf;
	static InitialContext ctx = null;
	static DataSource ds = null;
	static Connection con = null;
	static Properties prop = new Properties(); 
	static {
		conf = NutchConfiguration.create();
	}

	public static Connection getRegularConnection() {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", conf.get("dbConfig.user"));
		connectionProps.put("password", conf.get("dbConfig.password"));
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(conf.get("dbConfig.dataSource"),	connectionProps);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			Log.warn("Error while creating DB Connection"+ e.getLocalizedMessage());
		} catch (ClassNotFoundException e) {
			Log.warn("Error while creating DB connection"+ e.getLocalizedMessage(), e);
		} catch(Exception e){
			Log.warn(e.getLocalizedMessage());
		}
		Log.info("Connected to database" + conn);
		return conn;
	}
	
	public static Connection getSimpleConnection() {
		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", "CREST");
		connectionProps.put("password", "CREST");
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:XE",	connectionProps);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			Log.warn("Error while creating DB Connection"+ e.getLocalizedMessage());
		} catch (ClassNotFoundException e) {
			Log.warn("Error while creating DB connection"+ e.getLocalizedMessage(), e);
		} catch(Exception e){
			Log.warn(e.getLocalizedMessage());
		}
		Log.info("Connected to database" + conn);
		return conn;
	}
	
	public static Connection getConnection() {
		try {
			prop.load(new FileInputStream(System.getProperty("DBProperties")));
			ctx = new InitialContext();
			ds = (DataSource) ctx.lookup(prop.getProperty("JNDI_NAME"));
			con = ds.getConnection();
			con.setAutoCommit(false);
		} catch (NamingException e) {
			Log.info(e.getMessage());
		} catch (SQLException e) {
			Log.info(e.getMessage());
		} catch (FileNotFoundException e) {
			Log.info(e.getMessage());
		} catch (IOException e) {
			Log.info(e.getMessage());
		}
		return con;
	}
}
