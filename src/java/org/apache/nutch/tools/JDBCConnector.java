package org.apache.nutch.tools;

import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.log.Log;

public class JDBCConnector {
	private static Configuration conf;

	static {
		conf = NutchConfiguration.create();
	}

	public static Connection getConnection() {

		Connection conn = null;
		Properties connectionProps = new Properties();
		connectionProps.put("user", conf.get("dbConfig.user"));
		connectionProps.put("password", conf.get("dbConfig.password"));
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			conn = DriverManager.getConnection(conf.get("dbConfig.dataSource"),
					connectionProps);

		} catch (SQLException e) {
			Log.info(
					"Error while creating DB Connection"
							+ e.getLocalizedMessage(), e);
		} catch (ClassNotFoundException e) {
			Log.info(
					"Error while creating DB connection"
							+ e.getLocalizedMessage(), e);
		}

		System.out.println("Connected to database" + conn);
		return conn;
	}
}
