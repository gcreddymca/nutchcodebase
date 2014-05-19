package org.apache.nutch.crawl.dao;

import org.apache.nutch.tools.JDBCConnector;

import org.apache.nutch.crawl.vo.UrlVO;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDAO {
	
	public static final Logger LOG = LoggerFactory
			.getLogger(UrlDAO.class);

	
	public boolean create(List<UrlVO> urlVOs) {
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		int maxStatements = 1000;
		int count = 0;
		try {
			LOG.info("Connected to UrlCRUD DB");
			String query = "INSERT INTO URL_DETAIL (URL,STATUS,FETCH_TIME,MODIFIED_TIME,RETRIES_SINCE_FETCH,RETRY_INTERVAL,SCORE,SIGNATURE,METADATA,SEGMENT_ID) VALUES(?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(query);

			for (UrlVO url : urlVOs) {
				stmt.setString(1, url.getUrl());
				stmt.setInt(2, url.getStatus());
				stmt.setTimestamp(3, new Timestamp( url.getFetchTime()));
				stmt.setTimestamp(4, new Timestamp( url.getModifiedTime()));
				stmt.setInt(5, url.getRetriesSinceFetch());
				stmt.setLong(6, url.getFetchInterval());
				stmt.setFloat(7, url.getScore());
				stmt.setString(8, url.getSignature());
				stmt.setString(9, url.getMetadata());
				stmt.setInt(10, url.getSegmentId());
				stmt.addBatch();
				count++;
				if (count == urlVOs.size() || count == maxStatements) {
					stmt.executeBatch();
					LOG.info("Batch job executed on URL_DETAIL");
					count=0;
				}
				
				
			}
			result = true;
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info(
						"Error while rolling back"
								+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while creating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {

				try {
					try{
						conn.commit();
					}catch(SQLException s){
						LOG.info(
								"Error while rolling back"
										+ s.getLocalizedMessage(), s);
					}
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection", e);
				}

			}
		}
		return result;

	}

	public boolean createUrlDetail(UrlVO url) {
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		try {
			String query = "INSERT INTO URL_DETAIL (URL,STATUS,FETCH_TIME,MODIFIED_TIME,RETRIES_SINCE_FETCH,RETRY_INTERVAL,SCORE,SIGNATURE,METADATA,SEGMENT_ID) VALUES(?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(query);

			stmt.setString(1, url.getUrl());
			stmt.setInt(2, url.getStatus());
			stmt.setDate(3, new Date( url.getFetchTime()));
			stmt.setDate(4, new Date( url.getModifiedTime()));
			stmt.setInt(5, url.getRetriesSinceFetch());
			stmt.setLong(6, url.getFetchInterval());
			stmt.setFloat(7, url.getScore());
			stmt.setString(8, url.getSignature());
			stmt.setString(9, url.getMetadata());
			stmt.setInt(10, url.getSegmentId());
			stmt.execute();
			result = true;
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info(
						"Error while rolling back"
								+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while creating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {

				try {
					try{
						conn.commit();
					}catch(SQLException s){
						LOG.info(
								"Error while rolling back"
										+ s.getLocalizedMessage(), s);
					}
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection", e);
				}

			}
		}
		return result;

	}

	public boolean update(List<UrlVO> urlVOs) {
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		int maxStatements = 1000;
		int count = 0;
		try {
			String query = "UPDATE URL_DETAIL SET STATUS=?,FETCH_TIME=?,MODIFIED_TIME=?,RETRIES_SINCE_FETCH=?,RETRY_INTERVAL=?,SCORE=?,SIGNATURE=?,METADATA=? WHERE URL=?";
			stmt = conn.prepareStatement(query);

			for (UrlVO url : urlVOs) {
				stmt.setInt(1, url.getStatus());
				stmt.setTimestamp(2, new Timestamp( url.getFetchTime()));
				stmt.setTimestamp(3, new Timestamp( url.getModifiedTime()));
				stmt.setInt(4, url.getRetriesSinceFetch());
				stmt.setLong(5, url.getFetchInterval());
				stmt.setFloat(6, url.getScore());
				stmt.setString(7, url.getSignature());
				stmt.setString(8, url.getMetadata());
				stmt.setString(9, url.getUrl());
				//stmt.setInt(10, url.getSegmentId());
				stmt.addBatch();
				count++;
				if (count == urlVOs.size() || count == maxStatements) {
					stmt.executeBatch();
					LOG.info("Batch job executed on URL_DETAIL");
					count=0;
				}
			}
			result=true;
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info(
						"Error while rolling back"
								+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while updating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {

				try {
					try{
						conn.commit();
					}catch(SQLException s){
						LOG.info(
								"Error while rolling back"
										+ s.getLocalizedMessage(), s);
					}
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection", e);
				}

			}
		}
		return result;

	}
	
	public boolean delete(){
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not delete rows in URL_DETAIL");
			return result;
		}
		Statement stmt = null;
		int maxStatements = 1000;
		int count = 0;
		try {
			String query = "DELETE FROM URL_DETAIL";
			stmt = conn.createStatement();
			stmt.execute(query);
			result=true;
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info(
						"Error while rolling back"
								+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while deleting rows in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {

				try {
					try{
						conn.commit();
					}catch(SQLException s){
						LOG.info(
								"Error while rolling back"
										+ s.getLocalizedMessage(), s);
					}
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection", e);
				}

			}
		}
		return result;
	}
	
}
