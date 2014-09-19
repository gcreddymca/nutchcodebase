package org.apache.nutch.crawl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.nutch.crawl.vo.UrlVO;
import org.apache.nutch.tools.JDBCConnector;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDAO {
	
	public static final Logger LOG = LoggerFactory
			.getLogger(UrlDAO.class);

	static int CONNECTIONCOUNT = 0;
	static int domainId = 0;
	public boolean create(Set<UrlVO> urlVOs,int domainId) {
		Connection conn = JDBCConnector.getConnection();
		
		CONNECTIONCOUNT++;
		int connectioncount=CONNECTIONCOUNT;
		int crawlId = getCrawlId(domainId);
		if(crawlId == 0){
			crawlId = getLiveCrawlId(domainId);
		}
		 
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		PreparedStatement checkstmt = null;
		PreparedStatement updatestmt = null;
		ResultSet urlResultset = null;
		int maxStatements = 1000;
		int count = 0;
		try {
			LOG.info("Connected to UrlCRUD DB");
			String checkquery = "SELECT * from URL_DETAIL where URL=? and CRAWL_ID=?";
			checkstmt = conn.prepareStatement(checkquery);
			
			String insertquery = "INSERT INTO URL_DETAIL (URL,STATUS,LAST_FETCH_TIME,LATEST_FETCH_TIME,MODIFIED_TIME,RETRIES_SINCE_FETCH,RETRY_INTERVAL,SCORE,SIGNATURE,METADATA,SEGMENT_ID,CRAWL_ID,DOMAIN_ID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String updatequery = "UPDATE URL_DETAIL SET MODIFIED_TIME=?, SEGMENT_ID=? where URL=? and CRAWL_ID=? and DOMAIN_ID=?";
			stmt = conn.prepareStatement(insertquery);
			updatestmt = conn.prepareStatement(updatequery);
			
			
			
			for (UrlVO url : urlVOs) {
				//Checking url is existed with crawlId 
				checkstmt.setString(1, url.getUrl());
				checkstmt.setInt(2, crawlId);
				urlResultset = checkstmt.executeQuery();
				
				if (urlResultset != null && urlResultset.next()) {
					//updatestmt.setTimestamp(1, new Timestamp( url.getLastFetchTime()));
					//updatestmt.setTimestamp(2, new Timestamp( url.getLatestFetchTime()));
					updatestmt.setTimestamp(1, new Timestamp( System.currentTimeMillis()));
					updatestmt.setInt(2, url.getSegmentId());
					updatestmt.setString(3, url.getUrl());
					updatestmt.setInt(4, crawlId);
					updatestmt.setInt(5, domainId);
					updatestmt.addBatch();
					
				}else{
					stmt.setString(1, url.getUrl());
					stmt.setInt(2, url.getStatus());
					stmt.setTimestamp(3, new Timestamp( url.getLastFetchTime()));
					stmt.setTimestamp(4, new Timestamp( url.getLatestFetchTime()));
					stmt.setTimestamp(5, new Timestamp( url.getModifiedTime()));
					stmt.setInt(6, url.getRetriesSinceFetch());
					stmt.setLong(7, url.getFetchInterval());
					stmt.setFloat(8, url.getScore());
					stmt.setString(9, url.getSignature());
					stmt.setString(10, url.getMetadata());
					stmt.setInt(11, url.getSegmentId());
					stmt.setInt(12,crawlId);
					stmt.setInt(13,domainId);
					stmt.addBatch();
				}
				count++;
				if (count == urlVOs.size() || count == maxStatements) {
					updatestmt.executeBatch();
					stmt.executeBatch();
					LOG.info("In Create Method");
					LOG.info("Batch job executed on URL_DETAIL");
					count=0;
				}
			}
			urlResultset.close();
			result = true;
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info("Error while rolling back"+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while creating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if(updatestmt != null){
				try {
					updatestmt.close();
				} catch (SQLException e) {
					LOG.error(e.getLocalizedMessage());
				}
			}
			if(checkstmt != null){
				try {
					checkstmt.close();
				} catch (SQLException e) {
					LOG.error(e.getLocalizedMessage());
				}
			}
			if (stmt != null) {
				try {
					try{
						conn.commit();
					}catch(SQLException s){
						conn.rollback();
						LOG.info("Error while committing"+ s.getLocalizedMessage(), s);
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
		int crawlId = getCrawlId(domainId);
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		try {
			String query = "INSERT INTO URL_DETAIL (URL,STATUS,LAST_FETCH_TIME,LATEST_FETCH_TIME,MODIFIED_TIME,RETRIES_SINCE_FETCH,RETRY_INTERVAL,SCORE,SIGNATURE,METADATA,SEGMENT_ID,CRAWL_ID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(query);
			stmt.setString(1, url.getUrl());
			stmt.setInt(2, url.getStatus());
			stmt.setTimestamp(3, new Timestamp( url.getLastFetchTime()));
			stmt.setTimestamp(4, new Timestamp( url.getLatestFetchTime()));
			stmt.setTimestamp(5, new Timestamp( url.getModifiedTime()));
			stmt.setInt(6, url.getRetriesSinceFetch());
			stmt.setLong(7, url.getFetchInterval());
			stmt.setFloat(8, url.getScore());
			stmt.setString(9, url.getSignature());
			stmt.setString(10, url.getMetadata());
			stmt.setInt(11, url.getSegmentId());
			stmt.setInt(12, crawlId);
			stmt.execute();
			result = true;
			conn.commit();
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info("Error while rolling back"+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while creating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection", e);
				}
			}
		}
		return result;
	}
	
	public boolean createSpecificUrlDetail(UrlVO url,int crawlId,int domainId) {
		Connection conn = JDBCConnector.getConnection();
		//int crawlId = getLiveCrawlId(domainId);
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in URL_DETAIL");
			return result;
		}
		PreparedStatement stmt = null;
		try {
			String query = "INSERT INTO URL_DETAIL (URL,STATUS,LAST_FETCH_TIME,LATEST_FETCH_TIME,MODIFIED_TIME,RETRIES_SINCE_FETCH,RETRY_INTERVAL,SCORE,SIGNATURE,METADATA,SEGMENT_ID,CRAWL_ID,DOMAIN_ID) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";
			stmt = conn.prepareStatement(query);
			stmt.setString(1, url.getUrl());
			stmt.setInt(2, 2);
			stmt.setTimestamp(3, new Timestamp( System.currentTimeMillis()));
			stmt.setTimestamp(4, new Timestamp( System.currentTimeMillis()));
			stmt.setTimestamp(5, new Timestamp( System.currentTimeMillis()));
			stmt.setInt(6, 0);
			stmt.setLong(7, 2592000);
			stmt.setFloat(8, 0.0f);
			stmt.setString(9, null);
			stmt.setString(10, null);
			stmt.setInt(11, url.getSegmentId());
			stmt.setInt(12, crawlId);
			stmt.setInt(13, domainId);
			stmt.execute();
			result = true;
			conn.commit();
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info("Error while rolling back"+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while creating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection", e);
				}
			}
		}
		return result;
	}

	public boolean update(Set<UrlVO> urlVOs) {
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
			String query = "UPDATE URL_DETAIL SET STATUS=?,LATEST_FETCH_TIME=?,MODIFIED_TIME=?,RETRIES_SINCE_FETCH=?,RETRY_INTERVAL=?,SCORE=?,SIGNATURE=?,METADATA=?,LAST_FETCH_TIME=? WHERE URL=?";
			stmt = conn.prepareStatement(query);

			for (UrlVO url : urlVOs) {
				stmt.setInt(1, url.getStatus());
				stmt.setTimestamp(2, new Timestamp( url.getLatestFetchTime()));
				stmt.setTimestamp(3, new Timestamp( url.getModifiedTime()));
				stmt.setInt(4, url.getRetriesSinceFetch());
				stmt.setLong(5, url.getFetchInterval());
				stmt.setFloat(6, url.getScore());
				stmt.setString(7, url.getSignature());
				stmt.setString(8, url.getMetadata());			
				stmt.setTimestamp(9, new Timestamp( url.getLastFetchTime()));
				stmt.setString(10, url.getUrl());
				//stmt.setInt(10, url.getSegmentId());
				stmt.addBatch();
				count++;
				if (count == urlVOs.size() || count == maxStatements) {
					stmt.executeBatch();
					LOG.info("In Update Method");
					LOG.info("Batch job executed on URL_DETAIL");
					count=0;
				}
			}
			result=true;
			conn.commit();
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info("Error while rolling back"+ s.getLocalizedMessage(), s);
			}
			LOG.info("Error while updating row in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection");
					e.printStackTrace();
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
			conn.commit();
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
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection", e);
				}

			}
		}
		return result;
	}
	
	public Map<String, UrlVO> read( int domainId){
		Connection conn = JDBCConnector.getConnection();
		Map<String,UrlVO> result = new HashMap<String,UrlVO>();
		if (conn == null) {
			LOG.info("Connection not found. Could not read rows from URL_DETAIL");
			return result;
		}
		Statement stmt = null;
		UrlVO urlVO = null;
		try {
			String query = "SELECT * FROM URL_DETAIL WHERE CRAWL_ID = "+getCrawlId(domainId)+" and DOMAIN_ID = "+domainId;
			stmt = conn.createStatement();
			stmt.execute(query);
			ResultSet rs = stmt.getResultSet();
			while(rs.next()){
				String url = rs.getString("URL");
				urlVO = new UrlVO();
				urlVO.setUrl(url);
				urlVO.setLatestFetchTime(rs.getTimestamp("LATEST_FETCH_TIME").getTime());
				urlVO.setLastFetchTime(rs.getTimestamp("LAST_FETCH_TIME").getTime());
				urlVO.setModifiedTime(rs.getTimestamp("MODIFIED_TIME").getTime());
				urlVO.setFetchInterval(rs.getInt("RETRY_INTERVAL"));
				urlVO.setRetriesSinceFetch(rs.getByte("RETRIES_SINCE_FETCH"));
				urlVO.setScore(rs.getFloat("SCORE"));
				urlVO.setStatus(rs.getByte("STATUS"));
				urlVO.setSignature(rs.getString("SIGNATURE"));
				urlVO.setMetadata(rs.getString("METADATA"));
				urlVO.setSegmentId(rs.getInt("SEGMENT_ID"));
				result.put(url, urlVO);
			}
		} catch (SQLException e) {
			LOG.info("Error while selecting rows in URL_DETAIL" + e.getLocalizedMessage(), e);
		} finally {
			if (stmt != null) {
				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection", e);
				}

			}
		}
		return result;

	}
	
	public int getCrawlId(int domainId){
		Connection conn = JDBCConnector.getConnection();
		//System.out.println("getting crawlid");
		int crawl_id = 0;
		if(conn != null){
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				
				String query = "SELECT CRAWL_ID FROM CRAWL_MASTER WHERE PROGRESS=1 and DOMAIN_ID="+domainId;
				stmt.execute(query);
				ResultSet rs = stmt.getResultSet();
				if(rs.next())
				crawl_id = rs.getInt("CRAWL_ID");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOG.error(e.getLocalizedMessage());
			}finally {
				if (stmt != null) {
					try {
						stmt.close();
						conn.close();
					} catch (SQLException e) {
						LOG.info("Error while closing connection" + e);
					}

				}

			}
		}
		return crawl_id;
		
	}
	
	/**
	 * returns Live crawl id
	 * @return
	 */
	public int getLiveCrawlId(int domainId){
		Connection conn = JDBCConnector.getConnection();
		//System.out.println("getting crawlid");
		int crawl_id = 0;
		if(conn != null){
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				
				String query = "SELECT CRAWL_ID FROM CRAWL_MASTER WHERE LIVE=1 and DOMAIN_ID="+domainId;
				stmt.execute(query);
				ResultSet rs = stmt.getResultSet();
				if(rs.next())
				crawl_id = rs.getInt("CRAWL_ID");
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				LOG.error(e.getLocalizedMessage());
			}finally {
				if (stmt != null) {
					try {
						stmt.close();
						conn.close();
					} catch (SQLException e) {
						LOG.info("Error while closing connection" + e);
					}

				}

			}
		}
		return crawl_id;
		
	}
}
