package org.apache.nutch.crawl.dao;

import org.apache.nutch.crawl.vo.SegmentVO;
import org.apache.nutch.crawl.vo.DomainVO;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.nutch.tools.JDBCConnector;

public class DomainDAO {

	public static final Logger LOG = LoggerFactory.getLogger(DomainDAO.class);

	public List<SegmentVO> readSegmentsFromDomain(int domainId) {
		Connection conn = JDBCConnector.getConnection();
		List<SegmentVO> result = null;
		if (conn == null) {
			Log.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return result;
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String query = " SELECT * FROM SEGMENT_MASTER MASTER WHERE DOMAIN_ID="
					+ domainId + " ORDER BY PRIORITY";
			boolean success = stmt.execute(query);
			if (success) {
				ResultSet rs = stmt.getResultSet();
				result = new ArrayList<SegmentVO>();
				while (rs.next()) {
					SegmentVO sg = new SegmentVO();
					sg.setSegmentId(rs.getInt("SEGMENT_ID"));
					sg.setSegmentName(rs.getString("SEGMENT_NAME"));
					sg.setUrl_pattern_rule(rs.getString("URL_PATTERN_RULE"));
					if (rs.getInt("CRAWL") == 0) {
						sg.setCrawl(false);
					} else {
						sg.setCrawl(true);
					}
					sg.setPriority(rs.getInt("PRIORITY"));
					sg.setDomainId(rs.getInt("DOMAIN_ID"));
					result.add(sg);
				}
				rs.close();
			}
		} catch (SQLException e) {
			Log.info("Error while fetching row in SEGMENT_MASTER" + e);
			e.printStackTrace();
		} finally {
			if (stmt != null) {

				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					Log.info("Error while closing connection" + e);
				}

			}
		}
		return result;

	}

	public List<DomainVO> read() {
		return read(0);
	}
	public List<DomainVO> read(int domainId) {
		Connection conn = JDBCConnector.getConnection();
		List<DomainVO> result = null;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return result;
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String query = null;
			if(domainId == 0) {
				query = " SELECT * FROM DOMAIN ORDER BY DOMAIN_NAME desc";
			}else{
				query = " SELECT * FROM DOMAIN WHERE DOMAIN_ID="+domainId;
			}
			
			boolean success = stmt.execute(query);
			if (success) {
				ResultSet rs = stmt.getResultSet();
				result = new ArrayList<DomainVO>();
				while (rs.next()) {
					DomainVO domainVO = new DomainVO();
					domainVO.setDomainId(rs.getInt("DOMAIN_ID"));
					domainVO.setDomainName(rs.getString("DOMAIN_NAME"));
					domainVO.setUrl(rs.getString("URL"));
					domainVO.setSeedUrl(rs.getString("SEED_URL"));
					domainVO.setFinal_content_directory(rs.getString("FINAL_CONTENT_DIRECTORY"));
					domainVO.setCrawlStatus(rs.getString("CRAWL_STATUS"));
					result.add(domainVO);
				}
				rs.close();
			}
		} catch (SQLException e) {
			LOG.info("Error while fetching row in DOMAIN" + e);
			e.printStackTrace();
		} finally {
			if (stmt != null) {

				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection" + e);
				}

			}
		}
		return result;

	}

	public DomainVO readByPrimaryKey(int domainId) {
		Connection conn = JDBCConnector.getConnection();
		DomainVO domainVO = null;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return domainVO;
		}
		PreparedStatement stmt = null;
		try {

			String query = " SELECT * FROM DOMAIN where DOMAIN_ID=?";
			stmt = conn.prepareStatement(query);
			stmt.setInt(1, domainId);
			boolean success = stmt.execute();
			if (success) {
				ResultSet rs = stmt.getResultSet();
				rs.next();
				domainVO = new DomainVO();
				domainVO.setDomainId(rs.getInt("DOMAIN_ID"));
				domainVO.setDomainName(rs.getString("DOMAIN_NAME"));
				domainVO.setUrl(rs.getString("URL"));			
				domainVO.setSeedUrl(rs.getString("SEED_URL"));
				domainVO.setFinal_content_directory(rs.getString("FINAL_CONTENT_DIRECTORY"));
				domainVO.setCrawlStatus(rs.getString("CRAWL_STATUS"));
			}
		} catch (SQLException e) {
			LOG.info("Error while fetching row in DOMAIN" + e);
			e.printStackTrace();
		} finally {
			if (stmt != null) {

				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection" + e);
				}

			}
		}
		return domainVO;

	}
	
	public boolean updateCrawlStatus(DomainVO domainVO) {
		Connection conn = JDBCConnector.getConnection();
		boolean success = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return success;
		}
		PreparedStatement stmt = null;
		try {
			conn.setAutoCommit(false);
			String query = "UPDATE DOMAIN SET CRAWL_STATUS = ? WHERE DOMAIN_ID=?";
			stmt = conn.prepareStatement(query);
			stmt.setString(1, domainVO.getCrawlStatus());
			stmt.setInt(2, domainVO.getDomainId());
			success = stmt.execute();
			conn.commit();
		} catch (SQLException e) {
			LOG.info("Error while updating row in DOMAIN" + e);
			e.printStackTrace();
			try{
				conn.rollback();
			}catch(SQLException s){
				
			}
		} finally {
			if (stmt != null) {

				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection" + e);
				}

			}
		}
		return success;

	}
	
	public long checkCrawlInProgress() {
		Connection conn = JDBCConnector.getConnection();
		long count = 1;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return count;
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String query = "SELECT COUNT(*) AS ROWCOUNT FROM DOMAIN WHERE CRAWL_STATUS='STARTED'";
			boolean success = stmt.execute(query);
			if(success){
				ResultSet rs = stmt.getResultSet();
				rs.next();
				count = rs.getLong("ROWCOUNT");
				
				System.out.println("COUNT" + count);
			}
		} catch (SQLException e) {
			LOG.info("Error while fetching row count in DOMAIN" + e);
			e.printStackTrace();
		} finally {
			if (stmt != null) {

				try {
					stmt.close();
					conn.close();
				} catch (SQLException e) {
					LOG.info("Error while closing connection" + e);
				}

			}
		}
		return count;

	}
}
