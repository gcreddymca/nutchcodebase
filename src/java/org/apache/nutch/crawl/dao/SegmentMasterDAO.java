package org.apache.nutch.crawl.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nutch.crawl.vo.DomainVO;
import org.apache.nutch.crawl.vo.SegmentVO;
import org.apache.nutch.tools.JDBCConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SegmentMasterDAO {

	public static final Logger LOG = LoggerFactory
			.getLogger(SegmentMasterDAO.class);

	public boolean create(SegmentVO segment) {
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return false;
		}
		PreparedStatement stmt = null;
		try {
			String query = "INSERT INTO SEGMENT_MASTER (SEGMENT_ID,SEGMENT_NAME,URL_PATTERN_RULE,CRAWL,PRIORITY) VALUES(SEGMENT_MASTER_SEQUENCE.nextval,?,?,?,?)";
			stmt = conn.prepareStatement(query);
			stmt.setString(1, segment.getSegmentName());
			stmt.setString(2, segment.getUrl_pattern_rule());
			if (segment.isCrawl() == true) {
				stmt.setInt(3, 1);
			} else {
				stmt.setInt(3, 0);
			}
			stmt.setInt(4, segment.getPriority());
			result = stmt.execute();
			conn.commit();
		} catch (SQLException e) {
			try{
				conn.rollback();
			}catch(SQLException s){
				LOG.info(
						"Error while rolling back"
								+ s.getLocalizedMessage(), e);
			}
			LOG.info(
					"Error while creating row in SEGMENT_MASTER"
							+ e.getLocalizedMessage(), e);
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

	public List<SegmentVO> read(SegmentVO segment) {
		Connection conn = JDBCConnector.getConnection();
		List<SegmentVO> result = null;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return result;
		}
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			String query = "SELECT * from SEGMENT_MASTER";
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
					result.add(sg);
				}
				rs.close();
			}
		} catch (SQLException e) {
			LOG.info(
					"Error while fetching row in SEGMENT_MASTER"
							+ e.getLocalizedMessage(), e);
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

	public boolean readByPrimaryKey(SegmentVO segment, String primaryKey) {
		Connection conn = JDBCConnector.getConnection();
		boolean result = false;
		if (conn == null) {
			LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
			return result;
		}
		PreparedStatement stmt = null;
		try {
			String query = "SELECT * from SEGMENT_MASTER where SEGMENT_ID = ?";
			stmt = conn.prepareStatement(query);
			stmt.setInt(0, segment.getSegmentId());

			result = stmt.execute();
			ResultSet resultSet = stmt.getResultSet();
			resultSet.first();
			segment.setSegmentId(resultSet.getInt("SEGMENT_ID"));
			segment.setSegmentName(resultSet.getString("SEGMENT_NAME"));
			segment.setUrl_pattern_rule(resultSet.getString("URL_PATTERN_RULE"));
			if (resultSet.getInt("CRAWL") == 0) {
				segment.setCrawl(false);
			} else {
				segment.setCrawl(true);
			}
			segment.setPriority(resultSet.getInt("PRIORITY"));
			resultSet.close();

		} catch (SQLException e) {
			LOG.info("Error while fetching row in SEGMENT_MASTER"+ e.getLocalizedMessage(), e);
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
	
	 /**	
	  * This method is used to get all segment url pattern rules for given domainId
	  * @param domainId
	  * @return
	  */
	 public List<String> readNotCrawlSegmentUrlRule(int domainId) {
	  Connection conn = JDBCConnector.getConnection();
	  List<String> result = null;
	  if (conn == null) {
	   LOG.info("Connection not found. Could not create row in SEGMENT_MASTER");
	   return result;
	  }
	  Statement stmt = null;
	  try {
	   stmt = conn.createStatement();
	   String query = "SELECT * from SEGMENT_MASTER where DOMAIN_ID="+domainId+" AND CRAWL="+0;
	   boolean success = stmt.execute(query);
	   if (success) {
	    ResultSet rs = stmt.getResultSet();
	    result = new ArrayList<String>();
	    while (rs.next()) {
	     System.out.println("SegmentId:"+rs.getInt("SEGMENT_ID")+" UrlPatternRule: "+rs.getString("URL_PATTERN_RULE"));
	     result.add(rs.getString("URL_PATTERN_RULE"));
	    }
	    rs.close();
	   }
	  } catch (SQLException e) {
	   LOG.info("Error while fetching row in SEGMENT_MASTER"+ e.getLocalizedMessage(), e);
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
	 
	 public DomainVO readByPrimaryKey(int primaryKey, Connection conn) throws Exception {
			boolean connCreated = false;
		 	if(conn == null) {
				JDBCConnector jdbcCon = new JDBCConnector();
				conn = jdbcCon.getConnection();
				connCreated = true;
			}
			DomainVO domain = new DomainVO();
			boolean result = false;
			if (conn == null) {
				LOG.info("Connection not found. Could not create row in DOMAIN");
				return domain;
			}
			PreparedStatement stmt = null;
			try {
				String query = "SELECT * from DOMAIN where DOMAIN_ID = ?";
				stmt = conn.prepareStatement(query);
				stmt.setInt(1, primaryKey);
				result = stmt.execute();
				ResultSet resultSet = stmt.getResultSet();
				resultSet.next();
				domain.setDomainId(resultSet.getInt("DOMAIN_ID"));
				domain.setDomainName(resultSet.getString("DOMAIN_NAME"));
				domain.setSeedUrl(resultSet.getString("SEED_URL"));
				domain.setUrl(resultSet.getString("URL"));
				domain.setRaw_content_directory(resultSet
						.getString("raw_content_directory"));
				domain.setFinal_content_directory(resultSet
						.getString("final_content_directory"));

			} catch (Exception e) {
				LOG.info("Error while fetching row in DOMAIN_MASTER" + e);
				throw new Exception("Could Not read Domain from Doamin_Master");
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
						if(connCreated) {
							conn.close();
						}
					} catch (SQLException e) {
						LOG.info("Error while closing connection" + e);
					}
				}
			}
			return domain;
		}
	 
	 
	 public Map<String, String> readUrlsHtmlLocForSegment(int segmentId, int crawlId) {
			Connection conn = JDBCConnector.getConnection();
			Map<String, String> urlLocMap = new HashMap<String, String>();
			if (conn == null) {
				LOG.info("Connection not found. Could not create row in SEGMENT_URLS_DETAIL");
				return urlLocMap;
			}
			if (segmentId == 0) {
				LOG.info("Segment Id is null");
				return urlLocMap;
			}
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				String query = " SELECT * from URL_HTML_LOC where default_location=1 and SEGMENT_ID="+segmentId+" and CRAWL_ID="+crawlId;
				boolean success = stmt.execute(query);
				if (success) {
					ResultSet rs = stmt.getResultSet();
					while (rs.next()) {
						if(!rs.getString("URL").contains(" "))
						urlLocMap.put(rs.getString("URL_LOC"), rs.getString("URL"));
					}
				}
			} catch (SQLException e) {
				LOG.info("Error while fetching row in SEGMENT_URLS_DETAIL" + e);
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
			return urlLocMap;
		}

		public Map<String, String> readUrlHtmlLocforAllSegment(int crawlId, Connection conn) {
			boolean connCreated = false;
			if(conn == null) {
				conn = JDBCConnector.getConnection();
				connCreated = true;
			}
			Map<String, String> urlHtmlLoc = new HashMap<String, String>();
			if (conn == null) {
				LOG.info("Connection not found. Could not create row in SEGMENT_URLS_DETAIL");
				return urlHtmlLoc;
			}
			Statement stmt = null;
			try {
				stmt = conn.createStatement();
				String query = "SELECT * from URL_HTML_LOC where default_location=1 and CRAWL_ID="+crawlId;

				boolean success = stmt.execute(query);
				if (success) {
					ResultSet rs = stmt.getResultSet();
					while (rs.next()) {
						urlHtmlLoc.put(rs.getString("URL_LOC"), rs.getString("URL"));
					}
					rs.close();
					// result.add(segment);
				}
			} catch (SQLException e) {
				LOG.info("Error while fetching row in SEGMENT_URLS_DETAIL" + e);
			} finally {
				if (stmt != null) {
					try {
						stmt.close();
						if(connCreated) {
							conn.close();
						}
					} catch (SQLException e) {
						LOG.info("Error while closing connection" + e);
					}
				}
			}
			return urlHtmlLoc;
		}
}
