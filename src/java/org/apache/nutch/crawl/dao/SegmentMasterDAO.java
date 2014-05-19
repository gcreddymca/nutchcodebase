package org.apache.nutch.crawl.dao;

import org.apache.nutch.tools.JDBCConnector;

import org.apache.nutch.crawl.vo.SegmentVO;
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
			String query = "INSERT INTO SEGMENT_MASTER (SEGMENT_ID,SEGMENT_NAME,RULE,CRAWL,PRIORITY) VALUES(SEGMENT_MASTER_SEQUENCE.nextval,?,?,?,?)";
			stmt = conn.prepareStatement(query);
			stmt.setString(1, segment.getSegmentName());
			stmt.setString(2, segment.getRule());
			if (segment.isCrawl() == true) {
				stmt.setInt(3, 1);
			} else {
				stmt.setInt(3, 0);
			}
			stmt.setInt(4, segment.getPriority());
			result = stmt.execute();
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
					sg.setRule(rs.getString("RULE"));
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
			segment.setRule(resultSet.getString("RULE"));
			if (resultSet.getInt("CRAWL") == 0) {
				segment.setCrawl(false);
			} else {
				segment.setCrawl(true);
			}
			segment.setPriority(resultSet.getInt("PRIORITY"));
			resultSet.close();

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

}
