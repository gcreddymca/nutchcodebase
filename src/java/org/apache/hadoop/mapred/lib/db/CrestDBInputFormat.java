package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBWritable;

public class CrestDBInputFormat extends DBInputFormat{

	public InputSplit[] getSplits(JobConf job, int chunks) throws IOException {
		InputSplit[] d = super.getSplits(job, chunks);
		return d;
		
	}
	
	public RecordReader getRecordReader(InputSplit split,
			JobConf job, Reporter reporter) throws IOException {
		Class inputClass = job.getClass("mapred.jdbc.input.class",
				DBInputFormat.NullDBWritable.class);
		try {
			return new CrestDBRecordReader((DBInputSplit) split, inputClass, job);
		} catch (SQLException ex) {
			throw new IOException(ex.getMessage());
		}
	}
	
	
	protected class CrestDBRecordReader extends DBRecordReader{

		protected CrestDBRecordReader(DBInputSplit split, Class inputClass,
				JobConf job) throws SQLException {
			super(split, inputClass, job);
		}
		
		protected String getSelectQuery() {
			StringBuilder query = new StringBuilder();
			query.append("SELECT SEED_URL FROM DOMAIN");
			return query.toString();
		}
	}
}
