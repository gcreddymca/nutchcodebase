package org.apache.nutch.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.tools.JDBCConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransformationRunner implements Runnable{
	
	public static final Logger LOG = LoggerFactory.getLogger(TransformationRunner.class);
	
	private String url;
	private String rawHtml;
	private String finalpath;
	private int crawlId;
	private int domainId;
	private Map<String, String> urlLocMapToReplace;
	private Map<String, List<String>> transformationMap;
	private String domainUrl;
	
	private static final Configuration conf;
	private static Pattern[] requestParamsExclusionPatterns =null;
	
    public TransformationRunner(String url,String rawHtml,int crawlId,int domainId,String finalpath, Map<String, 
    		String> urlLocMapToReplace, Map<String, List<String>> transformationMap, String domainUrl){
    	this.url=url;
    	this.rawHtml=rawHtml;
    	this.crawlId=crawlId;
    	this.domainId=domainId;
    	this.finalpath=finalpath;
    	this.urlLocMapToReplace = urlLocMapToReplace;
    	this.transformationMap = transformationMap;
    	this.domainUrl = domainUrl;
    }
	
	@Override
	public void run() {
		try {
			urlTransformation(url, rawHtml, crawlId, domainId, finalpath, urlLocMapToReplace, transformationMap, domainUrl);
		} catch (Exception e) {
			LOG.error(e.getMessage());
		}
	}
	
	static {
		conf = NutchConfiguration.create();
		String[] exclusionList = conf.getStrings("url.request.parameter.exclusion.list");
		if(exclusionList != null){
		requestParamsExclusionPatterns = new Pattern[exclusionList.length];
		StringBuilder pattern = null;
		for (int i = 0; i < exclusionList.length; i++) {
			pattern = new StringBuilder();
			pattern.append("[;&]*");
			pattern.append(exclusionList[i]);
			pattern.append("[\\w%/.]*[=\\w-/.+%]*[$&]*");
			requestParamsExclusionPatterns[i] = Pattern.compile(pattern.toString());
			}
		}
	}
	
	public void urlTransformation(String url,String rawHtml,int crawlId,int domainId,String finalpath, 
		Map<String, String> urlLocMapToReplace, Map<String, List<String>> transformationMap, String domainUrl) throws Exception{
		Connection conn = JDBCConnector.getConnection();
		URLTransformationUtil tUtil = new URLTransformationUtil();
		if(conn==null){
			LOG.error("Unable to create Connection in urlTransformation method:");
		}else{
			try {
				List<String> urlSegments = tUtil.getURLHTMLLOC(url,crawlId,conn);
				if(!urlSegments.isEmpty()){
					if(!transformationMap.isEmpty()) {
						List<String> transformations = transformationMap.get(urlSegments.get(1));
						if(transformations !=null && transformations.size()>0){
							Iterator<String> iterator = transformations.iterator();
							// Iterate through available transformations for segment
							while (iterator.hasNext()) {
								String method = iterator.next();
								//handle all transformations
								rawHtml = tUtil.handleTransformations(rawHtml, method, domainUrl, url, crawlId, conn, requestParamsExclusionPatterns);
							}
						}	
					}
					tUtil.writeContentToFile(finalpath.concat(urlSegments.get(0)), rawHtml);
					urlSegments = null;
				}
			} catch (Exception e) {
				LOG.error(e.getLocalizedMessage());
				e.printStackTrace();
			}finally {
				try {
					if(conn!=null){
						conn.close();
					}
				} catch (SQLException e) {
					LOG.info("Error while closing connection in TransformationRunner: urlTransformation method: " + e.getMessage());
				}
			}
		}
	}
	
}
