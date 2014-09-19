package org.apache.nutch.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.tools.JDBCConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TransformationRunner implements Runnable{
	
	public static final Logger LOG = LoggerFactory.getLogger(TransformationRunner.class);
	
	private static final Configuration conf;
	public static Pattern[] requestParamsExclusionPatterns =null;
	private static final Pattern RESOURCE_EXTNS = Pattern
			.compile(".*\\.(gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|rpm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)$");
	private static final Pattern HREF_ATTRIBUTE = Pattern.compile(
			"href=\"(.*?)\"", Pattern.DOTALL);
	private static final Pattern ACTION_ATTRIBUTE = Pattern.compile(
			"action=\"(.*?)\"", Pattern.DOTALL);
	
	private String url;
	private String rawHtml;
	private String finalpath;
	private int crawlId;
	private int domainId;
	private Map<String, String> urlLocMapToReplace;
	private Map<String, List<String>> transformationMap;
	private String domainUrl;
	
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
	
	
	
	public void urlTransformation(String url,String rawHtml,int crawlId,int domainId,String finalpath, 
		Map<String, String> urlLocMapToReplace, Map<String, List<String>> transformationMap, String domainUrl) throws Exception{
		//Connection conn = JDBCConnector.getConnection();
		/*//URLTransformationUtil tUtil = new URLTransformationUtil();
		if(conn==null){
			LOG.error("Unable to create Connection in urlTransformation method:");
		}else{*/
			try {
				List<String> urlSegments = getURLHTMLLOC(url,crawlId);
				if(!urlSegments.isEmpty()){
					if(!transformationMap.isEmpty()) {
						List<String> transformations = transformationMap.get(urlSegments.get(1));
						if(transformations !=null && transformations.size()>0){
							Iterator<String> iterator = transformations.iterator();
							// Iterate through available transformations for segment
							while (iterator.hasNext()) {
								String method = iterator.next();
								//handle all transformations
								rawHtml = handleTransformations(rawHtml, method, domainUrl, url, crawlId, requestParamsExclusionPatterns);
							}
						}	
					}
					writeContentToFile(finalpath.concat(urlSegments.get(0)), rawHtml);
					urlSegments = null;
				}
			} catch (Exception e) {
				LOG.error("Error while transforming the url..."+url + "\n"+ e.getMessage());
				e.printStackTrace();
			}/*finally {
				try {
					if(conn!=null){
						conn.close();
					}
				} catch (SQLException e) {
					LOG.info("Error while closing connection in TransformationRunner: urlTransformation method: " + e.getMessage());
				}
			}*/
		}
	//}
	
	public  List<String> getURLHTMLLOC(String url, int crawlId) {
		List<String> urlSegments = new ArrayList<String>();
		if(url.equals("/")){
			urlSegments.add("/index.html");
		}else{
			Connection conn = JDBCConnector.getConnection();
			if (conn != null) {
				Statement stmt = null;
				ResultSet rs = null;
				try{
					 stmt = conn.createStatement();
					 String query = "SELECT URL_LOC, SEGMENT_ID FROM URL_HTML_LOC WHERE url= '"+url+"' and CRAWL_ID="+crawlId;
					 stmt.execute(query);
					 rs = stmt.getResultSet();
					 if(rs.next()){
						 urlSegments.add(rs.getString("URL_LOC"));
						 urlSegments.add(String.valueOf(rs.getInt("SEGMENT_ID")));
					 }
				} catch(SQLException e){
					 LOG.error("Error in getUrlHtmlLoc method:"+e.getMessage());
				}
				finally {
					try {
						if (stmt != null) {
							stmt.close();
						}
						if(rs != null) {
							rs.close();
						}
						if(conn != null) {
							conn.close();
						}
					} catch (SQLException e) {
						LOG.error(e.getMessage());
					}
				}
			}
		}
		
		return urlSegments;
	}
	
	public String handleTransformations(String rawHtml, String method, String domainUrl, String url, int crawlId, Pattern[] requestParamsExclusionPatterns) {
		if (method.equals("excludeRequestParameters")) {
			rawHtml = excludeRequestParameters(rawHtml, requestParamsExclusionPatterns);
		} 
		else if(method.equals("removeSpecialChars")) {
			   rawHtml = rawHtml.replaceAll("\r\n", "");
			//rawHtml = rawHtml.replaceAll("\0", "");
		} 
		else if(method.equals("addHTMLizedComment")) {
			String htmlizedStamp = addTimeStamptoURL(url,crawlId);
			rawHtml = rawHtml + htmlizedStamp;
		}
		else if (method.equals("excludeDomainURL")) {
			rawHtml = excludeDomainURL(domainUrl, rawHtml);
		}
		else if (method.equals("reMapLinks")) {
			//htmlContent = reMapLinks(domain, htmlContent, urlHtmlLoc, urlType);
		} 
		else if (method.equals("removeWhiteSpace")) {
			//htmlContent = removeWhiteSpace(htmlContent);
		}
		return rawHtml;
	}
	
	private void executeAddTimeStampQuery(String query, int count) throws SQLException {
		if(count < 2) {
			Connection connection = null;
			Statement stmt = null;
			try {
				connection = JDBCConnector.getConnection();	
				stmt = connection.createStatement();
				stmt.execute(query);
				connection.commit();	
			}finally {
				if(stmt != null) {
					stmt.close();
				}
				if(connection != null) {
					connection.close();
				}
				
			}
		}
	}
	
	/**
	 * this method adds timestamp in URL_HTML_LOC table to each url
	 * @param urlhtmlloc
	 * @param crawlId
	 */
	public String addTimeStamptoURL(String url,int crawlId){
		String date = new Date().toString();
		String htmlizedStamp = "\n<!--HTML generated by Hyperscale on "+ date + " -->";
		//Connection connection = JDBCConnector.getConnection();	
		int count = 0;
		String query = "UPDATE URL_HTML_LOC SET LAST_FETCH_TIME= '"+ date+"', HTML_FILE_STATUS='0' WHERE URL= '"+url+"' and  CRAWL_ID= "+crawlId;
		//if(connection != null){
			//Statement stmt = null;
			try {
				/*if(connection.isClosed()) {
					connection = JDBCConnector.getConnection();
				}
				stmt = connection.createStatement();
				stmt.execute(query);
				connection.commit();*/
				executeAddTimeStampQuery(query, count);
			} catch (SQLException e) {
				try {
					count++;
					executeAddTimeStampQuery(query, count);
				} catch (SQLException e1) {
					LOG.error("Error while executing update LAST_FETCH_TIME in URL_HTML_LOC table :" + e1.getMessage());
				}
				
			}
		/*} else{
			LOG.error("Connection null in addTimeStamptoURL method:");
		}*/
		return htmlizedStamp;
	}
	
	public  String excludeRequestParameters(String rawHtml, Pattern[] requestParamsExclusionPatterns) {
		String tempValue = null;
		String hrefValue = null;
		String hrefAttribute = null;
		Matcher hrefMatcher = HREF_ATTRIBUTE.matcher(rawHtml);
		// iterate through each href found in the htmlContent
		while (hrefMatcher.find()) {
			// Get href attribute from htmlContent
			hrefAttribute = hrefMatcher.group();
			hrefAttribute = hrefAttribute.replaceAll("\"", "");
			// Get href attribute value
			hrefValue = hrefAttribute.substring(hrefAttribute.indexOf("=") + 1,	hrefAttribute.length());
			tempValue = hrefValue;
			hrefValue = excludeParameter(hrefValue);
			// Replace href Value in the htmlContent if it doesn't contain any file extensions
			if (!tempValue.equals(hrefValue)) {
				rawHtml = rawHtml.replaceAll("[\"]"+tempValue+"[\"]", "\""+hrefValue+"\"");
			}
		}
		return rawHtml;
	}
	
	public String excludeParameter(String url){
		String urlPath = url;
		if (url.endsWith(NutchConstants.HTML_EXTN))
			return urlPath;
		if(requestParamsExclusionPatterns == null){
			return urlPath;
		}
		String group = getURLRequestParameters(url);
		if (group != null) {
			urlPath = url.replace(group, NutchConstants.EMPTY_STRING);
			for (Pattern pattern : requestParamsExclusionPatterns) {
				Matcher matcher = pattern.matcher(group);
				while (matcher.find()) {
					group = group.replace(matcher.group(),
							NutchConstants.EMPTY_STRING);
				}
			}
			if (group.length() > 0
					&& !group.equals(NutchConstants.QUESTION_MARK)) {
				StringBuilder urlBuilder = new StringBuilder();
				urlBuilder.append(urlPath);
				urlBuilder.append(group);
				urlPath = urlBuilder.toString();
			}
		}
		return urlPath;
	}
	
	public static String getURLRequestParameters(String url) {
		String reqparams = null;
		Matcher matcher = NutchConstants.URL_REQUEST_PARAMS_PATTERN
				.matcher(url);
		if (matcher.find()) {
			reqparams = matcher.group();
		}
		return reqparams;
	}

	public String excludeDomainURL(String domainUrl, String rawHtml) {
		String tempValue = null;
		String hrefValue = null;
		String hrefAttribute = null;
		String actionValue = null;
		String actionAttribute = null;
		Matcher hrefMatcher = HREF_ATTRIBUTE.matcher(rawHtml);
		// iterate through each href found in the rawHtmlContent
		while (hrefMatcher.find()) {
			// Get href attribute from htmlContent
			hrefAttribute = hrefMatcher.group();
			hrefAttribute = hrefAttribute.replaceAll("\"", "");
			// Get href attribute value
			hrefValue = hrefAttribute.substring(hrefAttribute.indexOf("=") + 1,	hrefAttribute.length());
			tempValue = hrefValue;
			Matcher extnMatcher = RESOURCE_EXTNS.matcher(hrefValue);
			// Replace href Value in the htmlContent if it doesn't contain any file extensions
			if (!(extnMatcher.find())) {
				if (hrefValue.contains(domainUrl)) {
					hrefValue = hrefValue.replace(domainUrl, "");
					rawHtml = rawHtml.replaceAll("[\"]"+tempValue+"[\"]", "\""+hrefValue+"\"");
				}
			}
		}
		Matcher actionMatcher = ACTION_ATTRIBUTE.matcher(rawHtml);
		// iterate through each action found in the rawHtmlContent
		while (actionMatcher.find()) {
			// Get action attribute from htmlContent
			actionAttribute = actionMatcher.group();
			actionAttribute = actionAttribute.replaceAll("\"", "");
			// Get action attribute value
			actionValue = actionAttribute.substring(actionAttribute.indexOf("=") + 1,	actionAttribute.length());
			tempValue = actionValue;
			Matcher extnMatcher = RESOURCE_EXTNS.matcher(actionValue);
			// Replace action Value in the htmlContent if it doesn't contain any file extensions
			if (!(extnMatcher.find())) {
				if (actionValue.contains(domainUrl)) {
					actionValue = actionValue.replace(domainUrl, "");
					rawHtml = rawHtml.replaceAll("[\"]"+tempValue+"[\"]", "\""+actionValue+"\"");
				}
			}
		}
		return rawHtml;
	}
	
	/**
	 * This method writes the content at the specified path
	 * 
	 * @param path
	 * @param content
	 * @throws IOException
	 */
	public void writeContentToFile(String path, String content)	 {
		FileWriter writer;
		try {
			File file = createFile(path);
			writer = new FileWriter(file, false);
			writer.write(content);
			writer.close();
			file = null;
		} catch (IOException e) {
			LOG.error("Error while writing content in writeContentToFile() method:   "+path+"   " +e.getLocalizedMessage());
		}
	}
	
	/**
	 * This method creates file at the specified path,it also creates directory
	 * if it doesnt exist
	 * 
	 * @param path
	 * @return htmlFile
	 * @throws IOException
	 */
	private File createFile(String path) throws IOException {
		// Get folder hierarchy from HTML Location
		String folderHierarchyStr = getURLPath(path);
		// Create folder hierarchy if does not exist
		File folderHierarchy = new File(folderHierarchyStr);
		if (folderHierarchyStr != null && !folderHierarchy.exists()) {
			folderHierarchy.mkdirs();
		}
		// Create file if does not exist
		File htmlFile = new File(path);
		if (!htmlFile.exists()) {
			htmlFile.createNewFile();
		}
		return htmlFile;
	}
	
	public static String getURLPath(String url) {
		String path = null;
		Matcher matcher = NutchConstants.URL_PATH_PATTERN.matcher(url);
		if (matcher.find()) {
			path = matcher.group();
		}
		return path;
	}

	
	
}
