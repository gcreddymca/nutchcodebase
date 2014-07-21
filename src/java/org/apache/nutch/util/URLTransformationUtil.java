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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.tools.JDBCConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class URLTransformationUtil {
	private static final Configuration conf;
	private static Pattern[] requestParamsExclusionPatterns =null;
	private static final Pattern RESOURCE_EXTNS = Pattern
			.compile(".*\\.(gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|rpm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)$");
	public static final Logger LOG = LoggerFactory
			.getLogger(URLTransformationUtil.class);
	private static final Pattern HREF_ATTRIBUTE = Pattern.compile(
			"href=\"(.*?)\"", Pattern.DOTALL);

	static {
		conf = NutchConfiguration.create();
		String[] exclusionList = conf
				.getStrings("url.request.parameter.exclusion.list");
		if(exclusionList != null){
		requestParamsExclusionPatterns = new Pattern[exclusionList.length];
		StringBuilder pattern = null;
		for (int i = 0; i < exclusionList.length; i++) {
			pattern = new StringBuilder();
			pattern.append("[;&]*");
			pattern.append(exclusionList[i]);
			pattern.append("[\\w%/.]*[=\\w-/.+%]*[$&]*");
			requestParamsExclusionPatterns[i] = Pattern.compile(pattern
					.toString());
		}
		
		}
	}

	public static String excludeRequestParameters(String url) {
		String urlPath = url;
		if (url.contains(NutchConstants.HTML_EXTN))
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

	public static String excludeBookmarks(String url) {
		Matcher matcher = NutchConstants.URL_BOOKMARKS.matcher(url);
		if (matcher.find()) {
			url = url.replace(matcher.group(), NutchConstants.EMPTY_STRING);
		}
		return url;
	}

	public static String getURLPath(String url) {
		String path = null;
		Matcher matcher = NutchConstants.URL_PATH_PATTERN.matcher(url);
		if (matcher.find()) {
			path = matcher.group();
		}
		return path;
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
	
	public  String getSingleUrlHTMLLOC(String url, int crawlId, Connection conn) {
		boolean connCreated = false;
		String urlLoc = null;
		if(url.equals("/")){
			 urlLoc = "/index.html";
		}else {
			 if(conn == null) {
				 conn = JDBCConnector.getConnection();
				 connCreated = true;
			 }
			 if (conn != null) {
				 Statement stmt = null;
				 try{
					 stmt = conn.createStatement();
					 String query = "SELECT URL_LOC FROM URL_HTML_LOC WHERE url= '"+url+"' and CRAWL_ID="+crawlId;
					 stmt.execute(query);
					 ResultSet rs = stmt.getResultSet();
					 if(rs.next()){
						 urlLoc = rs.getString("URL_LOC");
					 }
				 }
				 catch(SQLException e){
					 LOG.error(e.getMessage());
				 }finally {
					 if (stmt != null) {
						 try {
							 stmt.close();
							 if(connCreated) {
								 conn.close();
							 }
					 	 } catch (SQLException e) {
					 		 LOG.info("Error while closing connection" + e.getMessage());
						 }
					 }
				 }
		     }
		}
		return urlLoc;
	}
	
	public  List<String> getURLHTMLLOC(String url, int crawlId, Connection conn) {
		List<String> urlSegments = new ArrayList<String>();
		if(url.equals("/")){
			urlSegments.add("/index.html");
		 }else{
		if (conn != null) {
		  Statement stmt = null;
		 try{
			 
		 stmt = conn.createStatement();
		 String query = "SELECT URL_LOC, SEGMENT_ID FROM URL_HTML_LOC WHERE url= '"+url+"' and CRAWL_ID="+crawlId;
		 stmt.execute(query);
		 ResultSet rs = stmt.getResultSet();
		 if(rs.next()){
			 urlSegments.add(rs.getString("URL_LOC"));
			 urlSegments.add(String.valueOf(rs.getInt("SEGMENT_ID")));
		 }
		}
		 catch(SQLException e){
			 LOG.error("Error in getUrlHtmlLoc method:"+e.getMessage());
		}
		 finally {
			if (stmt != null) {
			try {
				stmt.close();
				} catch (SQLException e) {
				LOG.error(e.getMessage());
				}
			  }
			}
		   }
		 }
		return urlSegments;
	}
	
	public String getUrltype(String url,int crawlId){
		String urlType = "";
		if(!url.equals("/")){
			Connection conn = JDBCConnector.getConnection();
			Statement stmt = null;
			if (conn != null) {
				try {
					stmt = conn.createStatement();
					String query = "select sm.url_type from URL_DETAIL ud, SEGMENT_MASTER sm where ud.segment_id = sm.segment_id and ud.url= '"+url+"' and CRAWL_ID = "+crawlId;
					stmt.execute(query);
					ResultSet rs = stmt.getResultSet();
					if(rs.next()){
					urlType = rs.getString("URL_TYPE");	
					}
				} catch (SQLException e) {
					LOG.error(e.getMessage());
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
		}
		return urlType;
	}
	
	/*public String reMapLinks(String domainUrl, String htmlContent,
			Map<String, String> urlHtmlLoc, String urlType) {

		String tempValue = null;
		String hrefValue = null;
		String domain;
		if (domainUrl.contains("http://")) {
			domain = domainUrl.substring(domainUrl.indexOf("//") + 2,
					domainUrl.length());
		} else {
			domain = domainUrl;
		}
		String hrefAttribute = null;
		Matcher hrefMatcher = HREF_ATTRIBUTE.matcher(htmlContent);
		// iterate through each href found in the htmlContent
		while (hrefMatcher.find()) {

			// Get href attribute from htmlContent
			hrefAttribute = hrefMatcher.group();
			hrefAttribute = hrefAttribute.replaceAll("\"", "");

			// Get href attribute value
			hrefValue = hrefAttribute.substring(hrefAttribute.indexOf("=") + 1,
					hrefAttribute.length());

			tempValue = hrefValue;

			Matcher extnMatcher = RESOURCE_EXTNS.matcher(hrefValue);

			// Replace href Value in the htmlContent if it doesn't contain any
			// file extensions
			if (!(extnMatcher.find())) {

				if (hrefValue.contains(domainUrl)) {
					hrefValue = hrefValue.replace(domainUrl, "");
				}
				// remove request parameters from element as specified in
				// the exclusion list
				hrefValue = excludeRequestParameters(hrefValue);
				hrefValue = hrefValue.replaceAll("\r\n", "");
				hrefValue = hrefValue.replaceAll("\0", "");
				// remove bookmarks
				//hrefValue = excludeBookmarks(hrefValue);

				// iterate through Map in urlHtmlLoc segment
				for (Map.Entry<String, String> entry : urlHtmlLoc.entrySet()) {
					// if hrefVal matches with url in the map then replace
					// htmlContent
					// with the corresponding HtmlLocation
					if (hrefValue.length() > 0 && hrefValue.equals(entry.getValue())) {

						if (urlType.equals("AbsoluteWithHttp")) {

							hrefValue = "http://".concat(domain).concat(
									entry.getKey());

						} else if (urlType.equals("AbsoluteWithoutHttp")) {
							hrefValue = domain.concat(entry.getKey());
						} else if (urlType.equals("AbsoluteWithSlash")) {
							hrefValue = "//".concat(domain).concat(
									entry.getKey());
						} else if (urlType.equals("RelativeWithoutSlash")) {
							hrefValue = entry.getKey().substring(1,
									entry.getKey().length());
						} else {
							hrefValue = entry.getKey();
						}
					    hrefValue = hrefValue.split("\\/index.html")[0];
						htmlContent = htmlContent.replaceAll("[\"]"+tempValue+"[\"]", "\""+hrefValue+"\"");
					}

				}
			}

		}
		return htmlContent;
	}*/

	
	
	/**
	 * Replace href Value from htmlContent if it contains domain url
	 * 
	 * @param domainUrl
	 * @param rawHtml
	 */
	
	public String excludeDomainURL(String domainUrl, String rawHtml) {
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
			Matcher extnMatcher = RESOURCE_EXTNS.matcher(hrefValue);
			// Replace href Value in the htmlContent if it doesn't contain any file extensions
			if (!(extnMatcher.find())) {
				if (hrefValue.contains(domainUrl)) {
					hrefValue = hrefValue.replace(domainUrl, "");
					rawHtml = rawHtml.replaceAll("[\"]"+tempValue+"[\"]", "\""+hrefValue+"\"");
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
		} catch (IOException e) {
			LOG.error(e.getLocalizedMessage());
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
	
	public String handleTransformations(String rawHtml, String method, String domainUrl, String url, int crawlId, Connection conn, Pattern[] requestParamsExclusionPatterns) {
		if (method.equals("excludeRequestParameters")) {
			rawHtml = excludeRequestParameters(rawHtml, requestParamsExclusionPatterns);
		} 
		else if(method.equals("removeSpecialChars")) {
			rawHtml = rawHtml.replaceAll("\n", "");
			rawHtml = rawHtml.replaceAll("\r\n", "");
			rawHtml = rawHtml.replaceAll("\0", "");
		} 
		else if(method.equals("addHTMLizedComment")) {
			String htmlizedStamp = addTimeStamptoURL(url,crawlId,conn);
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
	
	/**
	 * this method adds timestamp in URL_HTML_LOC table to each url
	 * @param urlhtmlloc
	 * @param crawlId
	 */
	public String addTimeStamptoURL(String url,int crawlId, Connection conn){
		String date = new Date().toString();
		String htmlizedStamp = "\n<!--HTML generated by Hyperscale on "+ date + " -->";
		 //boolean connCreated = false;
			/*if(conn == null) {
				conn = JDBCConnector.getConnection();
				connCreated= true;
			}*/
		if(conn != null){
			Statement stmt = null;
			try {
				stmt= conn.createStatement();
				String query = "UPDATE URL_HTML_LOC SET LAST_FETCH_TIME= '"+ date+"' WHERE URL= '"+url+"' and  CRAWL_ID= "+crawlId;
				stmt.execute(query);
				conn.commit();
			} catch (SQLException e) {
				LOG.error("Error while executing update LAST_FETCH_TIME in URL_HTML_LOC table :" + e.getMessage());
			}finally {
				if (stmt != null) {
					try {
						stmt.close();
						/*if(connCreated){
							   conn.close();
						}*/
					} catch (SQLException e) {
						LOG.info("Error while closing Statement: " + e.getMessage());
					}
				}
			}
		} else{
			LOG.error("Connection null in addTimeStamptoURL method:");
		}
		return htmlizedStamp;
	}
	
	public  static String excludeRequestParameters(String url, Pattern[] requestParamsExclusionPatterns) {
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
	
}
