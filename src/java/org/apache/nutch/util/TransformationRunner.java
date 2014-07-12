package org.apache.nutch.util;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.dao.SegmentMasterDAO;
import org.apache.nutch.crawl.vo.DomainVO;
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
	
	private static final Configuration conf;
	private static Pattern[] requestParamsExclusionPatterns =null;
	private static final Pattern RESOURCE_EXTNS = Pattern
			.compile(".*\\.(jsp|gif|GIF|jpg|JPG|png|PNG|ico|ICO|css|CSS|sit|SIT|eps|EPS|wmf|WMF|zip|ZIP|ppt|PPT|mpg|MPG|xls|XLS|gz|GZ|rpm|RPM|tgz|TGZ|mov|MOV|exe|EXE|jpeg|JPEG|bmp|BMP|js|JS)$");
	
	private static final Pattern HREF_ATTRIBUTE = Pattern.compile("href=\"(.*?)\"", Pattern.DOTALL);
	
    public TransformationRunner(String url,String rawHtml,int crawlId,int domainId,String finalpath){
    	this.url=url;
    	this.rawHtml=rawHtml;
    	this.crawlId=crawlId;
    	this.domainId=domainId;
    	this.finalpath=finalpath;
    }
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			LOG.info("Transformation Thread Process Started:");
			urlTransformation(url, rawHtml, crawlId, domainId, finalpath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	
	public void urlTransformation(String url,String rawHtml,int crawlId,int domainId,String finalpath) throws Exception{
		SegmentMasterDAO smDAO = new SegmentMasterDAO();
		Map<String, String> urlLocMapToReplace = new HashMap<String, String>();
		urlLocMapToReplace = smDAO.readUrlHtmlLocforAllSegment(crawlId);
		DomainVO domainVO = smDAO.readByPrimaryKey(domainId);
		String urlhtmlloc = getURLHTMLLOC(url,crawlId);
		/*File file = new File(rawHtml);
		if(file.exists()){
		BufferedReader reader = new BufferedReader(new FileReader(rawHtml));
		while ((htmlContentAsString = reader.readLine()) != null) {
			htmlContent.append(htmlContentAsString);
		}*/
		try {
			if(null != urlhtmlloc){
			String finalHtmlContent = reMapLinks(domainVO.getUrl(), rawHtml, urlLocMapToReplace, getUrltype(url,crawlId));
			String htmlizedStamp = addTimeStamptoURL(url,crawlId);
			finalHtmlContent = finalHtmlContent + htmlizedStamp;
			writeContentToFile(finalpath.concat(urlhtmlloc), finalHtmlContent);
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error(e.getLocalizedMessage());
		}
	}
	
	public  String getURLHTMLLOC(String url, int crawlId) {
		// TODO Auto-generated method stub
		String urlLoc = null;
		if(url.equals("/")){
			 urlLoc = "/index.html";
		 }else{
		Connection conn = JDBCConnector.getConnection();
		 
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
			}
		 finally {
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
		return urlLoc;
	}
	
	public void writeContentToFile(String path, String content)
	 {
		// Create file in the location specified by path
		
		// Write content to file
		FileWriter writer;
		try {
			File file = createFile(path);
			writer = new FileWriter(file, false);
			writer.write(content);
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			LOG.error(e.getLocalizedMessage());
		}
	}
	
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
					// TODO Auto-generated catch block
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
	
	public String reMapLinks(String domainUrl, String htmlContent,
			Map<String, String> urlHtmlLoc, String urlType) {

		String tempValue = null;
		String hrefValue = null;
		String domain;
		/*if (domainUrl.contains("http://")) {
			domain = domainUrl.substring(domainUrl.indexOf("//") + 2,
					domainUrl.length());
		} else {
			domain = domainUrl;
		}*/
		if (domainUrl.contains("http://")) {
			domain = domainUrl.substring(domainUrl.indexOf("//") + 2,domainUrl.length());
			if(domain.contains("/")){
				domain = domain.substring(0,domain.indexOf("/"));
			}
			
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
			hrefValue = hrefAttribute.substring(hrefAttribute.indexOf("=") + 1, hrefAttribute.length());

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
				//remove new lines
				hrefValue = hrefValue.replaceAll("\n", "");
				hrefValue = hrefValue.replaceAll("\r\n", "");
				hrefValue = hrefValue.replaceAll("\0", "");
				// remove bookmarks
				hrefValue = excludeBookmarks(hrefValue);

				//Checking href contains extension jsp or not
				if(!hrefValue.contains(".jsp")){
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
					
				} else{
					//No remap link for jsp url
					htmlContent = htmlContent.replaceAll("[\"]"+tempValue+"[\"]", "\""+hrefValue+"\"");
				}
			}
		}
		return htmlContent;
	}
	
	public  static String excludeRequestParameters(String url) {
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
		LOG.info(">>>>>"+urlPath);

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
	
	public static String excludeBookmarks(String url) {
		Matcher matcher = NutchConstants.URL_BOOKMARKS.matcher(url);
		if (matcher.find()) {
			url = url.replace(matcher.group(), NutchConstants.EMPTY_STRING);
		}
		return url;
	}
	
	/**
	 * this method adds timestamp in URL_HTML_LOC table to each url
	 * @param urlhtmlloc
	 * @param crawlId
	 */
	public String addTimeStamptoURL(String url,int crawlId){
		String date = new Date().toString();
		
		String htmlizedStamp = "\n<!--HTML generated by Hyperscale on "+ date + " -->";
		Connection conn = JDBCConnector.getConnection();
		if(conn != null){
			Statement stmt = null;
			try {
				stmt= conn.createStatement();
				String query = "UPDATE URL_HTML_LOC SET LAST_FETCH_TIME= '"+ date+"' WHERE URL= '"+url+"' and  CRAWL_ID= "+crawlId;
				stmt.execute(query);
				conn.commit();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
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
		return htmlizedStamp;
	}
	
}
