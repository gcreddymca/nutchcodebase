package org.apache.nutch.util;

import java.util.regex.Pattern;

public class NutchConstants {
	public static final String QUESTION_MARK = "?";
	public static final String SEMICOLON = ";";
	public static final String AMPERSAND = "&";
	public static final String HTML_EXTN = ".html";
	public static final String JSP_EXTN = ".jsp";
	public static final String EMPTY_STRING = "";
	public static final String COLON = ":";
	public static final String FORWARD_SLASH = "/";
	public static final String QUESTION_MARK_PATTERN = "\\?";
	public static final Pattern PROTOCOLS = Pattern.compile("http");
	public static final Pattern URL_FILE_TYPES_PATTERN = Pattern
			.compile("(jsp|html)");
	public static final Pattern URL_PATH_PATTERN = Pattern
			.compile("^[\\w-./:]*/");
	public static final Pattern URL_REQUEST_PARAMS_PATTERN = Pattern
			.compile("[&;\\?][\\w\\W]*");
	public static final Pattern URL_BOOKMARKS = Pattern
			.compile("#[\\w-]*$");
	
	private NutchConstants(){
		
	}
}
