package org.apache.nutch.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RawHTMLFileCreationUtil {

	public static String getDefaultFolderHierarchy(String url,
			String outputFolder) throws SQLException {
		
		url = url.replaceAll("\\\\", "//");
		url = url.replaceAll(NutchConstants.COLON, NutchConstants.EMPTY_STRING);
		String[] strArr = url.split(NutchConstants.FORWARD_SLASH);
		StringBuilder outputFolderHierarchy = new StringBuilder(outputFolder);
		int arrLength = strArr.length;
		for (int j = 0; j < arrLength; j++) {
			String element = strArr[j].trim();
			if (element.length() == 0
					|| patternMatchesInput(NutchConstants.PROTOCOLS, element)) {
				continue;
			}

			if (patternMatchesInput(NutchConstants.URL_FILE_TYPES_PATTERN, element)) {
				break;
			}

			if (element.length() != 0) {
				outputFolderHierarchy = outputFolderHierarchy
						.append(NutchConstants.FORWARD_SLASH);
				outputFolderHierarchy = outputFolderHierarchy.append(element);
			}

		}
		return outputFolderHierarchy.toString();

	}
	
	

	public static boolean patternMatchesInput(Pattern pattern, String element) {
		Matcher matcher = pattern.matcher(element);
		return matcher.find();
	}

	public static void createDirectories(String dirsName) throws IOException {
		File newDir = new File(dirsName);
		if (!newDir.exists()) {
			newDir.mkdirs();
		}
	}

	public static void createFile(String fileName) throws IOException {
		File newFile = new File(fileName);
		if (!newFile.exists()) {
			newFile.createNewFile();
		}
	}
	
	public static String getRawHtmlContentFileName(String url) {
		String fileName = "index.html";
		if (!patternMatchesInput(NutchConstants.URL_FILE_TYPES_PATTERN, url)) {
			return fileName;
		}
		String group = URLTransformationUtil.getURLPath(url);
		if(group != null)
		fileName = 	url.replace(group, NutchConstants.EMPTY_STRING);
	

		if (fileName != null && fileName.contains(NutchConstants.JSP_EXTN)) {
			fileName = fileName.replace(NutchConstants.JSP_EXTN, NutchConstants.EMPTY_STRING);
			fileName = fileName.replaceAll(NutchConstants.QUESTION_MARK_PATTERN, NutchConstants.SEMICOLON);
			fileName = fileName + NutchConstants.HTML_EXTN;
		}

		return fileName;
	}


}
