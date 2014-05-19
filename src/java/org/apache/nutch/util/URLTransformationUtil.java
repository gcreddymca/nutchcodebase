package org.apache.nutch.util;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.util.NutchConfiguration;
import org.mortbay.log.Log;

public class URLTransformationUtil {
	private static final Configuration conf;
	private static final Pattern[] requestParamsExclusionPatterns;

	static {
		conf = NutchConfiguration.create();
		String[] exclusionList = conf
				.getStrings("url.request.parameter.exclusion.list");
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

	public static String excludeRequestParameters(String url) {
		String urlPath = url;
		if (url.contains(NutchConstants.HTML_EXTN))
			return urlPath;

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
}
