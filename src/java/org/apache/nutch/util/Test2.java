package org.apache.nutch.util;

public class Test2 {
	
	public static void main(String args[]){
		
		String tempValue = null;
		String hrefValue = "http://usscdev111.plantronics.com:10080/middle-east/category/music-entertainment;jsessionid=09BF288C6F0B1B5878AC95A1972E0CE1.atg2";
		String domainUrl="http://usscdev111.plantronics.com:10080";
		String domain;
		if (domainUrl.contains("http://")) {
			domain = domainUrl.substring(domainUrl.indexOf("//") + 2,
					domainUrl.length());
		} else {
			domain = domainUrl;
		}
		System.out.println("domain:"+hrefValue);
		if (hrefValue.contains(domainUrl)) {
			hrefValue = hrefValue.replace(domainUrl, "");
		}
		System.out.println("hrefValue:"+hrefValue);
	}

}
