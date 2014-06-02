package org.apache.nutch.crawl.vo;

import java.util.List;

public class SegmentVO {
	
	private int segmentId;
	private String segmentName;
	private String url_pattern_rule;
	
	private boolean crawl;
	private int priority;
	private List<UrlVO> urlVOs;
	private int domainId;
	private long crawlInterval;
	private long nextFetchTime;
	
	
	public String getUrl_pattern_rule() {
		return url_pattern_rule;
	}
	public void setUrl_pattern_rule(String url_pattern_rule) {
		this.url_pattern_rule = url_pattern_rule;
	}
	public int getSegmentId() {
		return segmentId;
	}
	public void setSegmentId(int segmentId) {
		this.segmentId = segmentId;
	}
	public String getSegmentName() {
		return segmentName;
	}
	public void setSegmentName(String segmentName) {
		this.segmentName = segmentName;
	}
	
	public boolean isCrawl() {
		return crawl;
	}
	public void setCrawl(boolean crawl) {
		this.crawl = crawl;
	}
	
	public int getPriority() {
		return priority;
	}
	public void setPriority(int priority) {
		this.priority = priority;
	}
	public List<UrlVO> getUrlVOs() {
		return urlVOs;
	}
	public void setUrlVOs(List<UrlVO> urlVOs) {
		this.urlVOs = urlVOs;
	}
	
	public int getDomainId() {
		return domainId;
	}
	public void setDomainId(int domainId) {
		this.domainId = domainId;
	}
	public long getCrawlInterval() {
		return crawlInterval;
	}
	public void setCrawlInterval(long crawlInterval) {
		this.crawlInterval = crawlInterval;
	}
	public long getNextFetchTime() {
		return nextFetchTime;
	}
	public void setNextFetchTime(long nextFetchTime) {
		this.nextFetchTime = nextFetchTime;
	}
	
}
