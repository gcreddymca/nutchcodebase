package org.apache.nutch.crawl.vo;

import java.util.List;

public class SegmentVO {
	
	private int segmentId;
	private String segmentName;
	private String rule;
	private boolean crawl;
	private int priority;
	private List<UrlVO> urlVOs;
	private int domainId;
	
	public int getDomainId() {
		return domainId;
	}
	public void setDomainId(int domainId) {
		this.domainId = domainId;
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
	public String getRule() {
		return rule;
	}
	public void setRule(String rule) {
		this.rule = rule;
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
	
}
