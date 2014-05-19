package org.apache.nutch.crawl.vo;

import java.util.List;

public class DomainVO {
	private int domainId;
	private String domainName;
	private String url;
	private String seedUrl;
	private String crawlStatus;
	private String directory;
	private List<SegmentVO> segmentVOs;
	
	public int getDomainId() {
		return domainId;
	}
	public void setDomainId(int domainId) {
		this.domainId = domainId;
	}
	public String getDomainName() {
		return domainName;
	}
	public void setDomainName(String domainName) {
		this.domainName = domainName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getSeedUrl() {
		return seedUrl;
	}
	public void setSeedUrl(String seedUrl) {
		this.seedUrl = seedUrl;
	}
	public List<SegmentVO> getSegmentVOs() {
		return segmentVOs;
	}
	public String getCrawlStatus() {
		return crawlStatus;
	}
	public void setCrawlStatus(String crawlStatus) {
		this.crawlStatus = crawlStatus;
	}
	public String getDirectory() {
		return directory;
	}
	public void setDirectory(String directory) {
		this.directory = directory;
	}
	public void setSegmentVOs(List<SegmentVO> segmentVOs) {
		this.segmentVOs = segmentVOs;
	}
	
}
