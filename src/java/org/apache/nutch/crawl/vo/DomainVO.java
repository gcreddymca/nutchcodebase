package org.apache.nutch.crawl.vo;

import java.util.List;

public class DomainVO {
	private int domainId;
	private String domainName;
	private String url;
	private String seedUrl;
	private String crawlStatus;
	private String raw_content_directory;
	private String final_content_directory;
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

	public void setSegmentVOs(List<SegmentVO> segmentVOs) {
		this.segmentVOs = segmentVOs;
	}

	public String getRaw_content_directory() {
		return raw_content_directory;
	}

	public void setRaw_content_directory(String raw_content_directory) {
		this.raw_content_directory = raw_content_directory;
	}

	public String getFinal_content_directory() {
		return final_content_directory;
	}

	public void setFinal_content_directory(String final_content_directory) {
		this.final_content_directory = final_content_directory;
	}

}
