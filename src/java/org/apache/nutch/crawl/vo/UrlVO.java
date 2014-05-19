package org.apache.nutch.crawl.vo;

import java.util.Date;

public class UrlVO {
	private String url;
	private byte status;
	private long fetchTime;
	private long modifiedTime;
	private byte retriesSinceFetch;
	private int fetchInterval;
	private float score;
	private String signature;
	private String metadata;
	private int segmentId;
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public byte getStatus() {
		return status;
	}
	public void setStatus(byte status) {
		this.status = status;
	}
	public long getFetchTime() {
		return fetchTime;
	}
	public void setFetchTime(long fetchTime) {
		this.fetchTime = fetchTime;
	}
	public long getModifiedTime() {
		return modifiedTime;
	}
	public void setModifiedTime(long modifiedTime) {
		this.modifiedTime = modifiedTime;
	}
	public byte getRetriesSinceFetch() {
		return retriesSinceFetch;
	}
	public void setRetriesSinceFetch(byte retriesSinceFetch) {
		this.retriesSinceFetch = retriesSinceFetch;
	}
	public int getFetchInterval() {
		return fetchInterval;
	}
	public void setFetchInterval(int fetchInterval) {
		this.fetchInterval = fetchInterval;
	}
	public float getScore() {
		return score;
	}
	public void setScore(float score) {
		this.score = score;
	}
	public String getSignature() {
		return signature;
	}
	public void setSignature(String signature) {
		this.signature = signature;
	}
	public String getMetadata() {
		return metadata;
	}
	public void setMetadata(String metadata) {
		this.metadata = metadata;
	}
	public int getSegmentId() {
		return segmentId;
	}
	public void setSegmentId(int segmentId) {
		this.segmentId = segmentId;
	}

	
}
