package org.apache.nutch.crawl.vo;

import java.util.Date;

public class UrlVO {
	private String url;
	private byte status;
	private long latestFetchTime;
	private long lastFetchTime;
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
	
	public long getLatestFetchTime() {
		return latestFetchTime;
	}
	public void setLatestFetchTime(long latestFetchTime) {
		this.latestFetchTime = latestFetchTime;
	}
	public long getLastFetchTime() {
		return lastFetchTime;
	}
	public void setLastFetchTime(long lastFetchTime) {
		this.lastFetchTime = lastFetchTime;
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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		UrlVO other = (UrlVO) obj;
		if (url == null) {
			if (other.url != null)
				return false;
		} else if (!url.equals(other.url))
			return false;
		return true;
	}
		
}
