package org.apache.nutch.crawl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.crawl.dao.DomainDAO;
import org.apache.nutch.crawl.dao.SegmentMasterDAO;
import org.apache.nutch.crawl.vo.DomainVO;
import org.apache.nutch.fetcher.Fetcher;
import org.apache.nutch.parse.ParseSegment;
import org.apache.nutch.util.NutchConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Crawler {

	private static final int READ_CRAWL_DB_ARGS_LEN = 2;
	private static final int UPDATEDB_ARGS_LEN = 2;
	private static final int PARSER_ARGS_LEN = 1;
	private static final int FETCH_ARGS_LEN = 1;
	private static final int GENERATOR_ARGS_LEN = 2;
	private static final int INJECTOR_ARGS_LEN = 1;
	public static final Logger LOG = LoggerFactory.getLogger(Crawler.class);

	/**
	 * This method updates nutch configuration based on the domain that needs to
	 * be crawled
	 * 
	 * @param crawlDir
	 * @param numberOfRounds
	 * @param domainId
	 * @return
	 * @throws Exception
	 */
	public boolean crawlByDomain(String crawlDir, int numberOfRounds,
			int domainId, List<String> url_pattern_rules) throws Exception {

		DomainDAO domainDAO = new DomainDAO();
		SegmentMasterDAO segmentDAO = new SegmentMasterDAO();
		  List<String> segmentUrlRules = new ArrayList<String>();
		// Check if crawl is in progress
		long count = domainDAO.checkCrawlInProgress();
		// Throw exception if crawl is already in progress
		if (count != 0) {
			throw new IllegalArgumentException("Crawling in progress");
		}
		if (crawlDir == null || crawlDir.length() == 0) {
			throw new IllegalArgumentException("Missing crawl directory path");
		}
	
		deleteDirectory(crawlDir);

		boolean success = false;

		// Fetch domain that needs to be crawled
		DomainVO domainVO = domainDAO.readByPrimaryKey(domainId);
		if(domainVO == null){
			throw new IllegalArgumentException("Domain Id passed is incorrect");
		}
		segmentUrlRules = segmentDAO.readNotCrawlSegmentUrlRule(domainId);
		Configuration conf = NutchConfiguration.create();
		String seedFile = conf.get("seed.urls.file");
		String regexFileTemplate = conf.get("urlfilter.regex.file.template");
		String regexFile = conf.get("urlfilter.regex.file");

		
		
		StringBuilder allowedUrls = new StringBuilder();
		// Create seed url file for domain being crawled
		URL seedUrl = Thread.currentThread().getContextClassLoader()
				.getResource(seedFile);
		FileWriter writer = new FileWriter(seedUrl.getPath());
		StringTokenizer seedUrls = new StringTokenizer(domainVO.getSeedUrl(), " ");
		String seedUrlsToken = null;
		while(seedUrls.hasMoreTokens()) {
			seedUrlsToken = seedUrls.nextToken();
			writer.write(domainVO.getUrl() + seedUrlsToken );
			writer.write(System.getProperty( "line.separator" ));
			if(domainVO.getSeedUrl().trim().length() > 1)
				allowedUrls.append("+^" + domainVO.getUrl() + seedUrlsToken + "\n");
			else
				allowedUrls.append("+^" + domainVO.getUrl());
		}
		
		writer.flush();
		writer.close();

		// Read regex-urlfilter.txt.template
		URL regexUrlTemplate = Thread.currentThread().getContextClassLoader()
				.getResource(regexFileTemplate);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				regexUrlTemplate.openStream()));
		String currentLine = null;
		StringBuilder regexString = new StringBuilder();

		// Store content as string
		while ((currentLine = reader.readLine()) != null) {
			regexString.append(currentLine + "\n");
		}
		reader.close();
		if(url_pattern_rules !=null && url_pattern_rules.size()>0){
			for (String urlRule : url_pattern_rules) {
				regexString.append("-^" + domainVO.getUrl()+ "/" + urlRule+"\n");
			}
		}
		else if(segmentUrlRules!=null && segmentUrlRules.size()>0){
			for(String urlRule : segmentUrlRules){
				regexString.append("-^" + domainVO.getUrl()+ "/" + urlRule+"\n");
			}
		}
		
		regexString.append(allowedUrls);
		
		URL regexUrl = Thread.currentThread().getContextClassLoader()
				.getResource(regexFile);
		FileWriter regexWriter = new FileWriter(regexUrl.getPath());
		regexWriter.write(regexString.toString());
		regexWriter.flush();
		regexWriter.close();

		domainVO.setCrawlStatus("STARTED");
		domainDAO.updateCrawlStatus(domainVO);

		// Create crawldb path
		String crawldbPath = crawlDir + "/crawldb";

		// Create segment directory path
		String segmentPath = crawlDir + "/segments";
		String[] args = null;
		try {

			success = crawl(numberOfRounds, seedUrl, crawldbPath, segmentPath);
			if (success) {
				domainVO.setCrawlStatus("DONE");
				domainDAO.updateCrawlStatus(domainVO);
			}

		} catch (Exception e) {
			LOG.info("Error occurred while crawling" + e.getLocalizedMessage());
			e.printStackTrace();
			domainVO.setCrawlStatus("FAILED");
			domainDAO.updateCrawlStatus(domainVO);
			throw new Exception(e.getLocalizedMessage());
		}

		// Read crawldb on successful completion of crawl
		if (success) {

			// Set crawldb and -dbDump option as arguments for Readdb
			args = new String[READ_CRAWL_DB_ARGS_LEN];
			args[0] = crawldbPath;
			args[1] = "-dbDump";
			try {
				CrawlDbReader.main(args);
			} catch (IOException i) {

				// Crawl db should not fail unless crawl dir is incorrect
				LOG.info("eError while reading crawling db"
						+ i.getLocalizedMessage());
			}
		}
		if(domainVO.getCrawlStatus().equals("STARTED")){
			domainVO.setCrawlStatus("FAILED");
			domainDAO.updateCrawlStatus(domainVO);
		}
		return success;
	}

	/**
	 * This method deletes a directory and its sub directories
	 * 
	 * @param directoryName
	 */
	private void deleteDirectory(String directoryName) {
		File directoryObj = new File(directoryName);
		if (directoryObj.exists()) {
			String[] fileList = directoryObj.list();
			if (fileList != null && fileList.length > 0) {
				for (String dirName : fileList) {
					deleteDirectory(directoryName+"/"+dirName);
				}
			}
			directoryObj.delete();

		}

	}

	/**
	 * This method executes all the steps required for crawling for a domain or
	 * domains in the following order Inject -> Generate -> Fetch -> Parse ->
	 * UpdateDb
	 * 
	 * @param crawlDir
	 * @param numberOfRounds
	 * @return Returns whether crawl was successful or not
	 * @throws Exception
	 */

	private boolean crawl(int numberOfRounds, URL seedUrl, String crawldbPath,
			String segmentPath) throws Exception {
		String[] args;
		File[] listOfSegments = null;
		File segmentPathFile = new File(segmentPath);

		// If segments are already created then seed urls have already been
		// injected in crawl db
		if (!segmentPathFile.exists()) {

			// Set Injector arguments as crawlDir and seed urls
			args = new String[2];
			args[1] = seedUrl.getPath();
			args[0] = crawldbPath;

			Injector.main(args);
		}

		if (!new File(crawldbPath + "/current").exists()) {
			return false;
		}
		// Run Generator, Fetcher, ParseSegment and Update db as many times
		// as number of rounds
		for (int i = 0; i < numberOfRounds; i++) {

			// Set Generator arguments as crawldb and segments directory
			// path
			args = new String[GENERATOR_ARGS_LEN];
			args[0] = crawldbPath;
			args[1] = segmentPath;

			Generator.main(args);

			segmentPathFile = new File(segmentPath);
			if (segmentPathFile.isDirectory()) {
				List<File> sortedListOfSegs = Arrays.asList(segmentPathFile.listFiles());
				Collections.sort(sortedListOfSegs, new Comparator<File>() {
			        @Override
			        public int compare(File o1, File o2) {
			            String p1 = o1.getAbsolutePath();
			            String p2 = o2.getAbsolutePath();
			            return p1.compareTo(p2);
			        }
			    });
				listOfSegments = new File[sortedListOfSegs.size()];
				listOfSegments = sortedListOfSegs.toArray(listOfSegments);
				
			}

			// Run Fetcher, ParseSegment and Update db only if generate has
			// created a new segment to crawled
			if (listOfSegments == null || listOfSegments.length == i) {
				break;
			}

			// Set latest segment path as argument for Fetcher
			args = new String[FETCH_ARGS_LEN];
			args[0] = listOfSegments[i].getAbsolutePath();

			Fetcher.main(args);

			// Set latest segment path as argument for ParseSegment
			args = new String[PARSER_ARGS_LEN];
			args[0] = listOfSegments[i].getAbsolutePath();

			ParseSegment.main(args);

			// Set latest segment path and crawldb as argument for Update db
			args = new String[UPDATEDB_ARGS_LEN];
			args[0] = crawldbPath;
			args[1] = listOfSegments[i].getAbsolutePath();
			CrawlDb.main(args);

		}
		return true;
	}

	/**
	 * This method updates nutch configuration based upon the domains to be
	 * crawled
	 * 
	 * @param crawlDir
	 * @param numberOfRounds
	 * @return
	 * @throws Exception
	 */
	public boolean crawlAllDomains(String crawlDir, int numberOfRounds)
			throws Exception {
		boolean success = false;
		DomainDAO domainDAO = new DomainDAO();
		long count = domainDAO.checkCrawlInProgress();
		if (count != 0) {
			throw new IllegalArgumentException("Crawling in progress");
		}
		if (crawlDir == null || crawlDir.length() == 0) {
			throw new IllegalArgumentException("Missing crawl directory path");
		}
		
		deleteDirectory(crawlDir);

		List<DomainVO> domainVOs = domainDAO.read();
		Configuration conf = NutchConfiguration.create();
		String seedFile = conf.get("seed.urls.file");
		String regexFileTemplate = conf.get("urlfilter.regex.file.template");
		String regexFile = conf.get("urlfilter.regex.file");

		// Create seed url file for domain being crawled
		URL seedUrl = Thread.currentThread().getContextClassLoader()
				.getResource(seedFile);
		FileWriter writer = new FileWriter(seedUrl.getPath());
		StringBuilder regex = new StringBuilder("+^(");
		int cnt = 0;
		for (DomainVO domainVO : domainVOs) {

			writer.write(domainVO.getUrl() + domainVO.getSeedUrl() + "\n");
			domainVO.setCrawlStatus("STARTED");
			domainDAO.updateCrawlStatus(domainVO);
			regex.append(domainVO.getUrl());

			cnt++;
			if (cnt != domainVOs.size()) {
				regex.append("|");
			}
		}
		regex.append(")");
		writer.flush();
		writer.close();

		URL regexTemplateUrl = Thread.currentThread().getContextClassLoader()
				.getResource(regexFileTemplate);
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				regexTemplateUrl.openStream()));
		StringBuilder regexString = new StringBuilder();
		String currentLine = null;

		while ((currentLine = reader.readLine()) != null) {
			regexString.append(currentLine + "\n");
		}
		regexString.append(regex.toString());
		reader.close();
		URL regexUrl = Thread.currentThread().getContextClassLoader()
				.getResource(regexFile);
		FileWriter regexWriter = new FileWriter(regexUrl.getPath());
		regexWriter.write(regexString.toString());
		regexWriter.flush();
		regexWriter.close();

		// Create crawldb path
		String crawldbPath = crawlDir + "/crawldb";

		// Create segment directory path
		String segmentPath = crawlDir + "/segments";
		String[] args = null;
		try {

			success = crawl(numberOfRounds, seedUrl, crawldbPath, segmentPath);
			if (success) {
				for (DomainVO domainVO : domainVOs) {
					domainVO.setCrawlStatus("FAILED");
					domainDAO.updateCrawlStatus(domainVO);
				}
			}

		} catch (Exception e) {
			LOG.info("Error occurred while crawling" + e.getLocalizedMessage());
			e.printStackTrace();
			for (DomainVO domainVO : domainVOs) {
				domainVO.setCrawlStatus("DONE");
				domainDAO.updateCrawlStatus(domainVO);
			}
			throw new Exception(e.getLocalizedMessage());
		}

		// Read crawldb on successful completion of crawl
		if (success) {

			// Set crawldb and -dbDump option as arguments for Readdb
			args = new String[READ_CRAWL_DB_ARGS_LEN];
			args[0] = crawldbPath;
			args[1] = "-dbDump";
			try {
				CrawlDbReader.main(args);
			} catch (IOException i) {

				// Crawl db should not fail unless crawl dir is incorrect
				LOG.info("Error while reading crawling db"
						+ i.getLocalizedMessage());
			}
		}
		for (DomainVO domainVO : domainVOs) {

			if (domainVO.getCrawlStatus().equals("STARTED")) {
				domainVO.setCrawlStatus("FAILED");
				domainDAO.updateCrawlStatus(domainVO);
			}
		}
		return success;
	}
}
