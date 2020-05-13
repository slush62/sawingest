package com.peraton.wmss.noaa.sawingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.TextMessage;
import javax.jms.MessageProducer;
import javax.jms.JMSException;

import java.io.*;
import java.util.HashMap;
import java.util.Locale;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

@SpringBootApplication
public class SawingestApplication {

	static Session ivSession;
	static MessageProducer ivProducer;
	static Logger ivLogger;
	static DateTime ivNow;
	static DateTimeFormatter ivFormatter;
	
	static int ivNumWatches;
	static int ivReplace;
	
	static double ivLat[][];
	static double ivLon[][];
	
	static HashMap<Integer,String> ivRemove;
	static HashMap<Integer,String> ivRawText;
	static HashMap<Integer,String> ivID;
	static HashMap<Integer,String> ivEndTime;
	static HashMap<Integer,String> ivWatchType;
	
	public SawingestApplication () throws JMSException, IOException {
		String queueName = System.getenv("QUEUENAME");
		String aURL = System.getenv("ARTEMISURL");
		String userName = System.getenv("USERNAME");
		String passWord = System.getenv("PASSWORD");
		try {
			JmsConnectionFactory factory = new JmsConnectionFactory(aURL);
			Connection connection = factory.createConnection(userName, passWord);
			connection.start();

			ivSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination destination = null;
			destination = ivSession.createQueue(queueName);	
			_setProducer(ivSession, destination);
		} catch( JMSException e) {
		    e.printStackTrace();
		}

		ivLogger = Logger.getLogger("SawingestApplication");
		
		File watchesDir = new File ("./watches");
		if( !watchesDir.exists()) {
			watchesDir.mkdir();
		}
		
		File logDir = new File ("./logs");
		if( !logDir.exists()) {
			logDir.mkdir();
		}
		FileHandler fh = new FileHandler(logDir+"/Sawingest.log");
		fh.setFormatter(new SimpleFormatter());
		fh.setLevel(Level.INFO);
		ivLogger.addHandler(fh);
	}
	
	public static void main(String[] args) throws JMSException {
		SpringApplication.run(SawingestApplication.class, args);

		DateTimeFormatter ivFormatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss").appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
		DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withLocale(Locale.ROOT).withChronology(ISOChronology.getInstanceUTC());

		String delayStr = System.getenv("DELAYTIMESECS");
		int delaySeconds = Integer.parseInt(delayStr);


		while (true) {
			ivNumWatches = 0;
			ivReplace = 0;
			ivRemove = new HashMap<Integer,String>();
			ivRawText = new HashMap<Integer,String>();
			ivEndTime = new HashMap<Integer,String>();
			ivID = new HashMap<Integer,String>();
			ivWatchType = new HashMap<Integer,String>();
			
			ivLat = new double[4][64];
			ivLat = new double[4][64];
			DateTime ivNow = new DateTime(DateTimeZone.UTC);
			
			try {
				for(int sawBulletin=0; sawBulletin<10; sawBulletin++) {
					String aURL = "https://tgftp.nws.noaa.gov/data/raw/ww/wwus30.kwns.saw."+sawBulletin+".txt";
					String aFile1 = "watches/KWNSWWUS30.saw."+sawBulletin+".txt";
					File outFile = new File(aFile1);
					if(outFile.exists()) {
						outFile.delete();
					}
					ivLogger.info("Getting: "+aURL+" as: "+aFile1);
					writeURLtoFile(aURL,aFile1);
					
					aURL = "https://tgftp.nws.noaa.gov/data/raw/wo/wous64.kwns.wou."+sawBulletin+".txt";
					String aFile2 = "watches/KWNSWWUS30.wou."+sawBulletin+".txt";
					outFile = new File(aFile2);
					if(outFile.exists()) {
						outFile.delete();
					}
					ivLogger.info("Getting: "+aURL+" as: "+aFile2);
					writeURLtoFile(aURL,aFile2);

					checkWatchFiles(aFile1, aFile2);
                }
				ivLogger.info("Number of watches: "+ivNumWatches);
				for(int watch=1;watch<ivNumWatches;watch++) {
					String payload = getJSONforWatch(watch);
					ivLogger.info(payload);
		            TextMessage msg = ivSession.createTextMessage(payload);
		            _getProducer().send(msg);								
				}
				ivLogger.info("Sleep "+delayStr+" - "+ivNow.toString(ivFormatter));
				try {
					Thread.sleep(delaySeconds*1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
			}
		}

	}


	private static String getJSONforWatch(int watch) throws JsonProcessingException {
		boolean doReplace = false;
		for(int k = 0;k<ivReplace;k++) {
			if(ivRemove.get(k).indexOf(ivID.get(k)) >= 0) {
				doReplace = true;
			}
		}
		if(! doReplace) {
			String poly = "Polygon((";
			for(int k=0;k<4;k++) {
				poly += String.valueOf(-1*ivLon[k][watch])+" "+String.valueOf(ivLat[k][watch])+",";
			}
			poly += String.valueOf(-1*ivLon[0][watch])+" "+String.valueOf(ivLat[0][watch])+"))";
			ivLogger.info(poly);
			
			DateTime endDt = new DateTime(DateTimeZone.UTC);
			String endDay = ivEndTime.get(watch).substring(0, 1);
			String endHR = ivEndTime.get(watch).substring(2, 3);
			String endMin = ivEndTime.get(watch).substring(4, 5);
			if(Integer.parseInt(endHR) < ivNow.getHourOfDay() && ivNow.getHourOfDay() > 12) {
				if(Integer.parseInt(endDay) < ivNow.getDayOfMonth() && Integer.parseInt(endDay) == 1) {
					int endMonth = ivNow.getMonthOfYear() + 1;
					int endYear = ivNow.getYear();
					if(endMonth > 12) {
						endMonth = 1;
						endYear++;
					}
					endDt = ivFormatter.parseDateTime(String.valueOf(endYear)+"-"+String.valueOf(endMonth)+"-"+endDay+" "+endHR+":"+endMin);
				}
			}
			HashMap<String,String> jMap = new HashMap<>();
			jMap.put("appname","sawingest");
			jMap.put("rawtext",ivRawText.get(watch));
			jMap.put("watchtype",ivWatchType.get(watch));
			jMap.put("watchnumber",ivID.get(watch));
			jMap.put("validuntil",endDt.toString());
			jMap.put("geom",poly);
			ObjectMapper om = new ObjectMapper();
			return om.writeValueAsString(jMap);
		}
		return null;
	}

	private void _setProducer(Session aSession, Destination aDestination)  throws JMSException {
		if( ivProducer == null) {
			ivProducer = aSession.createProducer(aDestination);
		}
	}
	
	private static MessageProducer _getProducer() { return ivProducer; }
	
	private static void writeURLtoFile(String aURL, String fileName) throws IOException {
		CloseableHttpClient httpClient = HttpClients.createDefault();
		try {
			System.out.println(aURL);
			HttpGet request = new HttpGet(aURL);
			request.addHeader("accept","text/plain");
			CloseableHttpResponse response = httpClient.execute(request);
			try {
				// Get HttpResponse Status
				if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
					HttpEntity entity = response.getEntity();
					if (entity != null) {
						String content = EntityUtils.toString(entity);
						BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
						writer.write(content);
						writer.close();
					}
				} else {
					System.out.println("HTTP Response: "+response.getStatusLine().getStatusCode()+"\n"+response.getStatusLine().getReasonPhrase());
				}
			} finally {
				response.close();
			}
		} finally {
			httpClient.close();
		}
		
	}

	private static void checkWatchFiles(String aFile1, String aFile2) throws IOException {
        BufferedReader reader = null;
        boolean cancelled = false;
        boolean watchAdded = false;
		try {
			reader = new BufferedReader(new FileReader(aFile1));
			String line = null;
			while( (line=reader.readLine()) != null) {
				System.out.println(line);
				if(line.indexOf("WW ") == 0) {
					ivNumWatches++;
					watchAdded = true;
					if(line.indexOf("CANCELLED") >= 0 || line.indexOf("EXPIRED") >= 0) {
						ivNumWatches--;
						watchAdded = false;
						cancelled = true;
					} else {
						String[] columns = line.split(" ");
						if(columns[2].indexOf("TORNADO") >= 0) {
							ivWatchType.put(ivNumWatches, "WT");
						} else {
							ivWatchType.put(ivNumWatches, "WS");							
						}
						ivID.put(ivNumWatches, columns[1]);
						int timeloc = line.indexOf("Z - ");
						ivEndTime.put(ivNumWatches, line.substring(timeloc+4, timeloc+10));
						ivRawText.put(ivNumWatches, line);
						cancelled = false;
						File theFile2 = new File(aFile2);
						if(theFile2.exists()) {
							BufferedReader reader2 = null;
							try {
								reader2 = new BufferedReader(new FileReader(aFile2));
								String line2 = null;
								while((line2 = reader2.readLine()) != null) {
									if(line2.indexOf("CANCELLED") >= 0 || line2.indexOf("EXPIRED") >= 0 || line2.indexOf("NO LONGER IN EFFECT") >= 0) {
										if(ivID.containsKey(ivNumWatches)) {
											ivID.remove(ivNumWatches);
										}
										if(ivWatchType.containsKey(ivNumWatches)) {
											ivWatchType.remove(ivNumWatches);
										}
										if(ivEndTime.containsKey(ivNumWatches)) {
											ivEndTime.remove(ivNumWatches);
										}
										if(ivRawText.containsKey(ivNumWatches)) {
											ivRawText.remove(ivNumWatches);
										}
										cancelled = true;
										watchAdded = false;
										ivNumWatches--;
									}
								}
							} finally {
								reader2.close();
							}
						}
					}
				} else if (line.indexOf("REPLACES WW") >= 0) {
					ivRemove.put(ivReplace,line.substring(12, 14));
					ivReplace++;
				} else if (line.indexOf("LAT...LON") >= 0 && cancelled == false) {
					if(ivRawText.containsKey(ivNumWatches)) {
						String newVal = ivRawText.get(ivNumWatches)+line;
						ivRawText.replace(ivNumWatches, newVal);
					} else {
						ivRawText.put(ivNumWatches, line);
					}
					String[] location = line.split(" ");
					for(int i=1; i<5; i++) {
						ivLat[i-1][ivNumWatches] = Double.parseDouble(location[i].substring(0, 3))/100.0;
						ivLon[i-1][ivNumWatches] = Double.parseDouble(location[i].substring(4, 7))/100.0;
						if(ivLon[i-1][ivNumWatches] < 60.0) {
							ivLon[i-1][ivNumWatches] += 100.0;
						}
					}
					if(Integer.parseInt(ivID.get(ivNumWatches)) == 9999) {
						if(ivID.containsKey(ivNumWatches)) {
							ivID.remove(ivNumWatches);
						}
						if(ivWatchType.containsKey(ivNumWatches)) {
							ivWatchType.remove(ivNumWatches);
						}
						if(ivEndTime.containsKey(ivNumWatches)) {
							ivEndTime.remove(ivNumWatches);
						}
						if(ivRawText.containsKey(ivNumWatches)) {
							ivRawText.remove(ivNumWatches);
						}
						ivNumWatches--;
						watchAdded = false;
					}
				}
			}
		} finally {
			reader.close();
		}
		if(watchAdded) {
			String aStr = ivNow.toString("MMHHmm");
			if(Integer.parseInt(aStr) > Integer.parseInt(ivEndTime.get(ivNumWatches))) {
				if(ivID.containsKey(ivNumWatches)) {
					ivID.remove(ivNumWatches);
				}
				if(ivWatchType.containsKey(ivNumWatches)) {
					ivWatchType.remove(ivNumWatches);
				}
				if(ivEndTime.containsKey(ivNumWatches)) {
					ivEndTime.remove(ivNumWatches);
				}
				if(ivRawText.containsKey(ivNumWatches)) {
					ivRawText.remove(ivNumWatches);
				}
				ivNumWatches--;				
			}
		}
	}

}
