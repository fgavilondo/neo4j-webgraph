package org.neo4japps.webgraph.importer;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import edu.uci.ics.crawler4j.url.WebURL;

public final class FailedUrls {

    private static final FailedUrls singleton = new FailedUrls();

    private final DateFormat formatter = SimpleDateFormat.getDateTimeInstance();

    // Map type of error --> (Map URL --> status message)
    private final Map<String, Map<String, String>> theMap = new HashMap<String, Map<String, String>>();

    public static FailedUrls getInstance() {
        return singleton;
    }

    public void add(String errorType, WebURL url, String statusMessage) {
        add(errorType, url.getURL(), statusMessage);
    }

    public void add(String errorType, String url, String statusMessage) {
        Map<String, String> map = getOrCreateMap(errorType);
        map.put(url, statusMessage + " - " + now());
    }

    private String now() {
        synchronized (formatter) {
            return formatter.format(new Date());
        }
    }

    private Map<String, String> getOrCreateMap(String errorType) {
        synchronized (theMap) {
            Map<String, String> map = theMap.get(errorType);
            if (map == null) {
                // using LinkedHashMap to preserve the chronological insertion order when iterating over the map
                map = Collections.synchronizedMap(new LinkedHashMap<String, String>());
                theMap.put(errorType, map);
            }
            return map;
        }
    }

    public String getStatusMessage(String errorType, WebURL webUrl) {
        return getOrCreateMap(errorType).get(webUrl.getURL());
    }

    public void report(Logger logger, int maxNumberOfUrlsToDisplay) {
        for (Entry<String, Map<String, String>> entry : theMap.entrySet()) {
            reportErrorType(logger, maxNumberOfUrlsToDisplay, entry.getKey(), entry.getValue());
        }
    }

    private void reportErrorType(Logger logger, int maxNumberOfUrlsToDisplay, String errorType, Map<String, String> map) {
        if (map.isEmpty()) {
            return;
        }

        logger.info(errorType + ": " + map.size() + " URLs");
        
        if (map.size() <= maxNumberOfUrlsToDisplay) {
            for (Entry<String, String> entry : map.entrySet()) {
                logger.info("  " + entry.getKey() + ": " + entry.getValue());
            }
        } else {
            reportToFile(logger, errorType, map);
        }
    }

    private void reportToFile(Logger logger, String errorType, Map<String, String> map) {
        final String fileName = errorType + ".report.txt";
        logger.info("Too many to display. See file: " + fileName);
        try {
            writeUrlsToFile(map, fileName);
        } catch (IOException e) {
            logger.warn(e);
        }
    }

    private void writeUrlsToFile(Map<String, String> map, String fileName) throws IOException {
        final String lineSeparator = System.getProperty("line.separator");
        final FileWriter fw = new FileWriter(fileName, false);
        for (Entry<String, String> entry : map.entrySet()) {
            fw.append(entry.getKey() + ": " + entry.getValue() + lineSeparator);
        }
        fw.close();
    }
}
