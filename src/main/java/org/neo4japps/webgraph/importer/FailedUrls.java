package org.neo4japps.webgraph.importer;

import edu.uci.ics.crawler4j.url.WebURL;
import org.apache.logging.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

public final class FailedUrls {

    private static final FailedUrls singleton = new FailedUrls();

    private static final DateFormat FORMATTER = SimpleDateFormat.getDateTimeInstance();

    // Map type of error --> (Map URL --> status message)
    private final Map<String, Map<String, String>> theMap = new HashMap<>();

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
        synchronized (FORMATTER) {
            return FORMATTER.format(new Date());
        }
    }

    private Map<String, String> getOrCreateMap(String errorType) {
        synchronized (theMap) {
            // using LinkedHashMap to preserve the chronological insertion order when iterating over the map
            return theMap.computeIfAbsent(errorType, k -> Collections.synchronizedMap(new LinkedHashMap<>()));
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

    private void reportErrorType(Logger logger, int maxNumberOfUrlsToDisplay, String errorType,
                                 Map<String, String> map) {
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
