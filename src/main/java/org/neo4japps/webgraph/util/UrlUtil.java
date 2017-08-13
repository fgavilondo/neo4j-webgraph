package org.neo4japps.webgraph.util;

import java.net.MalformedURLException;
import java.net.URL;

public final class UrlUtil {

    /**
     * E.g. for http://mydomain.com this method returns mydomain.com, for http://www.mydomain.com this method also
     * returns mydomain.com
     * 
     * @param urlString
     *            the String to parse as a URL
     * @return the host part of the URL, excluding any leading "www"
     * @throws MalformedURLException
     *             If the string specifies an unknown protocol
     */
    public static String extractHost(String urlString) throws MalformedURLException {
        URL url = new URL(cleanUp(urlString));
        return extractHost(url);
    }

    private static String cleanUp(String urlString) {
        return urlString.replace('\\', '/');
    }

    /**
     * E.g. for http://mydomain.com this method returns mydomain.com, for http://www.mydomain.com this method also
     * returns mydomain.com
     * 
     * @param url
     *            the URL to analyze
     * @return the host part of the URL, excluding any leading "www"
     */
    private static String extractHost(URL url) {
        return strip("www.", url.getHost());
    }

    /**
     * E.g. for http://mydomain.com this method returns "mydomain", for http://www.mydomain.com this method also returns
     * "mydomain"
     * 
     * @param url
     *            the URL to analyze
     * @return the domain part of the URL, i.e. the part up to the first ".", ignoring any leading "www"
     */
    public static String extractDomain(URL url) {
        String host = extractHost(url);
        String domain = host;
        int indexOfFirstDot = host.indexOf('.');
        if (indexOfFirstDot != -1) {
            domain = host.substring(0, indexOfFirstDot);
        }
        return domain;
    }

    private static String strip(String prefix, String host) {
        if (host.startsWith(prefix)) {
            return host.replaceFirst(prefix, "");
        }

        return host;
    }

    public static boolean isHomePage(URL url) {
        // Deal with \ in URLs
        // Browsers silently convert \ to / in URLs to fix accidental mistakes, but the URL class doesn't do so,
        // which means that such URLs appear to have an empty path, which is misleading
        String urlString = url.toString();

        // first remove any trailing \
        while (urlString.endsWith("\\")) {
            urlString = urlString.substring(0, urlString.length() - 1);
        }

        // if there are still any \ in the middle of the URL then it is definitely a leaf page with some (incorrect)
        // path in it
        if (urlString.contains("\\")) {
            return false;
        }

        // now we are dealing with a clean URL
        String path = url.getPath().trim();
        if ((path.isEmpty() || path.equals("/")) && url.getQuery() == null && url.getRef() == null) {
            return true;
        }

        return false;
    }
}
