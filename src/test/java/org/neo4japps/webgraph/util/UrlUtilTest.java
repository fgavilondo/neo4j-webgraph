package org.neo4japps.webgraph.util;

import java.net.MalformedURLException;
import java.net.URL;

import junit.framework.TestCase;

public class UrlUtilTest extends TestCase {

    public void testHostExtractedCorrectlyFromUrlStrings() throws MalformedURLException {
        assertEquals("mydomain.com", UrlUtil.extractHost("http://mydomain.com"));
        assertEquals("mydomain.com", UrlUtil.extractHost("http://www.mydomain.com"));

        assertEquals("subdomain.mydomain.com", UrlUtil.extractHost("http://subdomain.mydomain.com"));
        assertEquals("subdomain.mydomain.com", UrlUtil.extractHost("http://www.subdomain.mydomain.com"));
        
        assertEquals("subdomain.mydomain.com",
                UrlUtil.extractHost("http://subdomain.mydomain.com\\img\\someimage.jpg/"));
        assertEquals("subdomain.mydomain.com",
                UrlUtil.extractHost("http://subdomain.mydomain.com\\img\\someimage.jpg"));
    }

    public void testDomainsExtractedCorrectlyFromUrl() throws MalformedURLException {
        assertEquals("mydomain", UrlUtil.extractDomain(new URL("http://mydomain.com")));
        assertEquals("mydomain", UrlUtil.extractDomain(new URL("http://www.mydomain.com")));

        assertEquals("subdomain", UrlUtil.extractDomain(new URL("http://subdomain.mydomain.com")));
        assertEquals("subdomain", UrlUtil.extractDomain(new URL("http://www.subdomain.mydomain.com")));
        assertEquals("subdomain",
                UrlUtil.extractDomain(new URL("http://subdomain.mydomain.com\\img\\someimage.jpg/")));
        assertEquals("subdomain",
                UrlUtil.extractDomain(new URL("http://subdomain.mydomain.com\\img\\someimage.jpg")));
    }

    public void testPageTypeDeterminedCorrectlyFromUrl() throws MalformedURLException {
        assertTrue(UrlUtil.isHomePage(new URL("http://mydomain.com")));
        assertTrue(UrlUtil.isHomePage(new URL("http://mydomain.com/")));
        assertTrue(UrlUtil.isHomePage(new URL("http://mydomain.com\\\\")));

        assertFalse(UrlUtil.isHomePage(new URL("http://mydomain.com//")));
        assertFalse(UrlUtil.isHomePage(new URL("http://mydomain.com/bla")));
        assertFalse(UrlUtil.isHomePage(new URL("http://mydomain.com/?bla=1")));
        assertFalse(UrlUtil.isHomePage(new URL("http://mydomain.com/bla#ref1")));
        
        assertFalse(UrlUtil.isHomePage(new URL("http://subdomain.mydomain.com\\img\\someimage.jpg/")));
        assertFalse(UrlUtil.isHomePage(new URL("http://subdomain.mydomain.com\\img\\someimage.jpg")));
    }
}
