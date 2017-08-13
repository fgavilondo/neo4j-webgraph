package org.neo4japps.webgraph.customhandlers;

import junit.framework.TestCase;

public class TwitterClientTest extends TestCase {

    public void testCorrectJsonDeserialization() throws ServiceUnavailableException {
        TwitterClient client = new TwitterClient(null);

        TwitterJsonResponse json = client.doDeserialize(null, null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("", null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("nonsense", null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("{}", null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("{nonsense}", null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("{nonsense: 1}", null);
        assertFalse(json.isInitialized());

        json = client.doDeserialize("{count: 1}", null);
        assertEquals(1, json.count);

        json = client.doDeserialize("{\"count\":3115,\"url\":\"http://mydomain.com/\"}", null);
        assertEquals(3115, json.count);

        json = client.doDeserialize("{\"url\":\"http://mydomain.com/\",\"count\":3115}", null);
        assertEquals(3115, json.count);

        try {
            String errorJsonString = "{\"errors\":[{\"code\":48,\"message\":\"Unable to access URL counting services\"}]}";
            json = client.doDeserialize(errorJsonString, null);
            fail("Expected ServiceUnavailableException");
        } catch (ServiceUnavailableException expected) {
        }
    }
}
