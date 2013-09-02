package org.neo4japps.webgraph.customhandlers;

import junit.framework.TestCase;

public class FacebookClientTest extends TestCase {

	public void testCorrectUrlCleanup() {
		FacebookClient client = new FacebookClient(null);
		String cleanUrl = client.cleanUp("http://mydomain.com/o'connor");
		assertEquals("http://mydomain.com/o'connor", cleanUrl);
	}

	public void testCorrectJsonDeserialization()
			throws ServiceUnavailableException {
		FacebookClient client = new FacebookClient(null);

		FacebookJsonResponse json = client.doDeserialize(null, null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("nonsense", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("{}", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("[{}]", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("{nonsense}", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("[{nonsense}]", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("{nonsense: 1}", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("[{nonsense: 1}]", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("{total_count: 1}", null);
		assertFalse(json.isInitialized());

		json = client.doDeserialize("[{total_count: 1}]", null);
		assertEquals(1, json.total_count);

		String validJsonString = "[{\"url\":\"http://mydomain.com\",\"normalized_url\":\"http://mydomain.com/\",\"share_count\":4356,\"like_count\":1648,\"comment_count\":2622,\"total_count\":8626,\"click_count\":453,\"comments_fbid\":448586548274,\"commentsbox_count\":1}]";
		json = client.doDeserialize(validJsonString, null);
		assertEquals(8626, json.total_count);

		try {
			String errorJsonString = "{\"error_code\":2,\"error_msg\":\"Service temporarily unavailable\",\"request_args\":[{\"key\":\"v\",\"value\":\"1.0\"},{\"key\":\"method\",\"value\":\"links.getStats\"},{\"key\":\"format\",\"value\":\"json\"},{\"key\":\"urls\",\"value\":\"http:\\/\\/subdomain.mydomain.com\\/station.jsp?lc=94137\"},{\"key\":\"list\",\"value\":\"ob\"},{\"key\":\"lt\",\"value\":\"site\"},{\"key\":\"ut\",\"value\":\"2\"}]}";
			json = client.doDeserialize(errorJsonString, null);
			fail("Expected ServiceUnavailableException");
		} catch (ServiceUnavailableException expected) {
		}
	}
}
