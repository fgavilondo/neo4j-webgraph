package org.neo4japps.webgraph.customhandlers;

import org.neo4japps.webgraph.util.SimpleHttpClient;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * This class is thread-safe.
 */
public class FacebookClient extends SocialMediaClientBase {

	public FacebookClient(SimpleHttpClient httpClient) {
		super(httpClient);
	}

	@Override
	protected String getSocialApiUrl() {
		return "http://api.ak.facebook.com/restserver.php?v=1.0&method=links.getStats&format=json&urls=";
	}

	@Override
	protected String cleanUp(String url) {
		// Facebook's API rejects URLs with apostrophes in them, e.g.
		// http://my.domain.com/name/o'connor
		// even when they are url-encoded, like this:
		// http://my.domain.com/name/o%27connor
		// What Facebook expects is an escaped apostrophe, like this
		// http://my.domain.com/name/o\'connor
		// But unfortunately it is impossible to create an URI from such a
		// string in Java because it violates RFC 2396
		// return url.replace("'", "\\'");
		// So we'll just have to let such URLs fail
		return url;
	}

	@Override
	protected SocialJsonResponse deserialize(String jsonString, String url)
			throws ServiceUnavailableException {
		return doDeserialize(jsonString, url);
	}

	/**
	 * Package private visibility for unit testing
	 */
	FacebookJsonResponse doDeserialize(String jsonString, String url)
			throws ServiceUnavailableException {
		// Sample Facebook JSON (note that an array is returned!):
		// [{"url":"http://mydomain.com","normalized_url":"http://mydomain.com/","share_count":4356,"like_count":1648,"comment_count":2622,"total_count":8626,"click_count":453,"comments_fbid":448586548274,"commentsbox_count":1}]

		if (jsonString != null
				&& jsonString.contains("Service temporarily unavailable")) {
			// Sample error JSON:
			// {"error_code":2,"error_msg":"Service temporarily unavailable","request_args":[{"key":"v","value":"1.0"},{"key":"method","value":"links.getStats"},{"key":"format","value":"json"},{"key":"urls","value":"http:\/\/mydomain.com\/somepage.jsp?key=1234"},{"key":"list","value":"ob"},{"key":"lt","value":"site"},{"key":"ut","value":"2"}]}
			throw new ServiceUnavailableException(
					"Service temporarily unavailable when retrieving URL "
							+ url);
		}

		FacebookJsonResponse res = null;

		try {
			if (jsonString == null || jsonString.trim().equals("")) {
				throw new JsonSyntaxException("Can't parse empty JSON string");
			}

			FacebookJsonResponse[] deserializedArray = new Gson().fromJson(
					jsonString, FacebookJsonResponse[].class);
			res = deserializedArray[0];
		} catch (JsonSyntaxException e) {
			if (url != null) {
				// we use null URLs in unit test to avoid logging all the
				// exceptions
				logger.warn("Can't parse JSON (" + jsonString
						+ ") returned by " + url, e);
			}

			res = new FacebookJsonResponse();
		}

		return res;
	}
}
