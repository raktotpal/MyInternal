package com.kogentix.internal.jsonStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class ParseJSONStream {
	
//	static Properties props = new Properties();
//	static ProducerConfig config = new ProducerConfig(props);
//
//	static Producer<String, String> producer = new Producer<String, String>(config);
	

	public static void main(String[] args) throws MalformedURLException,
			IOException, JSONException {

		// props.put("metadata.broker.list", "broker1:9092");
		// props.put("request.required.acks", "1");

		String streamUrl = args[0];
		String accessToken = args[1];

		// String topicToPublish = args[2];

		URL url = new URL(streamUrl);

		HttpURLConnection connection = null;

		while (true) {
			connection = (HttpURLConnection) url.openConnection();
			connection.setReadTimeout(1000 * 60 * 60);
			// connection.setConnectTimeout(1000 * 10);

			if (accessToken != null) {
				connection.setRequestProperty("token", accessToken);
			}

			// connection.setRequestProperty("Accept-Encoding", "gzip");

			if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
				throw new IOException(connection.getResponseMessage() + " ("
						+ connection.getResponseCode() + ")");
			}

			InputStream inputStream = connection.getInputStream();

			BufferedReader streamReader = new BufferedReader(
					new InputStreamReader(inputStream, "UTF-8"));

			StringBuilder responseStrBuilder = new StringBuilder();

			String inputStr;
			while ((inputStr = streamReader.readLine()) != null)
				responseStrBuilder.append(inputStr);

			
			
			// System.out.println(responseStrBuilder.toString());
			JSONObject jsonObject = new JSONObject(
					responseStrBuilder.toString());

			System.out.println(jsonObject.toString());
		}

		// producer.close();
	}
	
	public static void main1(String[] args) throws MalformedURLException,
			IOException {

//		props.put("metadata.broker.list", "broker1:9092");
//		props.put("request.required.acks", "1");

		String streamUrl = args[0];
		String accessToken = args[1];
		
//		String topicToPublish = args[2];

		StringBuffer sb = new StringBuffer("{");

		URL url = new URL(streamUrl);

		HttpURLConnection connection = null;

		connection = (HttpURLConnection) url.openConnection();
		connection.setReadTimeout(1000 * 60 * 60);
		// connection.setConnectTimeout(1000 * 10);

		if (accessToken != null) {
			connection.setRequestProperty("token", accessToken);
		}

		// connection.setRequestProperty("Accept-Encoding", "gzip");

		if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
			throw new IOException(connection.getResponseMessage() + " ("
					+ connection.getResponseCode() + ")");
		}

		InputStream inputStream = connection.getInputStream();

		// get an instance of the json parser from the json factory
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createJsonParser(inputStream);

		// continue parsing the token till the end of input is reached
		while (!parser.isClosed()) {
			// get the token
			JsonToken token = parser.nextToken();
			// if its the last token then we are done
			// we want to look for a field that says dataset

				
			if (token != null && JsonToken.FIELD_NAME.equals(token)
					&& "events".equals(parser.getCurrentName())) {
				// we are entering the datasets now. The first token should be
				// start of array
				token = parser.nextToken();
				if (!JsonToken.START_ARRAY.equals(token)) {
					break;
				}
				// each element of the array is an album so the next token
				// should be {
				token = parser.nextToken();
				if (!JsonToken.START_OBJECT.equals(token)) {
					break;
				}
				
				//sb.append(parser.getText());

				// we are now looking for a field that says "album_title". We
				// continue looking till we find all such fields. This is
				// probably not a best way to parse this json, but this will
				// suffice for this example.
				while (true) {
					token = parser.nextToken();
					
					if (parser.getCurrentName() != null && parser.getCurrentName().equalsIgnoreCase("events") && parser.getText().equalsIgnoreCase("}")) {
						break;
					}

					if (token != null && parser.getText().equalsIgnoreCase("rid")) {

						if (sb.length() > 1)
							sb.deleteCharAt(sb.length() - 1).deleteCharAt(
									sb.length() - 1);
						
						String msgToPublish = sb.toString().replace(",}", "}").replace(",]", "]");

						System.out.println(msgToPublish);
						
						
//						KeyedMessage<String, String> data = new KeyedMessage<String, String>(
//								topicToPublish, msgToPublish);
//						producer.send(data);
						
						

						sb.setLength(1);

						System.out
								.println("***********************************************************************************");
						continue;
					}

					if (token == null) {
						break;
					}

					if (parser.getCurrentName() != null) {
						if (parser.getText().equalsIgnoreCase("{")
								|| parser.getText().equalsIgnoreCase("[")) {
							sb.append("\"" + parser.getCurrentName() + "\":"
									+ parser.getText());
							continue;
						}

						if (parser.getText().equalsIgnoreCase("}")
								|| parser.getText().equalsIgnoreCase("]")) {
							sb.append(parser.getText() + ",");
							continue;
						}
					} else {
						if (parser.getText().equalsIgnoreCase("{")
								|| parser.getText().equalsIgnoreCase("[")) {
							sb.append(parser.getText());
							continue;
						}

						if (parser.getText().equalsIgnoreCase("}")
								|| parser.getText().equalsIgnoreCase("]")) {
							sb.append(parser.getText() + ",");
							continue;
						}
					}

					if (parser.getCurrentName() == null) {
						sb.append(parser.getText() + ",");
					} else {
						if (!parser.getCurrentName().equalsIgnoreCase(
								parser.getText())) {
							sb.append("\"" + parser.getCurrentName() + "\":\""
									+ parser.getText() + "\",");
						}

					}

				}

			}

		}

//		producer.close();
	}
}
