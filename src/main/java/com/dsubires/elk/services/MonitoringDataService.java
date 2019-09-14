package com.dsubires.elk.services;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.*;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.dsubires.elk.models.DeviceStatus;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

@Service
/**
 * Demon that checks the status data entered by the devices on elasticsearch and
 * sends push notification if the temperature recorded by a device exceeds a
 * certain threshold.
 * 
 * @author david subires
 *
 */
public class MonitoringDataService {

	private ArrayList<DeviceStatus> elkData = new ArrayList<DeviceStatus>();
	private Logger logger = LogManager.getLogger("MonitoringDataService");
	@Value("${elastic.host}")
	private String elasticHost;
	@Value("${elastic.port}")
	private Integer elasticPort;
	@Value("${elastic.index.store}")
	private String elasticIndex;
	@Value("${elastic.index.preprocessing}")
	private String elasticIndexPreprocessing;
	@Value("${pushbullet.url}")
	private String pushbulletUrl;
	@Value("${pushbullet.token}")
	private String pushbulletToken;
	@Value("${devices.temperature.threshold}")
	private Integer devicesTemperatureThreshold;

	@Scheduled(fixedDelay = 15000)
	public void scheduledWork() {

		logger.info("MonitoringDataDaemon starting at {}", new Date());

		try {
			// query data from data_preprocessing index
			RestClient restClient = RestClient.builder(new HttpHost(elasticHost, elasticPort, "http")).build();
			Request request = new Request("GET", "/" + elasticIndexPreprocessing + "/_search?pretty");
			String jsonInput = "{\r\n" + "	\"from\" : 0, \"size\" : 10000,\r\n" + "    \"query\": {\r\n"
					+ "        \"match_all\": {}\r\n" + "    }\r\n" + "}";
			request.setJsonEntity(jsonInput);

			Response response = restClient.performRequest(request);
			String json = EntityUtils.toString(response.getEntity());

			JSONObject jsonObject = (JSONObject) new JSONObject(json).get("hits");
			JSONArray data = jsonObject.getJSONArray("hits");
			Iterator<Object> iterator = data.iterator();

			if (data.length() != 0) {
				// delete data from data_preprocessing
				request = new Request("POST", "/" + elasticIndexPreprocessing + "/_delete_by_query");
				jsonInput = "{\r\n" + "    \"query\": {\r\n" + "        \"match_all\": {}\r\n" + "    }\r\n" + "}";
				request.setJsonEntity(jsonInput);
				response = restClient.performRequest(request);

				// debug delete_by_query response
				// System.out.println(EntityUtils.toString(response.getEntity()));

				// parse data from JSON
				while (iterator.hasNext()) {
					JSONObject document = ((JSONObject) iterator.next()).getJSONObject("_source");
					DeviceStatus deviceStatus = new DeviceStatus();
					deviceStatus.setDevice(document.getString("device"));
					deviceStatus.setTemperature(document.getInt("temperature"));
					deviceStatus.setTimestamp(document.getString("timestamp"));
					elkData.add(deviceStatus);
				}

				// check temperature values
				for (DeviceStatus deviceStatus : elkData) {
					if (deviceStatus.getTemperature() > devicesTemperatureThreshold) {
						File file = new File("skip-notification");
						if(!file.exists()) {
							sendNotification(deviceStatus);	
						}
						logger.info(
								"Warning --> Temperature: {}, Device: {}, Timestamp: {}",
								deviceStatus.getTemperature(), deviceStatus.getDevice(), deviceStatus.getTimestamp());
					}
				}

				// insert data in device_status
				request = new Request("POST", "/_bulk");
				jsonInput = "";
				for (DeviceStatus deviceStatus : elkData) {
					jsonInput += "{ \"index\" : { \"_index\" : \"" + elasticIndex + "\" }\r\n" + "{ \"device\" : \""
							+ deviceStatus.getDevice() + "\", \"timestamp\" : \"" + deviceStatus.getTimestamp()
							+ "\", \"temperature\" : \"" + deviceStatus.getTemperature() + "\" }\n";
				}
				request.setJsonEntity(jsonInput);
				response = restClient.performRequest(request);

				// debug _bulk response
				// System.out.println(EntityUtils.toString(response.getEntity()));

				logger.info("{} documents moved from data_preprocessing to device_status at {}.", data.length(), new Date());
				
			} else {
				logger.info("the data_preprocessing index is empty at {}", new Date());

			}

			restClient.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void sendNotification(DeviceStatus deviceStatus) {

		String json = "{\n" + "      \"type\": \"note\",\n" + "      \"title\": \"Alerta temperatura\",\n"
				+ "      \"body\" : \"Temperatura superior a " + devicesTemperatureThreshold
				+ " grados en el siguiente dispositivo: " + deviceStatus.getDevice() + " at "
				+ deviceStatus.getTimestamp() + "\"\n" + "}";

		@SuppressWarnings("unused")
		ClientResponse response;
		WebResource webResource;
		Client client = Client.create();
		webResource = client.resource(pushbulletUrl);
		response = webResource.header("Access-Token", pushbulletToken).type("application/json")
				.post(ClientResponse.class, json);

	}

}
