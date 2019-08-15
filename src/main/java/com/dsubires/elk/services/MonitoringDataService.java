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
	private ArrayList<DeviceStatus> warningHistory;
	private Logger logger = LogManager.getLogger("MonitoringDataService");
	@Value("${elastic.host}")
	private String elasticHost;
	@Value("${elastic.port}")
	private Integer elasticPort;
	@Value("${elastic.index}")
	private String elasticIndex;
	@Value("${pushbullet.url}")
	private String pushbulletUrl;
	@Value("${pushbullet.token}")
	private String pushbulletToken;
	@Value("${alert.history.filename}")
	private String alertHistoryFilename;
	@Value("${devices.temperature.threshold}")
	private Integer devicesTemperatureThreshold;

	@Scheduled(fixedDelay = 15000)
	public void scheduledWork() {

		logger.info("MonitoringDataDaemon starting at {}", new Date());

		ArrayList<DeviceStatus> warning = new ArrayList<DeviceStatus>();
		File tempFile = new File("elk.ser");
		if (tempFile.exists()) {
			warningHistory = deserializeData();
		} else {
			warningHistory = new ArrayList<DeviceStatus>();
		}

		try {
			RestClient restClient = RestClient.builder(new HttpHost(elasticHost, elasticPort, "http")).build();
			Request request = new Request("GET", "/" + elasticIndex + "/_search?pretty");
			String jsonInput = "{\r\n" + "	\"from\" : 0, \"size\" : 10000,\r\n" + "    \"query\": {\r\n"
					+ "        \"match_all\": {}\r\n" + "    }\r\n" + "}";
			request.setJsonEntity(jsonInput);
			Response response = restClient.performRequest(request);
			String json = EntityUtils.toString(response.getEntity());

			JSONObject jsonObject = (JSONObject) new JSONObject(json).get("hits");
			JSONArray data = jsonObject.getJSONArray("hits");
			Iterator<Object> iterator = data.iterator();

			// process data
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
					warning.add(deviceStatus);
				}
			}

			// process warnings
			for (DeviceStatus deviceStatus : warning) {
				if (!warningHistory.contains(deviceStatus)) {
					sendNotification(deviceStatus);
					warningHistory.add(deviceStatus);
					logger.info("processing warning...\n" + deviceStatus.toString());
				}
			}

			serializeData(warningHistory);
			restClient.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private void serializeData(ArrayList<DeviceStatus> dataToSerialize) {
		try {
			FileOutputStream fileOut = new FileOutputStream(alertHistoryFilename);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(dataToSerialize);
			out.close();
			fileOut.close();
		} catch (IOException i) {
			i.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	private ArrayList<DeviceStatus> deserializeData() {

		ArrayList<DeviceStatus> dataToDeserialize = null;
		try {
			FileInputStream fileIn = new FileInputStream(alertHistoryFilename);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			dataToDeserialize = (ArrayList<DeviceStatus>) in.readObject();
			in.close();
			fileIn.close();
		} catch (IOException i) {
			i.printStackTrace();
		} catch (ClassNotFoundException c) {
			c.printStackTrace();
		}
		return dataToDeserialize;
	}

	private void sendNotification(DeviceStatus deviceStatus) {

		String json = "{\n" + "      \"type\": \"note\",\n" + "      \"title\": \"Alerta temperatura\",\n"
				+ "      \"body\" : \"Temperatura superior a " + devicesTemperatureThreshold
				+ "grados en el siguiente dispositivo: " + deviceStatus.getDevice() + " at "
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
