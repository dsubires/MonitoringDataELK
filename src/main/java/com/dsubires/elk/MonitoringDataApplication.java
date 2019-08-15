package com.dsubires.elk;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
/**
 * main class of the MonitoringDataApplication application
 * 
 * 
 * @author David Subires
 *
 */
public class MonitoringDataApplication {

	public static void main(String[] args) {
		SpringApplication.run(MonitoringDataApplication.class, args);
	}

}
