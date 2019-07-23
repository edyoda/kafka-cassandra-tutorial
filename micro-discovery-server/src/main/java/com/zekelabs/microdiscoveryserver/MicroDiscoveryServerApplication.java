package com.zekelabs.microdiscoveryserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class MicroDiscoveryServerApplication {

	public static void main(String[] args) {
		SpringApplication.run(MicroDiscoveryServerApplication.class, args);
	}

}
