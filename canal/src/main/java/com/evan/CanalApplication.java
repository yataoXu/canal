package com.evan;

import com.evan.annotation.EnableCanalClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableCanalClient
public class  CanalApplication {

	public static void main(String[] args) {
		SpringApplication.run(CanalApplication.class, args);
	}

}
