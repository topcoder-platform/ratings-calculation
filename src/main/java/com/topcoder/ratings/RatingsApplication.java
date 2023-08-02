package com.topcoder.ratings;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = "com.topcoder")
public class RatingsApplication {

	public static void main(String[] args) {
    
		SpringApplication.run(RatingsApplication.class, args);
	}
}
