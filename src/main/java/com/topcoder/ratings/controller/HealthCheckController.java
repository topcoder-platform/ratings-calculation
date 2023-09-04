package com.topcoder.ratings.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;

@RestController
@RequestMapping("v5/ratings")
public class HealthCheckController {

	@Operation(tags = "Health-API", description = "Get application health status", operationId = "checkHealth")
	@ApiResponse(responseCode = "200")

	@GetMapping("/health")
	public ResponseEntity<Void> checkHealth() {

		return new ResponseEntity<>(HttpStatus.OK);
	}
}