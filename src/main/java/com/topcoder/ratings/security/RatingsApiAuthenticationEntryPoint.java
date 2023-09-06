package com.topcoder.ratings.security;

import java.io.IOException;
import java.util.Date;

import org.springframework.http.MediaType;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.AuthenticationEntryPoint;

import com.topcoder.ratings.libs.JSONDataConverter;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class RatingsApiAuthenticationEntryPoint implements AuthenticationEntryPoint {

	@Override
	public void commence(HttpServletRequest request, HttpServletResponse response,
			AuthenticationException authException) throws IOException, ServletException {

		ErrorDTO errorDTO = new ErrorDTO("Unauthorized, token validation failed!", new Date());

		response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
		response.setContentType(MediaType.APPLICATION_JSON_VALUE);

		response.getOutputStream().print(JSONDataConverter.convertToString(errorDTO));
	}
}