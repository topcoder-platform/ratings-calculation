package com.topcoder.ratings.security;

import java.io.IOException;
import java.util.Date;

import org.springframework.http.MediaType;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.web.access.AccessDeniedHandler;

import com.topcoder.ratings.libs.JSONDataConverter;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

public class RatingsApiAccessDeniedHandler implements AccessDeniedHandler {
  @Override
	public void handle(HttpServletRequest request, HttpServletResponse response,
			AccessDeniedException accessDeniedException) throws IOException, ServletException {

		ErrorDTO errorDTO = new ErrorDTO("Forbidden, you are not authorised to perform this action!", new Date());

		response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		response.setContentType(MediaType.APPLICATION_JSON_VALUE);

		response.getOutputStream().print(JSONDataConverter.convertToString(errorDTO));

	}
}
