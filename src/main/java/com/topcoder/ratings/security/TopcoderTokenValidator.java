package com.topcoder.ratings.security;

import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.Jwt;

public class TopcoderTokenValidator implements OAuth2TokenValidator<Jwt> {

	private final Map<String, Map<String, String>> tokenPropertiesToValidate;

	private final String issuer;

	public TopcoderTokenValidator(Map<String, Map<String, String>> tokenPropertiesToValidate, String issuer) {
		super();
		this.tokenPropertiesToValidate = tokenPropertiesToValidate;
		this.issuer = issuer;
	}

	@Override
	public OAuth2TokenValidatorResult validate(Jwt token) {
		for (String key : tokenPropertiesToValidate.get(issuer).keySet()) {
			String claimValue = token.getClaimAsString(key);
			if (StringUtils.isBlank(claimValue) || !claimValue.contains(tokenPropertiesToValidate.get(issuer).get(key))) {
        return OAuth2TokenValidatorResult.failure(new OAuth2Error("401"));
      } 
		}

		return OAuth2TokenValidatorResult.success();
	}
}