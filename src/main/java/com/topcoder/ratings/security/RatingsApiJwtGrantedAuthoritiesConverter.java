package com.topcoder.ratings.security;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.apache.commons.lang3.StringUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;

public class RatingsApiJwtGrantedAuthoritiesConverter implements Converter<Jwt, Collection<GrantedAuthority>> {
  private String authorityPrefix;

	private String authorityClaims[];

	public String getAuthorityPrefix() {
		return authorityPrefix;
	}

	public void setAuthorityPrefix(String authorityPrefix) {
		this.authorityPrefix = authorityPrefix;
	}

	public String[] getAuthorityClaims() {
		return authorityClaims;
	}

	public void setAuthorityClaims(String[] authorityClaims) {
		this.authorityClaims = authorityClaims;
	}

	@Override
	public Collection<GrantedAuthority> convert(Jwt source) {

		Collection<GrantedAuthority> grantedAuthority = new ArrayList<>();

		for (String authority : getAuthorities(source)) {
			grantedAuthority.add(new SimpleGrantedAuthority(this.authorityPrefix + authority));
		}

		return grantedAuthority;
	}

	private String getAuthoritiesClaimName(Jwt jwt) {

		for (String claimName : this.authorityClaims) {
			if (jwt.hasClaim(claimName)) {
				return claimName;
			}
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	private Collection<String> getAuthorities(Jwt jwt) {

		String claimName = getAuthoritiesClaimName(jwt);
		if (claimName == null) {
			return Collections.emptyList();
		}

		// collect the scopes from m2m token - they are a space separated string
		Object authorities = jwt.getClaim(claimName);
		if (authorities instanceof String && StringUtils.isNotBlank(((String) (authorities)))) {
			return Arrays.asList(((String) (authorities)).split(" "));
		}

		// collect the roles from the user token - they are a collection
		if (authorities instanceof Collection && !((Collection<String>) authorities).isEmpty()) {
			return new ArrayList<String>((Collection<String>) authorities);
		}

		return Collections.emptyList();
	} 
}
