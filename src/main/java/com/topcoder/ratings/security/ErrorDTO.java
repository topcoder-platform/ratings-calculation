package com.topcoder.ratings.security;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorDTO {

	private String message;

	@JsonFormat(shape = JsonFormat.Shape.NUMBER)
	private Date timestamp;
}