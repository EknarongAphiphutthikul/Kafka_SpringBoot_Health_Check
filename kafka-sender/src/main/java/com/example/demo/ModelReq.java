package com.example.demo;

import java.io.Serializable;
import java.util.Map;

import lombok.Data;

@Data
public class ModelReq implements Serializable  {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4477387685559305650L;
	private String topic;
	private Map<String, String> headers;
	private String msg;
	
}
