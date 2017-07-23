package com.viyaan.property;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Viyaan
 *
 */
public class PropertiesLoader {

	private static InputStream input = null;
	static {
		String filename = "kafka-twitter.properties";
		input = PropertiesLoader.class.getClassLoader().getResourceAsStream(filename);
		if (input == null) {
			System.out.println("Sorry, unable to find " + filename);
		}
	}

	public static Properties getKafkaProperties() {
		Properties props = new Properties();
		try {
			props.load(input);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return props;
	}

}
