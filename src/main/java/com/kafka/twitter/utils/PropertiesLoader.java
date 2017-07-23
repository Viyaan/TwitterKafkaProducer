package com.kafka.twitter.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author Viyaan
 *
 */
public class PropertiesLoader {

	   Properties prop;

		public PropertiesLoader() throws Exception{
			prop = new Properties();
	        InputStream is = PropertiesLoader.class.getClassLoader().getResourceAsStream("kafka-twitter.properties");
	        prop.load(is);
		}

		public String getString(String key){
			return prop.getProperty(key);
		}


}
