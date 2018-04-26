package com.bie.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 
 *
 * @author 别先生
 * @date 2018年4月26日 
 * 配置管理项
 */
public class ConfigurationManagement {

	//目的是通过key获取value
	private static Properties prop = new Properties();
	
	//静态代码块，可以研究一下JVM虚拟机
	static{
		try {
			//加载my.properties文件
			InputStream is = ConfigurationManagement.class
				.getClassLoader().getResourceAsStream("my.properties");
			//加载到Properties对象中，可以通过key获取到value值
			prop.load(is);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 通过key值获取到value值
	 * @param key
	 * @return
	 */
	public static String getProperties(String key){
		return prop.getProperty(key);
	}
	
	/**
	 * 获取整数类型的配置项
	 * @param key
	 * @return
	 */
	public static Integer getInteger(String key){
		String value = prop.getProperty(key);
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	
	/***
	 * 获取到布尔类型的配置项
	 * @param key
	 * @return
	 */
	public static Boolean getBoolean(String key){
		String value = prop.getProperty(key);
		try {
			return Boolean.valueOf(value);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return false;
	}
	
	
	/**
	 * 获取到Long类型的配置项
	 * @param key
	 * @return
	 */
	public static Long getLong(String key){
		String value = prop.getProperty(key);
		try {
			return Long.valueOf(value);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		return 0L;
	}
	
	
	public static void main(String[] args) {
		//测试是否可以通过配置项来获取到配置的值
		String driver = ConfigurationManagement.getProperties("jdbc.driver");
		String size = ConfigurationManagement.getProperties("jdbc.datasource.size");
		String url = ConfigurationManagement.getProperties("jdbc.url");
		String user = ConfigurationManagement.getProperties("jdbc.user");
		String password = ConfigurationManagement.getProperties("jdbc.password");
		
		Boolean local = ConfigurationManagement.getBoolean("spark.local");
		Integer session = ConfigurationManagement.getInteger("spark.local.taskid.session");
		Integer page = ConfigurationManagement.getInteger("spark.local.taskid.page");
		
		
		System.out.println("driver:" + driver + ",\nsize=" + size + ",\nurl=" + url
				+ ",\nuser=" + user + ",\npassword=" + password);
		
		System.out.println("local:" + local + ",\nsession=" + session + ",\npage=" + page);
	}
	
	
}
