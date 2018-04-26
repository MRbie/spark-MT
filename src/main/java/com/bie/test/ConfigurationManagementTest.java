package com.bie.test;

import com.bie.conf.ConfigurationManagement;
/**
 * 
 *
 * @author 别先生
 * @date 2018年4月26日 
 * 配置管理组件测试类
 */
public class ConfigurationManagementTest {

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
