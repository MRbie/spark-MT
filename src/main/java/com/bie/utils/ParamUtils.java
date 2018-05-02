package com.bie.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bie.conf.ConfigurationManager;

/**
 * 参数工具类
 * @author 别先生
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		//获取到值spark.local并且判断是否存在此值
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		//如果值为true
		if(local) {
			//任务类型，根据传递进来的taskType类型进行获取值返回
			return ConfigurationManager.getLong(taskType);  
		} else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		//将值field转为json格式字符串，提取某一个字段。
		JSONArray jsonArray = jsonObject.getJSONArray(field);
		if(jsonArray != null && jsonArray.size() > 0) {
			//如果不为空且大于0，那么返回这个json格式的字符串
			return jsonArray.getString(0);
		}
		//否则返回null
		return null;
	}
	
}
