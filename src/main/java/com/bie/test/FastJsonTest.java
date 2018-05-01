package com.bie.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
/**
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * 获取到json格式的数据
 */
public class FastJsonTest {

	public static void main(String[] args) {
		String json = 
				"[{'学生':'张三', '班级':'一班', '年级':'大一', '科目':'高数', '成绩':90},"
				+ "{'学生':'李四', '班级':'二班', '年级':'大一', '科目':'高数', '成绩':80}]";
		JSONArray jsonArray = JSONArray.parseArray(json);
		//JSONObject jsonObject = jsonArray.getJSONObject(0);
		for(int i =0; i< jsonArray.size(); i++){
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			System.out.println("学生 :" + jsonObject.getString("学生") 
					+ ",班级:" + jsonObject.getString("班级")
					+ ",年级:" + jsonObject.getString("年级")
					+ ",科目:" + jsonObject.getString("科目")
					+ ",成绩:" + jsonObject.getString("成绩")); 
		}
		
	}
	
	
}
