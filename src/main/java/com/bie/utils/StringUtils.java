package com.bie.utils;

/**
 * 字符串工具类
 * @author 别先生
 *
 */
public class StringUtils {

	/**
	 * 判断字符串是否为空
	 * @param str 字符串
	 * @return 是否为空
	 */
	public static boolean isEmpty(String str) {
		//如果字符串为null或者字符串为""，那么返回true
		return str == null || "".equals(str);
	}
	
	/**
	 * 判断字符串是否不为空
	 * @param str 字符串
	 * @return 是否不为空
	 */
	public static boolean isNotEmpty(String str) {
		//如果字符串不为null且字符串不为""，那么返回true
		return str != null && !"".equals(str);
	}
	
	/**
	 * 截断字符串两侧的逗号
	 * @param str 字符串
	 * @return 字符串
	 */
	public static String trimComma(String str) {
		//如果字符串开始为,那么将正数第二个赋值
		if(str.startsWith(",")) {
			str = str.substring(1);
		}
		//如果字符串结尾为,那么将倒数第二个赋值
		if(str.endsWith(",")) {
			str = str.substring(0, str.length() - 1);
		}
		return str;
	}
	
	/**
	 * 补全两位数字
	 * @param str
	 * @return
	 */
	public static String fulfuill(String str) {
		if(str.length() == 2) {
			return str;
		} else {
			//如果不是两位，补一个0，这个方法可能存在一些小问题
			return "0" + str;
		}
	}
	
	/**
	 * 从拼接的字符串中提取字段
	 * @param str 字符串
	 * @param delimiter 分隔符 
	 * @param field 字段
	 * @return 字段值
	 */
	public static String getFieldFromConcatString(String str, 
			String delimiter, String field) {
		//异常处理
		try {
			//参数delimiter作为分隔符
			String[] fields = str.split(delimiter);
			//遍历字段fields
			for(String concatField : fields) {
				// searchKeywords=|clickCategoryIds=1,2,3
				if(concatField.split("=").length == 2) {
					String fieldName = concatField.split("=")[0];
					String fieldValue = concatField.split("=")[1];
					if(fieldName.equals(field)) {
						return fieldValue;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 从拼接的字符串中给字段设置值
	 * @param str 字符串
	 * @param delimiter 分隔符 
	 * @param field 字段名
	 * @param newFieldValue 新的field值
	 * @return 字段值
	 */
	public static String setFieldInConcatString(String str, 
			String delimiter, String field, String newFieldValue) {
		//参数delimiter作为分隔符,进行分割操作
		String[] fields = str.split(delimiter);
		
		//对分割好的字符数组进行操作
		for(int i = 0; i < fields.length; i++) {
			String fieldName = fields[i].split("=")[0];
			if(fieldName.equals(field)) {
				String concatField = fieldName + "=" + newFieldValue;
				fields[i] = concatField;
				break;
			}
		}
		
		//遍历字符数组，然后进行|拼接操作
		StringBuffer buffer = new StringBuffer("");
		for(int i = 0; i < fields.length; i++) {
			buffer.append(fields[i]);
			if(i < fields.length - 1) {
				buffer.append("|");  
			}
		}
		
		return buffer.toString();
	}
	
}
