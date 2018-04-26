package com.bie.utils;

import java.math.BigDecimal;

/**
 * 数字工具类
 * @author 别先生
 *
 */
public class NumberUtils {

	/**
	 * 格式化小数
	 * @param str 字符串
	 * @param scale 四舍五入的位数
	 * @return 格式化小数
	 */
	public static double formatDouble(double num, int scale) {
		//参数1,num转换为BigDecimal类型
		BigDecimal bd = new BigDecimal(num);  
		//对小数保留几位，scale指定几位，源码0 =< scale <=7   7>= scale >=0
		return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}
	
}
