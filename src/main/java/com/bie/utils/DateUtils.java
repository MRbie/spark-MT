package com.bie.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * 
 *
 * @author 别先生
 * @date 2018年4月25日 
 * 日期时间工具类
 */
public class DateUtils {

	//年月日时分秒格式,yyyy-MM-dd HH:mm:ss
	public static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	//年月日格式,yyyy-MM-dd
	public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	//年月日格式,yyyyMMdd
	public static final SimpleDateFormat DATEKEY_FORMAT = new SimpleDateFormat("yyyyMMdd");
	
	/**
	 * 判断一个时间是否在另一个时间之前
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean before(String time1, String time2) {
		try {
			//格式化第一个参数时间yyyy-MM-dd HH:mm:ss
			Date dateTime1 = TIME_FORMAT.parse(time1);
			//格式化第二个参数时间yyyy-MM-dd HH:mm:ss
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			//判断，如果第一个时间在第二个时间之前，就返回true，否则返回false
			if(dateTime1.before(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 判断一个时间是否在另一个时间之后
	 * @param time1 第一个时间
	 * @param time2 第二个时间
	 * @return 判断结果
	 */
	public static boolean after(String time1, String time2) {
		try {
			//格式化第一个参数时间yyyy-MM-dd HH:mm:ss
			Date dateTime1 = TIME_FORMAT.parse(time1);
			//格式化第二个参数时间yyyy-MM-dd HH:mm:ss
			Date dateTime2 = TIME_FORMAT.parse(time2);
			
			//判断，如果第一个时间在第二个时间之后，就返回true，否则返回false
			if(dateTime1.after(dateTime2)) {
				return true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * 计算时间差值（单位为秒）
	 * @param time1 时间1
	 * @param time2 时间2
	 * @return 差值
	 */
	public static int minus(String time1, String time2) {
		try {
			//格式化第一个参数时间yyyy-MM-dd HH:mm:ss
			Date datetime1 = TIME_FORMAT.parse(time1);
			//格式化第二个参数时间yyyy-MM-dd HH:mm:ss
			Date datetime2 = TIME_FORMAT.parse(time2);
			
			//第一个时间减去第二个时间
			long millisecond = datetime1.getTime() - datetime2.getTime();
			
			//返回计算的差值
			return Integer.valueOf(String.valueOf(millisecond / 1000));  
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	/**
	 * 获取年月日和小时
	 * @param datetime 时间（yyyy-MM-dd HH:mm:ss）
	 * @return 结果（yyyy-MM-dd_HH）
	 */
	public static String getDateHour(String datetime) {
		//根据split方法过滤第一个参数，即yyyy-MM-dd
		String date = datetime.split(" ")[0];
		//根据split方法过滤第二个参数，即HH:mm:ss
		String hourMinuteSecond = datetime.split(" ")[1];
		//根据split方法过滤第一个参数，即HH
		String hour = hourMinuteSecond.split(":")[0];
		//返回过滤的参数yyyy-MM-dd和HH
		return date + "_" + hour;
	}  
	
	/**
	 * 获取当天日期（yyyy-MM-dd）
	 * @return 当天日期
	 */
	public static String getTodayDate() {
		//获取年月日格式,yyyy-MM-dd
		return DATE_FORMAT.format(new Date());  
	}
	
	/**
	 * 获取昨天的日期（yyyy-MM-dd）
	 * @return 昨天的日期
	 */
	public static String getYesterdayDate() {
		//单例模式
		Calendar cal = Calendar.getInstance();
		//获取到今天此刻的日期
		cal.setTime(new Date());  
		//今天的日期减一操作
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		//获取到昨天的日期
		Date date = cal.getTime();
		
		//格式化昨天的日期
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期（yyyy-MM-dd）
	 * @param date Date对象
	 * @return 格式化后的日期
	 */
	public static String formatDate(Date date) {
		//获取到年月日格式,yyyy-MM-dd
		return DATE_FORMAT.format(date);
	}
	
	/**
	 * 格式化时间（yyyy-MM-dd HH:mm:ss）
	 * @param date Date对象
	 * @return 格式化后的时间
	 */
	public static String formatTime(Date date) {
		//年月日时分秒格式,yyyy-MM-dd HH:mm:ss
		return TIME_FORMAT.format(date);
	}
	
	/**
	 * 解析时间字符串
	 * @param time 时间字符串 
	 * @return Date
	 */
	public static Date parseTime(String time) {
		try {
			//解析为年月日时分秒格式,yyyy-MM-dd HH:mm:ss
			return TIME_FORMAT.parse(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static String formatDateKey(Date date) {
		//年月日格式,yyyyMMdd
		return DATEKEY_FORMAT.format(date);
	}
	
	/**
	 * 格式化日期key
	 * @param date
	 * @return
	 */
	public static Date parseDateKey(String datekey) {
		try {
			//年月日格式,yyyyMMdd
			return DATEKEY_FORMAT.parse(datekey);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 格式化时间，保留到分钟级别
	 * yyyyMMddHHmm
	 * @param date
	 * @return
	 */
	public static String formatTimeMinute(Date date) {
		//时间格式保存为yyyyMMddHHmm
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmm");  
		return sdf.format(date);
	}
	
	
	
	public static void main(String[] args) {
		//单例模式
		Calendar cal = Calendar.getInstance();
		//获取到今天此刻的日期
		cal.setTime(new Date());  
		cal.add(Calendar.DAY_OF_YEAR, -1);  
		
		Date date = cal.getTime();
		
		System.out.println(DATE_FORMAT.format(date));
	}
	
	
}
