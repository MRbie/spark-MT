package com.bie.spark.session;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSQLParser;
import org.apache.spark.sql.hive.HiveContext;

import com.bie.conf.ConfigurationManager;
import com.bie.dao.ITaskDao;
import com.bie.dao.factory.DaoFactory;
import com.bie.test.MockData;
import com.bie.utils.Constants;
import com.bie.utils.SparkUtils;

/***
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * 1、用户访问session分析
 * 
 */
public class UserVisitSessionAnalyzeSpark {

	/**
	 * 获取SQLContext
	 * 如果是在本地测试环境的话，那么就生成SQLContext对象
	 * 如果是在生产环境运行的话，那么就生成HiveContext对象
	 * @param sc SparkContext
	 * @return
	 */
	@SuppressWarnings("unused")
	private static SQLContext getSQLContext(SparkContext sc){
		//spark.local=true,本地模式默认为true。
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		//如果是本地模式就创建一个SQLContext对象。
		if(local){
			return new SQLContext(sc);
		}else{
			//如果不是本地模式，就创建一个HiveContext对象。
			return new HiveContext(sc);
		}
	}
	
	
	/***
	 * 生成模拟数据（只有本地模式，才会去生成模拟数据）
	 * 生成模拟数据
	 * @param sc
	 * @param sqlContext
	 */
	@SuppressWarnings("unused")
	private static void mockData(JavaSparkContext sc ,SQLContext sqlContext ){
		//是否是本地模式
		Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		if(local){
			//如果是本地模式，就调用测试数据进行测试
			MockData.mock(sc, sqlContext);
		}
	}
	
	public static void main(String[] args) {
		//构建spark的上下文
		SparkConf sparkConf = new SparkConf();	
		//设置SparkName的值UserVisitSessionAnalyzeSpark
		sparkConf.setAppName(Constants.SPARK_APP_NAME_SESSION);
		//设置本地模式
		sparkConf.setMaster("local");
		
		//获取到sc,将配置信息传递给JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		//获取到是本地模式还是非本地模式
		SQLContext sqlContext = getSQLContext(sc.sc());
		
		//生成模拟测试数据
		SparkUtils.mockData(sc, sqlContext);
	
		//创建需要使用的dao组件
		//ITaskDao taskDao = DaoFactory.getTaskDao();
		
		
		//关闭SparkContext
		sc.close();
	}
	
	
}
