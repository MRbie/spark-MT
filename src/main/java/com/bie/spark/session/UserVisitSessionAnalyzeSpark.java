package com.bie.spark.session;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.httpclient.util.DateUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;

import com.alibaba.fastjson.JSONObject;
import com.bie.bean.Task;
import com.bie.conf.ConfigurationManager;
import com.bie.dao.ITaskDao;
import com.bie.dao.factory.DaoFactory;
import com.bie.test.MockData;
import com.bie.utils.Constants;
import com.bie.utils.DateUtils;
import com.bie.utils.ParamUtils;
import com.bie.utils.SparkUtils;
import com.bie.utils.StringUtils;
import com.bie.utils.ValidUtils;

import io.netty.util.internal.StringUtil;
import scala.Tuple2;
import scala.collection.immutable.Stream.Cons;

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
	 * 1、获取SQLContext
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
	 * 2、生成模拟数据（只有本地模式，才会去生成模拟数据）
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
	
	
	/**
	 * 3、获取指定日期范围内的用户访问行为数据
	 * @param sqlContext SQLContext
	 * @param jsonObject 任务参数
	 * @return 行为数据RDD
	 */
	@SuppressWarnings("unused")
	private static JavaRDD<Row> getActionRDDByDateRange(SQLContext sqlContext,
			JSONObject taskParam){
		//获取到startDate日期
		String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
		//获取到endDate日期
		String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);
		
		//执行的sql语句
		//规律，在双引号内部，'' "" ++ 
		String sql = "select * "
				+ "from user_visit_action "
				+ "where date >= '" + startDate + "' "
				+ "and date <= '"+ endDate +"' ";
//				+ "and session_id not in('','','')" ;
		//执行sql
		DataFrame actionDF = sqlContext.sql(sql);
		
		/**
		 * 这里就很有可能发生上面说的问题
		 * 比如说，Spark SQl默认就给第一个stage设置了20个task，但是根据你的数据量以及算法的复杂度
		 * 实际上，你需要1000个task去并行执行
		 * 
		 * 所以说，在这里，就可以对Spark SQL刚刚查询出来的RDD执行repartition重分区操作
		 */
		
		//return actionDF.javaRDD().repartition(1000);
		
		return actionDF.javaRDD();
	}
	
	/**
	 * 4、获取sessionid2到访问行为数据的映射的RDD
	 * @param actionRDD
	 * @return
	 * 匿名内部类
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD){
		return actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>, String, Row>() {

			/**
			 * 序列化操作
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
				//实例化一个对象list，Tuple2<String,Row>是scala类型的
				List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
				//循环遍历
				while(iterator.hasNext()){
					//遍历出这row
					Row row = iterator.next();
					//然后对row进行添加到list中
					list.add(new Tuple2<String, Row>(row.getString(2), row));
				}
				return list;
			}
		});
	}
	
	
	/***
	 * 5、对行文数据按照session粒度进行聚合
	 * @param sc JavaSparkContext 
	 * @param sqlContext SQLContext
	 * @param sessionid2actionRDD JavaPairRDD<String, Row>
	 * @return session粒度聚合数据
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, String> aggregateBySession(
			JavaSparkContext sc, SQLContext sqlContext, 
			JavaPairRDD<String, Row> sessionid2actionRDD){
		//现在actionRDD中的元素是Row,一个Row就是一行用户访问行为记录，比如一次点击或者搜索
		//我们现在需要将这个Row映射成<sessionid,Row>的格式
		
		
		// 对行为数据按session粒度进行分组
		JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = 
				sessionid2actionRDD.groupByKey();
		
		
		// 对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
		// 到此为止，获取的数据格式，如下：
		//<userid,partAggrInfo(sessionid,searchKeywords,clickCategoryIds)>
		JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
			
			/**
			 * PairFunction
			 * 参数一，相当于函数的输入
			 * 参数2和参数3，相当于是函数的输出(tuple),分别是Tuple第一个和第二个值
			 */
			new PairFunction<Tuple2<String,Iterable<Row>>, Long, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
				//获取到sessionid
				String sessionid = tuple._1;
				//获取到一行信息
				Iterator<Row> iterator = tuple._2.iterator();
				
				StringBuffer searchKeywordsBuffer = new StringBuffer("");
				StringBuffer clickCategoryIdsBuffer = new StringBuffer("");
				
				Long userid = null;
				//session的起始和结束时间
				Date startTime = null;
				Date endTime = null;
				//session的访问步长
				int stepLength = 0;
				
				//遍历session所有的访问行为
				while(iterator.hasNext()){
					//提取每个访问行为的搜索词字段和点击品字段
					Row row = iterator.next();
					if(userid == null){
						userid = row.getLong(1);
					}
					String searchKeyword = row.getString(5);
					Long clickCategoryId = row.getLong(6);
					
					// 实际上这里要对数据说明一下
					// 并不是每一行访问行为都有searchKeyword何clickCategoryId两个字段的
					// 其实，只有搜索行为，是有searchKeyword字段的
					// 只有点击品类的行为，是有clickCategoryId字段的
					// 所以，任何一行行为数据，都不可能两个字段都有，所以数据是可能出现null值的
					
					// 我们决定是否将搜索词或点击品类id拼接到字符串中去
					// 首先要满足：不能是null值
					// 其次，之前的字符串中还没有搜索词或者点击品类id
					
					//进行判断searchKeyword是否为null和是否存在
					if(StringUtils.isNotEmpty(searchKeyword)){
						if(!searchKeywordsBuffer.toString().contains(searchKeyword)){
							searchKeywordsBuffer.append(searchKeyword);
						}
					}
					//Long类型可以判断不为null
					if(clickCategoryId != null){
						if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
							clickCategoryIdsBuffer.append(clickCategoryId + ",");
						}
					}
					
					//计算session开始和结束的时间
					Date actionTime = DateUtils.parseTime(row.getString(4));
					//startTime为null
					if(startTime == null){
						startTime = actionTime;
					}
					//endTime为null，将actionTime赋值给endTime和startTime
					if(endTime == null){
						endTime = actionTime;
					}
					
					//
					if(actionTime.before(startTime)){
						startTime = actionTime;
					}
					//
					if(actionTime.after(endTime)){
						endTime = actionTime;
					}
			
					//计算session的访问步长
					stepLength++;
				}
				
				//截断字符串两侧的逗号
				String searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString());
				String clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString());
				
				//计算session的访问时长（s）
				long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;
				
				
				// 大家思考一下
				// 我们返回的数据格式，其实<sessionid,partAggrInfo>
				// 但是，这一步聚合完了以后，其实，我们是还需要将每一行数据，跟对应的用户信息进行聚合
				// 问题就来了，如果是跟用户信息进行聚合的话，那么key，就不应该是sessionid
				// 就应该是userid，才能够跟<userid,Row>格式的用户信息进行聚合
				// 如果我们这里直接返回<sessionid,partAggrInfo>，还得再做一次mapToPair算子
				// 将RDD映射成<userid,partAggrInfo>的格式，那么就多此一举
				
				// 所以，我们这里其实可以直接，返回的数据格式，就是<userid,partAggrInfo>
				// 然后跟用户信息join的时候，将partAggrInfo关联上userInfo
				// 然后再直接将返回的Tuple的key设置成sessionid
				// 最后的数据格式，还是<sessionid,fullAggrInfo>
				
				// 聚合数据，用什么样的格式进行拼接？
				// 我们这里统一定义，使用key=value|key=value
				
				String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
						+ Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|"
						+ Constants.FIELD_CLICK_CATEGORY_IDS + "=" + visitLength + "|"
						+ Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
						+ Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)
						;
				return new Tuple2<Long, String>(userid, partAggrInfo);
			}
		});
		
		//查询所有用户的数据，并且映射成<userid,Row>的格式
		String sql = "select * from user_info ";
		JavaRDD<Row> userInfoRDD = sqlContext.sql(sql).javaRDD();
		
		//将查询的数据进行映射为<userid,row>格式的数据
		JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(
				
				new PairFunction<Row, Long, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Row> call(Row row) throws Exception {
						//获取到第一个userid的值
						return new Tuple2<Long, Row>(row.getLong(0), row);
					}
					
		});
		
		/**
		 * 这里就可以说一下，比较适合采用reduce join转换为map join的方式
		 * 
		 * userid2PartAggrInfoRDD，可能数据量还是比较大，比如，可能在1千万数据
		 * userid2InfoRDD，可能数据量还是比较小的，你的用户数量才10万用户
		 * 
		 */
		
		//将session粒度聚合数据，与用户信息进行Join
		//格式<userid,Tuple<String, Row>>
		JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = 
				userid2PartAggrInfoRDD.join(userid2InfoRDD);
		
		//对join起来的数据进行拼接，并且返回<sessionid,fullAggrInfo>格式的数据
		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2FullInfoRDD.mapToPair(
				
				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
						//重点详细关注这一块到底如何获取和获取的是什么
						//partAggrInfo的位置是第二个
						String partAggrInfo = tuple._2._1;
						Row userInfoRow = tuple._2._2;
						
						//从拼接的字符串中提取字段，获取到sessionid的值
						String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
						
						//获取到age年龄的值
						int age = userInfoRow.getInt(3);
						//获取到professional
						String professional = userInfoRow.getString(4);
						//获取到城市的值
						String city = userInfoRow.getString(5);
						//获取到sex的值
						String sex = userInfoRow.getString(6);
						
						//进行字符串的拼接操作
						String fullAggrInfo = partAggrInfo + "|"
								+ Constants.FIELD_AGE + "=" + age + "|"
								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
								+ Constants.FIELD_CITY + "=" + city + "|"
								+ Constants.FIELD_SEX + "=" + sex + "|"
								;
						
						return new Tuple2<String, String>(sessionid, fullAggrInfo);
					}
		});
		
		
		/**
		 * reduce join转换为map join
		 */
		
//		List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
//		final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);
//		
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = userid2PartAggrInfoRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						// 得到用户信息map
//						List<Tuple2<Long, Row>> userInfos = userInfosBroadcast.value();
//						
//						Map<Long, Row> userInfoMap = new HashMap<Long, Row>();
//						for(Tuple2<Long, Row> userInfo : userInfos) {
//							userInfoMap.put(userInfo._1, userInfo._2);
//						}
//						
//						// 获取到当前用户对应的信息
//						String partAggrInfo = tuple._2;
//						Row userInfoRow = userInfoMap.get(tuple._1);
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		/**
		 * sample采样倾斜key单独进行join
		 */
		
//		JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1, 9);
//		
//		JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._1, 1L);
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
//				
//				new Function2<Long, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Long call(Long v1, Long v2) throws Exception {
//						return v1 + v2;
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Long>, Long, Long>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple)
//							throws Exception {
//						return new Tuple2<Long, Long>(tuple._2, tuple._1);
//					}
//					
//				});
//		
//		final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;  
//		
//		JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(
//				
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//					
//				});
//			
//		JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(
//				
//				new Function<Tuple2<Long,String>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Boolean call(Tuple2<Long, String> tuple) throws Exception {
//						return !tuple._1.equals(skewedUserid);
//					}
//					
//				});
//		
//		JavaPairRDD<String, Row> skewedUserid2infoRDD = userid2InfoRDD.filter(
//				
//				new Function<Tuple2<Long,Row>, Boolean>() {
//
//					private static final long serialVersionUID = 1L;
//		
//					@Override
//					public Boolean call(Tuple2<Long, Row> tuple) throws Exception {
//						return tuple._1.equals(skewedUserid);
//					}
//					
//				}).flatMapToPair(new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(
//							Tuple2<Long, Row> tuple) throws Exception {
//						Random random = new Random();
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//						
//						for(int i = 0; i < 100; i++) {
//							int prefix = random.nextInt(100);
//							list.add(new Tuple2<String, Row>(prefix + "_" + tuple._1, tuple._2));
//						}
//						
//						return list;
//					}
//					
//				});
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(100);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
//					}
//					
//				}).join(skewedUserid2infoRDD).mapToPair(
//						
//						new PairFunction<Tuple2<String,Tuple2<String,Row>>, Long, Tuple2<String, Row>>() {
//
//							private static final long serialVersionUID = 1L;
//		
//							@Override
//							public Tuple2<Long, Tuple2<String, Row>> call(
//									Tuple2<String, Tuple2<String, Row>> tuple)
//									throws Exception {
//								long userid = Long.valueOf(tuple._1.split("_")[1]);  
//								return new Tuple2<Long, Tuple2<String, Row>>(userid, tuple._2);  
//							}
//							
//						});
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);
//		
//		JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);
//		
//		JavaPairRDD<String, String> sessionid2FullAggrInfoRDD = joinedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<Long, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		/**
		 * 使用随机数和扩容表进行join
		 */
		
//		JavaPairRDD<String, Row> expandedRDD = userid2InfoRDD.flatMapToPair(
//				
//				new PairFlatMapFunction<Tuple2<Long,Row>, String, Row>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Iterable<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple)
//							throws Exception {
//						List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
//						
//						for(int i = 0; i < 10; i++) {
//							list.add(new Tuple2<String, Row>(0 + "_" + tuple._1, tuple._2));
//						}
//						
//						return list;
//					}
//					
//				});
//		
//		JavaPairRDD<String, String> mappedRDD = userid2PartAggrInfoRDD.mapToPair(
//				
//				new PairFunction<Tuple2<Long,String>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(Tuple2<Long, String> tuple)
//							throws Exception {
//						Random random = new Random();
//						int prefix = random.nextInt(10);
//						return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);  
//					}
//					
//				});
//		
//		JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedRDD.join(expandedRDD);
//		
//		JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
//				
//				new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, String>() {
//
//					private static final long serialVersionUID = 1L;
//
//					@Override
//					public Tuple2<String, String> call(
//							Tuple2<String, Tuple2<String, Row>> tuple)
//							throws Exception {
//						String partAggrInfo = tuple._2._1;
//						Row userInfoRow = tuple._2._2;
//						
//						String sessionid = StringUtils.getFieldFromConcatString(
//								partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);
//						
//						int age = userInfoRow.getInt(3);
//						String professional = userInfoRow.getString(4);
//						String city = userInfoRow.getString(5);
//						String sex = userInfoRow.getString(6);
//						
//						String fullAggrInfo = partAggrInfo + "|"
//								+ Constants.FIELD_AGE + "=" + age + "|"
//								+ Constants.FIELD_PROFESSIONAL + "=" + professional + "|"
//								+ Constants.FIELD_CITY + "=" + city + "|"
//								+ Constants.FIELD_SEX + "=" + sex;
//						
//						return new Tuple2<String, String>(sessionid, fullAggrInfo);
//					}
//					
//				});
		
		
		return sessionid2FullAggrInfoRDD;
	}
	
	
	/***
	 * 6、过滤session数据，并进行聚合统计操作
	 * @param sessionid2AggrInfoRDD
	 * @param taskParam
	 * @param sessionAggrStatAccumulator
	 * @return
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, String> filterSessionAndAggrStat(
			JavaPairRDD<String, String> sessionid2AggrInfoRDD, 
			final JSONObject taskParam,
			final Accumulator<String> sessionAggrStatAccumulator){
		// 为了使用我们后面的ValieUtils，所以，首先将所有的筛选参数拼接成一个连接串
		// 此外，这里其实大家不要觉得是多此一举
		// 其实我们是给后面的性能优化埋下了一个伏笔		
		//开始年龄
		String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
		//结束年龄
		String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
		//professionals
		String professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
		//城市
		String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
		//姓别
		String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
		//搜索
		String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
		//categoryIds
		String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);
		
		//字符串拼接
		String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "")
				 + (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "")
				 + (professionals != null ? Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" : "")
					+ (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "")
					+ (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "")
					+ (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "")
					+ (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds: "")
				;
		
		//如果拼接的字符串末尾是|开头就截取扔掉
		if(_parameter.endsWith("\\|")){
			_parameter = _parameter.substring(0, _parameter.length() - 1);
		}
		
		//将拼接的字符串赋值为final类型的
		final String parameter = _parameter;
		
		//根据筛选参数进行过滤
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = 
				sessionid2AggrInfoRDD.filter(new Function<Tuple2<String,String>, Boolean>() {
					
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Boolean call(Tuple2<String, String> tuple) 
							throws Exception {
						//首先，从tuple中，获取聚合数据
						String aggrInfo = tuple._2;
						
						// 接着，依次按照筛选条件进行过滤
						// 按照年龄范围进行过滤（startAge、endAge）
						if(!ValidUtils.between(aggrInfo, Constants.FIELD_AGE,
								parameter, Constants.PARAM_START_AGE, 
								Constants.PARAM_END_AGE)){
							//如果不包含上面所述的字段，直接返回false
							return false;			
						}
						
						// 按照职业范围进行过滤（professionals）
						// 互联网,IT,软件
						// 互联网
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, 
								parameter, Constants.PARAM_PROFESSIONALS)){
							return false;
						}
						
						// 按照城市范围进行过滤（cities）
						// 北京,上海,广州,深圳
						// 成都
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter, Constants.PARAM_CITIES)){
							return false;
						}
						
						// 按照性别进行过滤
						// 男/女
						// 男，女
						if(!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter, Constants.PARAM_SEX)){
							return false;
						}
						
						
						// 按照搜索词进行过滤
						// 我们的session可能搜索了 火锅,蛋糕,烧烤
						// 我们的筛选条件可能是 火锅,串串香,iphone手机
						// 那么，in这个校验方法，主要判定session搜索的词中，有任何一个，与筛选条件中
						// 任何一个搜索词相当，即通过
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter, Constants.PARAM_KEYWORDS)){
							return false;
						}
						
						// 按照点击品类id进行过滤
						if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter, Constants.PARAM_CATEGORY_IDS)){
							return false;
						}
						
						
						// 如果经过了之前的多个过滤条件之后，程序能够走到这里
						// 那么就说明，该session是通过了用户指定的筛选条件的，也就是需要保留的session
						// 那么就要对session的访问时长和访问步长，进行统计，根据session对应的范围
						// 进行相应的累加计数
						
						// 主要走到这一步，那么就是需要计数的session,session_count
						sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
						
						// 计算出session的访问时长和访问步长的范围，并进行相应的累加
						//visitLength
						//long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
						//stepLength
						//long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));		
							
						//计算访问时长范围
						//calculateVisitLength(visitLength); 
						//计算访问步长范围
						//calculateStepLength(stepLength); 
						
						return true;
					}
					
					/**
					 * 计算访问时长范围
					 * @param visitLength
					 */
					private void calculateVisitLength(long visitLength) {
						if(visitLength >=1 && visitLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);  
						} else if(visitLength >=4 && visitLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);  
						} else if(visitLength >=7 && visitLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);  
						} else if(visitLength >=10 && visitLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);  
						} else if(visitLength > 30 && visitLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);  
						} else if(visitLength > 60 && visitLength <= 180) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);  
						} else if(visitLength > 180 && visitLength <= 600) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);  
						} else if(visitLength > 600 && visitLength <= 1800) {  
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);  
						} else if(visitLength > 1800) {
							sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);  
						} 
					}
					
					/**
					 * 计算访问步长范围
					 * @param stepLength
					 */
					private void calculateStepLength(long stepLength) {
						if(stepLength >= 1 && stepLength <= 3) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);  
						} else if(stepLength >= 4 && stepLength <= 6) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);  
						} else if(stepLength >= 7 && stepLength <= 9) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);  
						} else if(stepLength >= 10 && stepLength <= 30) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);  
						} else if(stepLength > 30 && stepLength <= 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);  
						} else if(stepLength > 60) {
							sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);    
						}
					}

					
				});
		
		
		
		return filteredSessionid2AggrInfoRDD;
	}
	
	/**
	 * 7、获取通过筛选条件的session的访问明细数据RDD
	 * @param sessionid2aggrInfoRDD
	 * @param sessionid2actionRDD
	 * @return
	 */
	@SuppressWarnings("unused")
	private static JavaPairRDD<String, Row> getSessionid2detailRDD(
			JavaPairRDD<String, String> sessionid2aggrInfoRDD,
			JavaPairRDD<String, Row> sessionid2actionRDD){
		
		JavaPairRDD<String, Row> sessionid2detailRDD = sessionid2aggrInfoRDD
				.join(sessionid2actionRDD)
				.mapToPair(new PairFunction<Tuple2<String,Tuple2<String,Row>>, String, Row>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple) 
							throws Exception {
							
						return new Tuple2<String, Row>(tuple._1, tuple._2._2);
					}
		});
		
		return sessionid2detailRDD;
	}
	
	
	
	
	
	public static void main(String[] args) {
		//设置参数,仅供测试，测试结束，注释即可
		args = new String[]{"1"};
		
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
		ITaskDao taskDao = DaoFactory.getTaskDao();
		
		//如果要进行session粒度的数据聚合，首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
		//如果要根据用户在创建任务时指定的参数，来进行数据过滤和筛选。
		//那么就首先得查询出来指定的任务
		//spark.local.taskid.session
		Long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);
		//然后根据taskid进行查询数据表操作，将查询的结果进行返回
		Task task = taskDao.findById(taskid);
		//如果task为null的时候
		if(null == task){
			System.out.println(new Date() 
					+ ": cannot find this task with id [" + taskid + "].");  
			//打印错误信息以后直接返回即可
			return;
		}
		//将查询的结果转换为json格式的数据
		JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());
		
		// 如果要进行session粒度的数据聚合
		// 首先要从user_visit_action表中，查询出来指定日期范围内的行为数据
		
		/**
		 * actionRDD，就是一个公共RDD
		 * 第一，要用ationRDD，获取到一个公共的sessionid为key的PairRDD
		 * 第二，actionRDD，用在了session聚合环节里面
		 * 
		 * sessionid为key的PairRDD，是确定了，在后面要多次使用的
		 * 1、与通过筛选的sessionid进行join，获取通过筛选的session的明细数据
		 * 2、将这个RDD，直接传入aggregateBySession方法，进行session聚合统计
		 * 
		 * 重构完以后，actionRDD，就只在最开始，使用一次，用来生成以sessionid为key的RDD
		 * 
		 */
		JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam);
		//根据actionRDD获取到sessionid2ActionRDD
		JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
		
		/**
		 * 持久化，很简单，就是对RDD调用persist()方法，并传入一个持久化级别
		 * 
		 * 如果是persist(StorageLevel.MEMORY_ONLY())，纯内存，无序列化，那么就可以用cache()方法来替代
		 * StorageLevel.MEMORY_ONLY_SER()，第二选择
		 * StorageLevel.MEMORY_AND_DISK()，第三选择
		 * StorageLevel.MEMORY_AND_DISK_SER()，第四选择
		 * StorageLevel.DISK_ONLY()，第五选择
		 * 
		 * 如果内存充足，要使用双副本高可靠机制
		 * 选择后缀带_2的策略
		 * StorageLevel.MEMORY_ONLY_2()
		 * 
		 */
		//JavaPairRDD<String, Row> persist = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		//对sessionid2ActionRDD持久化操作
		sessionid2ActionRDD = sessionid2ActionRDD.persist(StorageLevel.MEMORY_ONLY());
		//sessionid2actionRDD.checkpoint();
		
		
		// 首先，可以将行为数据，按照session_id进行groupByKey分组
		// 此时的数据的粒度就是session粒度了，然后呢，可以将session粒度的数据
		// 与用户信息数据，进行join
		// 然后就可以获取到session粒度的数据，同时呢，数据里面还包含了session对应的user的信息
		// 到这里为止，获取的数据是<sessionid,(sessionid,searchKeywords,clickCategoryIds,age,professional,city,sex)>  
		JavaPairRDD<String, String> sessionid2AggrInfoRDD = 
				aggregateBySession(sc, sqlContext, sessionid2ActionRDD);
		
		//单元测试,获取10条数据
		System.out.println("过滤前行数：" +  sessionid2AggrInfoRDD.count());
		for(Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(10)){
			System.out.println("过滤前："  + ", " + tuple._2);
		}
		
		// 接着，就要针对session粒度的聚合数据，按照使用者指定的筛选参数进行数据过滤
		// 相当于我们自己编写的算子，是要访问外面的任务参数对象的
		// 所以，大家记得我们之前说的，匿名内部类（算子函数），访问外部对象，是要给外部对象使用final修饰的
		
		// 重构，同时进行过滤和统计
		Accumulator<String> sessionAggrStatAccumulator = sc.accumulator("", new SessionAggrStatAccumulator());
		
		//进行session过滤操作
		JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = 
				filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParam, sessionAggrStatAccumulator);
		//设置级别
		filteredSessionid2AggrInfoRDD = filteredSessionid2AggrInfoRDD.persist(StorageLevel.MEMORY_ONLY());
		
		//单元测试，过滤后数据输出
		System.out.println("过滤后行数：" +  filteredSessionid2AggrInfoRDD.count());
		for(Tuple2<String, String> tuple : filteredSessionid2AggrInfoRDD.take(10)){
			System.out.println("过滤后："  + ", " + tuple._2);
		}
		
		
		// 生成公共的RDD：通过筛选条件的session的访问明细数据
		
		/**
		 * 重构：sessionid2detailRDD，就是代表了通过筛选的session对应的访问明细数据
		 */
		JavaPairRDD<String, Row> sessionid2detailRDD = 
				getSessionid2detailRDD(filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
		//设置级别，
		sessionid2detailRDD = sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
		
		
		
		/**
		 * 对于Accumulator这种分布式累加计算的变量的使用，有一个重要说明
		 * 
		 * 从Accumulator中，获取数据，插入数据库的时候，一定要，一定要，是在有某一个action操作以后
		 * 再进行。。。
		 * 
		 * 如果没有action的话，那么整个程序根本不会运行。。。
		 * 
		 * 是不是在calculateAndPersisitAggrStat方法之后，运行一个action操作，比如count、take
		 * 不对！！！
		 * 
		 * 必须把能够触发job执行的操作，放在最终写入MySQL方法之前
		 * 
		 * 计算出来的结果，在J2EE中，是怎么显示的，是用两张柱状图显示
		 */
		
		
		//关闭SparkContext
		sc.close();
	}
	
	
}
