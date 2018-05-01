package com.bie.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

import com.bie.conf.ConfigurationManager;

/**
 * 
 *
 * @author 别先生
 * @date 2018年4月26日 
 * jdbc工具类
 * 	正式项目一般采用框架进行开发。
 * 
 */
public class JdbcHelper {

	//单例模式
	private static JdbcHelper instance = null;
	//数据库连接池
	public LinkedList<Connection> dataSource = new LinkedList<Connection>();
		
	
	//实现单例模式,懒汉式，线程安全
	public static JdbcHelper getInstance(){
		//两步检查机制
		if(instance == null){
			//线程安全
			synchronized(JdbcHelper.class){
				if(instance == null){
					instance = new JdbcHelper();
				}
			}
		}
		//返回创建的对象
		return instance;
	}
	
	//单例模式，私有的构造方法
	private JdbcHelper(){
		// 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
		// 这个，可以通过在配置文件中配置的方式，来灵活的设定
		int datasourceSize = ConfigurationManager.getInteger(
				Constants.JDBC_DATASOURCE_SIZE);
		
		// 然后创建指定数量的数据库连接，并放入数据库连接池中
		for(int i = 0; i < datasourceSize; i++) {
			//spark.local
			boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
			String url = null;
			String user = null;
			String password = null;
			
			if(local) {
				//window的mysql数据库
				url = ConfigurationManager.getProperty(Constants.JDBC_URL);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
			} else {
				//虚拟机服务器上面的mysql数据库
				url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
				user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
				password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
			}
			
			try {
				//数据库连接操作
				Connection conn = DriverManager.getConnection(url, user, password);
				dataSource.push(conn);  
			} catch (Exception e) {
				e.printStackTrace(); 
			}
		}
	}
	
	//静态代码块加载数据库驱动
	static {
		try {
			String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
			//加载数据库驱动
			Class.forName(driver);
		} catch (Exception e) {
			e.printStackTrace();  
		}
	}
	
	/**
	 * 数据库连接等待机制
	 * @return
	 */
	public synchronized Connection getConnection(){
		//判断数据库连接池是否为0
		while(dataSource.size() == 0 ){
			try {
				//连接数目使用完了，进入等待状态
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		//拿到数据库连接
		return dataSource.poll();
	}
	
	
	/***
	 * 执行增加，删除，修改的方法
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql, Object[] params) {
		int flag = 0;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			//获取到数据库连接
			conn = getConnection();
			//使用Connection对象，取消自动提交
			conn.setAutoCommit(false);  
			
			//预处理sql语句
			pstmt = conn.prepareStatement(sql);
			//对sql的参数字段进行处理,对?的占位符设置参数
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);  
				}
			}
			//执行sql语句
			flag = pstmt.executeUpdate();
			//提交sql语句
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			if(conn != null) {
				//如果资源不为null,将资源放到线程池
				dataSource.push(conn);  
			}
		}
		//返回是否执行成功的标志
		return flag;
	}
	
	
	/***
	 * 执行查询的方法
	 * @param sql 执行查询的sql语句
	 * @param params 执行查询的参数
	 * @param callback 静态内部接口作为参数
	 */
	public void executeQuery(String sql, Object[] params,
			QueryCallback callback) {
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try {
			//获取到数据库连接
			conn = getConnection();
			//预处理sql语句
			pstmt = conn.prepareStatement(sql);
			//将查询的sql的占位符进行设置参数值
			if(params != null && params.length > 0) {
				for(int i = 0; i < params.length; i++) {
					pstmt.setObject(i + 1, params[i]);   
				}
			}
			//执行查询方法
			rs = pstmt.executeQuery();
			
			//根据传递的QueryCallback类型的参数处理查询的结果
			callback.process(rs);  
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(conn != null) {
				//如果conn连接不为null,就将连接放到数据库连接池中
				dataSource.push(conn);  
			}
		}
	}
	
	/***
	 * 执行批量删除的方法
	 * @param sql
	 * @param paramsList
	 * @return 返回删除的int类型的数组
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] rtn = null;
		Connection conn = null;
		PreparedStatement pstmt = null;
		
		try {
			//获取到数据库连接
			conn = getConnection();
			
			// 第一步：使用Connection对象，取消自动提交
			conn.setAutoCommit(false);  
			//预处理sql语句
			pstmt = conn.prepareStatement(sql);
			
			// 第二步：使用PreparedStatement.addBatch()方法加入批量的SQL参数
			if(paramsList != null && paramsList.size() > 0) {
				//循环遍历参数
				for(Object[] params : paramsList) {
					//对参数进行设置占位符
					for(int i = 0; i < params.length; i++) {
						pstmt.setObject(i + 1, params[i]);  
					}
					//添加这个删除操作
					pstmt.addBatch();
				}
			}
			
			// 第三步：使用PreparedStatement.executeBatch()方法，执行批量的SQL语句
			rtn = pstmt.executeBatch();
			
			// 最后一步：使用Connection对象，提交批量的SQL语句
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();  
		} finally {
			if(conn != null) {
				//如果conn连接不为null,就将conn数据库连接放到数据库连接池
				dataSource.push(conn);  
			}
		}
		
		return rtn;
	}
	
	
	
	/**
	 * 静态内部类：查询回调接口
	 * @author Administrator
	 *
	 */
	public static interface QueryCallback {
		
		/**
		 * 处理查询结果
		 * @param rs 
		 * @throws Exception
		 */
		void process(ResultSet rs) throws Exception;
		
	}
	
	
	
	
}
