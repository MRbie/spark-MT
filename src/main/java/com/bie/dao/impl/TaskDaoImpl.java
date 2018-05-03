package com.bie.dao.impl;

import java.sql.ResultSet;

import com.bie.bean.Task;
import com.bie.dao.ITaskDao;
import com.bie.utils.JdbcHelper;

/**
 * 任务管理的实现类
 *
 * @author 别先生
 * @date 2018年5月1日 
 *
 */
public class TaskDaoImpl implements ITaskDao{

	/*@Override
	public Task findById(long teskId) {
		//创建Task对象
		final Task task = new Task();
		
		//获取到数据库连接
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		//sql语句
		String sql = "select * from task where task_id = ? ";
		//参数
		Object[] params = new Object[]{teskId};
		
		// 设计一个内部接口QueryCallback
		// 那么在执行查询语句的时候，我们就可以封装和指定自己的查询结果的处理逻辑
		// 封装在一个内部接口的匿名内部类对象中，传入JDBCHelper的方法
		// 在方法内部，可以回调我们定义的逻辑，处理查询结果
		// 并将查询结果，放入外部的变量中
		//执行查询操作
		jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
			//静态内部类(接口)，继承了这个接口，实现接口内的所有方法
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					task.setTaskid(taskid);
					task.setTaskName(taskName); 
					task.setCreateTime(createTime); 
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);  
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam);  
				}
			}
		});
		
		//将查询结果进行返回操作
		return task;
	}

	@Override
	public int insertTask(Object[] params) {
		//获取到数据库的连接
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		//插入sql语句
		String sql = "insert into "
				+ "task(task_name,create_time,start_time,finish_time,task_type,task_status,task_param) "
				+ "values(?,?,?,?,?,?,?)";
		//执行插入操作
		int flag = jdbcHelper.executeUpdate(sql, params);
		if(flag > 0){
			//System.out.println("插入成功");
			//返回执行的结果flag
			return flag;
		}
		return 0;
	}*/
	
	/**
	 * 根据主键查询任务
	 * @param taskid 主键
	 * @return 任务
	 */
	public Task findById(long taskid) {
		final Task task = new Task();
		
		String sql = "select * from task where task_id=?";
		Object[] params = new Object[]{taskid};
		
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		jdbcHelper.executeQuery(sql, params, new JdbcHelper.QueryCallback() {
			
			@Override
			public void process(ResultSet rs) throws Exception {
				if(rs.next()) {
					task.setTaskid(rs.getLong(1));
					task.setTaskName(rs.getString(2)); 
					task.setCreateTime(rs.getString(3)); 
					task.setStartTime(rs.getString(4));
					task.setFinishTime(rs.getString(5));
					task.setTaskType(rs.getString(6));  
					task.setTaskStatus(rs.getString(7));
					task.setTaskParam(rs.getString(8));
					
					/*long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					task.setTaskid(taskid);
					task.setTaskName(taskName); 
					task.setCreateTime(createTime); 
					task.setStartTime(startTime);
					task.setFinishTime(finishTime);
					task.setTaskType(taskType);  
					task.setTaskStatus(taskStatus);
					task.setTaskParam(taskParam);*/  
				}
			}
			
		});
		
		/**
		 * 说在后面的话：
		 * 
		 * 大家看到这个代码，包括后面的其他的DAO，就会发现，用JDBC进行数据库操作，最大的问题就是麻烦
		 * 你为了查询某些数据，需要自己编写大量的Domain对象的封装，数据的获取，数据的设置
		 * 造成大量很冗余的代码
		 * 
		 * 所以说，之前就是说，不建议用Scala来开发大型复杂的Spark的工程项目
		 * 因为大型复杂的工程项目，必定是要涉及很多第三方的东西的，MySQL只是最基础的，要进行数据库操作
		 * 可能还会有其他的redis、zookeeper等等
		 * 
		 * 如果你就用Scala，那么势必会造成与调用第三方组件的代码用java，那么就会变成scala+java混编
		 * 大大降低我们的开发和维护的效率
		 * 
		 * 此外，即使，你是用了scala+java混编
		 * 但是，真正最方便的，还是使用一些j2ee的开源框架，来进行第三方
		 * 技术的整合和操作，比如MySQL，那么可以用MyBatis/Hibernate，大大减少我们的冗余的代码
		 * 大大提升我们的开发速度和效率
		 * 
		 * 但是如果用了scala，那么用j2ee开源框架，进来，造成scala+java+j2ee开源框架混编
		 * 简直会造成你的spark工程的代码上的极度混乱和惨不忍睹
		 * 后期非常难以维护和交接
		 * 
		 */
		
		return task;
	}

	
}
