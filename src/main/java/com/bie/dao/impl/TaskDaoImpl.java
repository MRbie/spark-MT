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

	@Override
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
				//如果有下一个
				if(rs.next()){
					long taskid = rs.getLong(1);
					String taskName = rs.getString(2);
					String createTime = rs.getString(3);
					String startTime = rs.getString(4);
					String finishTime = rs.getString(5);
					String taskType = rs.getString(6);
					String taskStatus = rs.getString(7);
					String taskParam = rs.getString(8);
					
					//静态内部类调用外部的变量，这个变量必须是final类型的
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
	}

	
}
