package com.bie.test;

import com.bie.bean.Task;
import com.bie.dao.ITaskDao;
import com.bie.dao.factory.DaoFactory;

/**
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * task的dao层进行测试
 */
public class TaskDaoImplTest {

	public static void main(String[] args) {
		//1、测试task的插入操作
		ITaskDao taskDao = DaoFactory.getTaskDao();
		//执行插入的参数
		Object[] params = new Object[]{"张三","2018-5-1","2018-5-1","2018-5-10",
				"管理员","良好","参数一"};
		//执行插入操作
		/*int flag = taskDao.insertTask(params);
		if(flag > 0){
			System.out.println("插入成功");
		}*/
		
		
		//2、查询操作
		Task task = taskDao.findById(1);
		System.out.println(task.toString());
	}
	
}
