package com.bie.dao.factory;

import com.bie.dao.ITaskDao;
import com.bie.dao.impl.TaskDaoImpl;

/**
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * dao管理的工厂模式使用
 * 
 */
public class DaoFactory {

	/**
	 * 使用工厂模式直接调用这个方法使用即可。任务管理的dao层
	 * @return
	 */
	public static ITaskDao getTaskDao(){
		return new TaskDaoImpl();
	}
	
	
	
}
