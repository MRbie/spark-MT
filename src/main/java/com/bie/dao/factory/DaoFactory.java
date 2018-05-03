package com.bie.dao.factory;

import com.bie.dao.ISessionAggrStatDAO;
import com.bie.dao.ISessionDetailDAO;
import com.bie.dao.ISessionRandomExtractDAO;
import com.bie.dao.ITaskDao;
import com.bie.dao.ITop10CategoryDAO;
import com.bie.dao.ITop10SessionDAO;
import com.bie.dao.impl.SessionAggrStatDAOImpl;
import com.bie.dao.impl.SessionDetailDAOImpl;
import com.bie.dao.impl.SessionRandomExtractDAOImpl;
import com.bie.dao.impl.TaskDaoImpl;
import com.bie.dao.impl.Top10CategoryDAOImpl;
import com.bie.dao.impl.Top10SessionDAOImpl;

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
	
	public static ISessionAggrStatDAO getSessionAggrStatDAO(){
		return new SessionAggrStatDAOImpl();
	}
	
	
	public static ISessionDetailDAO getSessionDetailDAO(){
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO(){
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO(){
		return new Top10SessionDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
		return new SessionRandomExtractDAOImpl();
	}
	
}
