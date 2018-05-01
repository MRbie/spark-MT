package com.bie.dao;

import com.bie.bean.Task;

/**
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * 任务管理Dao的接口
 */
public interface ITaskDao {

	/**
	 * 根据主键查询任务
	 * @param teskId
	 * @return
	 */
	Task findById(long teskId);
	
	/**
	 * 执行插入的方法
	 * @param params
	 * @return
	 */
	int insertTask(Object[] params);
	
	
}
