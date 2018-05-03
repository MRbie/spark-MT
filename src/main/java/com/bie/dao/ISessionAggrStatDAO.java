package com.bie.dao;

import com.bie.bean.SessionAggrStat;

/***
 * 
 *
 * @author 别先生
 * @date 2018年5月3日 
 *
 * session聚合统计模块DAO接口
 */
public interface ISessionAggrStatDAO {

	/**
	 * 插入session聚合统计结果
	 * @param sessionAggrStat 
	 */
	void insert(SessionAggrStat sessionAggrStat);
}
