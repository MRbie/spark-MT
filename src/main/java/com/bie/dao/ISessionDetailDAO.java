package com.bie.dao;

import java.util.List;

import com.bie.bean.SessionDetail;

/**
 * Session明细DAO接口
 *
 * @author 别先生
 * @date 2018年5月3日 
 *
 */
public interface ISessionDetailDAO {

	/**
	 * 插入一条session明细数据
	 * @param sessionDetail 
	 */
	void insert(SessionDetail sessionDetail);
	
	/**
	 * 批量插入session明细数据
	 * @param sessionDetails
	 */
	void insertBatch(List<SessionDetail> sessionDetails);
}
