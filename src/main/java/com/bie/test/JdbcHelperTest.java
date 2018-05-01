package com.bie.test;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.bie.utils.JdbcHelper;


/***
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * 
 * jdbcHelper的测试类
 */
public class JdbcHelperTest {
	
	
	public static void main(String[] args) {
		//获取到jdbcHelper的单例
		JdbcHelper jdbcHelper = JdbcHelper.getInstance();
		
		//1、测试增加，删除，修改的方法
		//jdbcHelper.executeUpdate("insert into test values(?,?)", new Object[]{3, "张三"});
		//jdbcHelper.executeUpdate("update test set id = ? where id= ?", new Object[]{1 , 1008611});
		//jdbcHelper.executeUpdate("delete from test where id = ?", new Object[]{1008612});
		
		//存放查询的值
		final Map<String, Object> map = new HashMap<String, Object>();
		//2、执行查询的方法
		jdbcHelper.executeQuery("select * from test where id = ?", new Object[]{1}, 
				new JdbcHelper.QueryCallback() {
					
					@Override
					public void process(ResultSet rs) throws Exception {
						// TODO Auto-generated method stub
						if(rs.next()){
							int id = rs.getInt(1);
							String name = rs.getString(2);
							
							// 匿名内部类的使用，有一个很重要的知识点
							// 如果要访问外部类中的一些成员，比如方法内的局部变量
							// 那么，必须将局部变量，声明为final类型，才可以访问
							// 否则是访问不了的
							map.put("id", id);
							map.put("name", name);
						}
					}
		});
		
		//打印查询出的值
		System.out.println("id:" + map.get("id") + ",name:" + map.get("name"));
		
		//3、执行批量删除的方法
		List<Object[]> list = new ArrayList<Object[]>();
		list.add(new Object[]{1});
		list.add(new Object[]{2});
		list.add(new Object[]{3});
		jdbcHelper.executeBatch("delete from test where id = ? ", list);
		
		//4、批量添加的方法
		List<Object[]> paramList = new ArrayList<Object[]>();
		paramList.add(new Object[]{1,"张三"});
		paramList.add(new Object[]{2,"李四"});
		paramList.add(new Object[]{3,"王五"});
		jdbcHelper.executeBatch("insert into test values(?,?)", paramList);
		
	}
	
	
}
