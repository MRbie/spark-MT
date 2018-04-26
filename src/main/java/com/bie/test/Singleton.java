package com.bie.test;

/***
 * 
 *
 * @author 别先生
 * @date 2018年4月26日 
 * 单例模式
 */
public class Singleton {

	//创建对象
	public static Singleton instance = null;
	
	//私有构造方法
	private Singleton(){}
	
	public static Singleton getInstance(){
		//二步验证法
		if(null == instance){
			synchronized(Singleton.class){
				if(null == instance){
					instance = new Singleton();
				}
			}
		}
		return instance;
	}
	
	
}
