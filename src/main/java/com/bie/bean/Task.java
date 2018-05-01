package com.bie.bean;

import java.io.Serializable;

/***
 * 
 *
 * @author 别先生
 * @date 2018年5月1日 
 * 任务的实体类
 */
public class Task implements Serializable{

	/**
	 * 实现可序列化
	 */
	private static final long serialVersionUID = 1L;
	
	private long taskid;//任务编号
	private String taskName;//任务名称
	private String createTime;//创建时间
	private String startTime;//开始时间
	private String finishTime;//结束时间
	private String taskType;//任务类型
	private String taskStatus;//任务状态
	private String taskParam;//任务参数
	
	public long getTaskid() {
		return taskid;
	}
	public void setTaskid(long taskid) {
		this.taskid = taskid;
	}
	public String getTaskName() {
		return taskName;
	}
	public void setTaskName(String taskName) {
		this.taskName = taskName;
	}
	public String getCreateTime() {
		return createTime;
	}
	public void setCreateTime(String createTime) {
		this.createTime = createTime;
	}
	public String getStartTime() {
		return startTime;
	}
	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}
	public String getFinishTime() {
		return finishTime;
	}
	public void setFinishTime(String finishTime) {
		this.finishTime = finishTime;
	}
	public String getTaskType() {
		return taskType;
	}
	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}
	public String getTaskStatus() {
		return taskStatus;
	}
	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}
	public String getTaskParam() {
		return taskParam;
	}
	public void setTaskParam(String taskParam) {
		this.taskParam = taskParam;
	}
	@Override
	public String toString() {
		return "Task [taskid=" + taskid + ", taskName=" + taskName + ", createTime=" + createTime + ", startTime="
				+ startTime + ", finishTime=" + finishTime + ", taskType=" + taskType + ", taskStatus=" + taskStatus
				+ ", taskParam=" + taskParam + "]";
	}
	
	
}
