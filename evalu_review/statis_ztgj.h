/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : 开关状态估计统计
 * History
 * ===============================================
 * 2015-12-30  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_ZTGJ_H_
#define _STATIS_ZTGJ_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisZTGJ
{
public:
	StatisZTGJ();
	~StatisZTGJ();
	//获取开关状态估计数值
	int GetZTGJsum(int statis_type,ES_TIME *es_time);
	//插入开关状态估计数值到数据库
	int InsertZTGJ(int statis_type, ES_TIME *es_time);
	//开关状态估计数值计算
	int GetZTGJ(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_ztgj_sum;//开关状态估计数值
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

