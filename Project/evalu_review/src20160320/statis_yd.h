/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : 异动指标
 * History
 * ===============================================
 * 2015-11-12  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_YD_H_
#define _STATIS_YD_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisYD
{
public:
	StatisYD();
	~StatisYD();
	//获取DMS发布且GPMS发布异动单数量
	int GetGDMSYDDnum(int statis_type,ES_TIME *es_time);
	//获取GPMS发布异动单数量
	int GetGPMSYDDnum(int statis_type,ES_TIME *es_time);
	//计算异动单指标值
	int SetYDDRate();
	//插入异动单数据到数据库
	int InsertYDDRate(int statis_type, ES_TIME *es_time);
	//异动单计算
	int GetYDDRate(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_gdmsydd_num;//GDMS异动单
	map<int,float> map_gpmsydd_num;//GPMS异动单
	
	//九地市异动单得分
	map<int,float> map_ydd_mark;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif