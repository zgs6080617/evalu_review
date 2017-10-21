/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : ftp网络质量监控指标
 * History
 * ===============================================
 * 2015-10-26  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_NET_H_
#define _STATIS_NET_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisNet
{
public:
	StatisNet();
	~StatisNet();
	//获取九地市ftp服务器IP地址
	int GetConfig();
	//每隔半小时计算一次网络质量指标
	int UpdateNetCount();
	//计算网络质量指标值
	int GetNetValue();
	//插入指标值
	int InsertNetScore();
private:
	//九地市IP地址
	map<int,string> map_ipaddr;
	//九地市网络质量得分
	map<int,float> map_netscore;
	//九地市网络质量实时得分
	map<int,int> map_netquality;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

