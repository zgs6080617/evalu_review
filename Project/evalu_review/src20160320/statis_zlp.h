/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : 指令票指标
 * History
 * ===============================================
 * 2015-11-12  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_ZLP_H_
#define _STATIS_ZLP_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisZLP
{
public:
	StatisZLP();
	~StatisZLP();
	//获取DMS发布且GPMS发布指令票数量
	int GetDMSZLPnum(int statis_type,ES_TIME *es_time);
	//获取GPMS发布指令票数量
	int GetGPMSZLPnum(int statis_type,ES_TIME *es_time);
	//计算指令票指标值
	int SetZLPRate();
	//插入指令票数据到数据库
	int InsertZLPRate(int statis_type, ES_TIME *es_time);
	//指令票计算
	int GetZLPRate(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_dmszlp_num;//DMS指令票
	map<int,float> map_gpmszlp_num;//GPMS指令票
	
	//九地市指令票得分
	map<int,float> map_zlp_mark;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif