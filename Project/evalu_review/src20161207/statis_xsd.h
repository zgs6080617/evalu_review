/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : 小水电指标
 * History
 * ===============================================
 * 2015-12-10  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_XSD_H_
#define _STATIS_XSD_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisXSD
{
public:
	StatisXSD();
	~StatisXSD();
	//获取小水电数值
	int GetXSDsum(int statis_type,ES_TIME *es_time);
	//插入小水电数值到数据库
	int InsertXSD(int statis_type, ES_TIME *es_time);
	//获取小水电采集率数值
	int GetXSDRate(int statis_type,ES_TIME *es_time);
	//插入小水电采集率数值到数据库
	int InsertXSDRate(int statis_type, ES_TIME *es_time);
	//小水电数值计算
	int GetXSD(int statis_type, ES_TIME *es_time);

	/************************************************************************/
	/* 小水电未采集成功个数                                                   */ 
	/************************************************************************/
	int GetXSDFailedNum(int statis_type,ES_TIME *es_time);
	/************************************************************************/
	/* 插入小水电未采集成功个数                                                */ 
	/************************************************************************/
	int InsertXSDFailedNumData(int statis_type,ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_xsd_sum;//小水电数值
	map<int,float> map_xsd_rate;//小水电采集率数值
	/*
	 *	小水电未采集成功个数
	 */
	map<int,float> map_xsdfailed_num;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

