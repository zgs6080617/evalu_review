/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dms.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 主站运行率统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_DMS_H_
#define _STATIS_DMS_H_
#include <iostream>
#include <vector>
#include "dcisg.h"
#include "es_define.h"
#include "oci_db.h"

class StatisDMS
{
public:
	StatisDMS(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisDMS();

	int GetDMSRunningRate(int statis_type, ES_TIME *es_time);

	//int GetWeight();

	//std::map<string, float> m_code_weight;
	
private:
	int InsertResult(int statis_type, int year, int mon, int day);

private:
	CDci *m_oci;
	ErrorInfo err_info;
	std::map<int, float> m_co_result;
	//std::map<int, float> m_co_score;
	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
