/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_support.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 辅助统计头文件
 * History
 * ===============================================
 * 2015-01-28  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_SUPPORT_H_
#define _STATIS_SUPPORT_H_
#include <iostream>
#include <map>
#include "dcisg.h"
#include "es_define.h"

class StatisSupport
{
public:
	StatisSupport();
	~StatisSupport();

	//整改率明细统计
	int GetCorrective(ES_TIME *es_time, int _type);

	/// @brief 两周整改率指标计算
	int GetCorrectRate(int statis_type, ES_TIME *es_time);
	/// @brief 两周整改率数据入库
	int InsertData(int statis_type, ES_TIME *es_time);
	/// @brief 清空
	void Clear();
	/// @brief 获取两周整改率日数据
	int GetDMSCorrectiveByDay(ES_TIME *es_time);
	/// @brief 计算两周整改率日指标
	int CaculateCorrectRateForDay(int statis_type, ES_TIME *es_time);
	/// @brief 计算两周整改率月指标
	int CaculateCorrectRateForMonth(int statis_type, ES_TIME *es_time);

private:
	CDci *m_oci;
	ErrorInfo err_info;

public:
	std::map<int, ES_Corrective> m_organ_corrective;
	std::multimap<int, ES_RESULT> m_corrective_result;
};

#endif
