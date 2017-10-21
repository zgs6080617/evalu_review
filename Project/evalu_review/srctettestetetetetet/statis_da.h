/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_da.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : DA相关统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_DA_H_
#define _STATIS_DA_H_
#include <iostream>
#include <vector>
#include "dcisg.h"
#include "es_define.h"
#include "oci_db.h"

//#define MULTI_DATT	//每日多条投退记录

class StatisDA
{
public:
	StatisDA(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisDA();

	int GetDARunningRate(int statis_type, ES_TIME *es_time);
	int GetDAProcess(ES_TIME *es_time);

private:
	int GetDAFinishTimesByDay(ES_TIME *es_time);
	int GetDAStartTimesByDay(ES_TIME *es_time);
	int GetDALinesByDay(ES_TIME *es_time);
	int GetDADisableTimeByDay(ES_TIME *es_time);
	int GetDASuccessTimesByDay(ES_TIME *es_time);
	int GetFATouyunLines(int statis_type, ES_TIME *es_time);
	int GetFALines(int statis_type, ES_TIME *es_time);

	int GetDAFinishTimesByMonth(ES_TIME *es_time);
	int GetDAStartTimesByMonth(ES_TIME *es_time);
	int GetDADisableTimeByMonth(ES_TIME *es_time);
	int GetDASuccessTimesByMonth(ES_TIME *es_time);

	int GetDARate(int statis_type, ES_TIME *es_time);

	//未应用
	int InsertResultDASuccessRate(int statis_type, ES_TIME *es_time);
	int InsertResultDAAbleRate(int statis_type, ES_TIME *es_time);
	//未应用
	int InsertResultDAAccuracyRate(int statis_type, ES_TIME *es_time);
	int InsertResultFATouyunRate(int statis_type, ES_TIME *es_time);
	int GetDASrcDaRelation();

	void Clear();

	//int GetWeight();

private:
	CDci *m_oci;
	ErrorInfo err_info;
	std::map<std::string, int> m_srcdv_organ;
	std::map<std::string, ES_DATIMES> m_dv_finish;
	std::map<int, float> m_organ_finish;
	std::map<std::string, ES_DATIMES> m_dv_start;
	std::map<int, float> m_organ_start;
	std::map<int, float> m_organ_lines;
	std::map<std::string, ES_DATIMES> m_dv_disable;
	std::map<int, float> m_organ_disable;
	std::multimap<std::string, ES_DATT> m_dv_datt;
	std::map<std::string, ES_DATIMES> m_dv_success;
	std::map<int, float> m_organ_success;
	std::map<int, float> m_organ_fatylines;
	std::map<int, float> m_organ_falines;
	std::multimap<int, ES_RESULT> m_suc_result;
	std::multimap<int, ES_RESULT> m_able_result;
	std::multimap<int, ES_RESULT> m_acc_result;
	std::map<int, float> m_ty_result;

	/*std::map<int, float> m_ty_score;
	std::map<string, float> m_code_weight;*/

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
