/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_yc.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 用采相关统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_YC_H_
#define _STATIS_YC_H_
#include <iostream>
#include <algorithm>
#include <string>
#include "dcisg.h"
#include "es_define.h"

class StatisYC
{
public:
	StatisYC(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisYC();

	int GetYCRate(int statis_type, ES_TIME *es_time);
	int GetYCProcess(ES_TIME *es_time);

private:
	int GetPpnumByDay(ES_TIME *es_time);
	int GetTqnumByDay(ES_TIME *es_time);

	int GetYCMonth(ES_TIME *es_time);
	int GetTdIntimeNum(int statis_type, ES_TIME *es_time);
	int GetTdconfirmNum(int statis_type, ES_TIME *es_time);
	int GetTdTotalNum(int statis_type, ES_TIME *es_time);
	int GetDMSFailureNum(int statis_type, ES_TIME *es_time);//DMS停电故障数
	int GetGPMSFailureNum(int statis_type, ES_TIME *es_time);//GPMS停电故障数

	int GetYCsRate(int statis_type, ES_TIME *es_time);

	int GetYCRateByDay(ES_TIME *es_time);
	int GetYCRateByMonth(ES_TIME *es_time);

	int InsertResultTZRate(int statis_type, ES_TIME *es_time);
	int InsertResultTdintimeRate(int statis_type, ES_TIME *es_time);//不用了
	int InsertResultDmstdpubRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDFailureRate(int statis_type, ES_TIME *es_time);

	int GetDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMS停电研判发布数zgs
	int GetGDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMS停电研判发布数zgs 按停电范围
	int GetGDMSJudged(int statis_type, ES_TIME *es_time);
	int GetGDMSJudged_new(int statis_type, ES_TIME *es_time);
	int GetDmsNoUseJudged(int statis_type, ES_TIME *es_time);
	int SetRightJudgedDetType(int statis_type, ES_TIME *es_time);
	int GetRightJudgedNum(int statis_type, ES_TIME *es_time);
	int InsertResultTDJudgedRate(int statis_type, ES_TIME *es_time);//插入故障研判发布率结果
	int InsertResultTDJudgedRateDET(int statis_type, ES_TIME *es_time);//插入研判成功但未发布明细

	int GetJudgedNoUseDet(int statis_type, ES_TIME *es_time);

	int GetStringCount(char *dms_str,const char *dms_ivalue,char *gpms_str,const char *gpms_ivalue);

	int GetAllOrgan(map<int,float> &map_organ);

	//int GetWeight();

	void Clear();

private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::map<int, float> m_organ_ppnum;
	std::map<int, float> m_organ_cjtotal;
	std::map<int, float> m_organ_sendnum;
	std::map<int, float> m_organ_intime;
	std::map<int, float> m_organ_confirm;
	std::map<int, float> m_organ_tdtotal;//存放DMS总发送到GPMS停电条数
	std::map<int, float> m_organ_dmsfailure;//
	std::map<int, float> m_organ_gpmsfailure;//


	std::map<int, float> m_tzmatch_result;
	std::map<int, float> m_tzsuccess_result;
	std::map<int, float> m_intime_result;
	std::map<int, float> m_confirm_result;
	std::map<int, float> m_failure_result;//

	//std::map<int, float> m_tzmatch_score;
	//std::map<int, float> m_tzsuccess_score;
	//std::map<int, float> m_intime_score;
	//std::map<int, float> m_confirm_score;
	//std::map<int, float> m_failure_score;//
	//std::map<int, float> m_judged_score;

	//std::map<string, float> m_code_weight;

	std::multimap<int, ES_GDMSJUDGED> map_Suc_judged;//研判正确但未发布列表
	std::map<int, float> map_organ_judgednum;//DMS研判发布条数
	std::map<int, float> map_organ_judgedNoUse;//研判成功但未應用個數
	std::map<int, float> m_judged_result;//存放故障研判发布率结果
	std::multimap<int,ES_GDMSJUDGED> map_judged;//存放研判未发布时间早组合
	
	std::vector<string> dms_tdintr;
	std::vector<string> gpms_tdintr;

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
