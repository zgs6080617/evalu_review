/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_yc.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : �ò����ͳ��ͷ�ļ�
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
	int GetDMSFailureNum(int statis_type, ES_TIME *es_time);//DMSͣ�������
	int GetGPMSFailureNum(int statis_type, ES_TIME *es_time);//GPMSͣ�������

	int GetYCsRate(int statis_type, ES_TIME *es_time);

	int GetYCRateByDay(ES_TIME *es_time);
	int GetYCRateByMonth(ES_TIME *es_time);

	int InsertResultTZRate(int statis_type, ES_TIME *es_time);
	int InsertResultTdintimeRate(int statis_type, ES_TIME *es_time);//������
	int InsertResultDmstdpubRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDFailureRate(int statis_type, ES_TIME *es_time);

	int GetDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMSͣ�����з�����zgs
	int GetGDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMSͣ�����з�����zgs ��ͣ�緶Χ
	int GetGDMSJudged(int statis_type, ES_TIME *es_time);
	int GetGDMSJudged_new(int statis_type, ES_TIME *es_time);
	int GetDmsNoUseJudged(int statis_type, ES_TIME *es_time);
	int SetRightJudgedDetType(int statis_type, ES_TIME *es_time);
	int GetRightJudgedNum(int statis_type, ES_TIME *es_time);
	int InsertResultTDJudgedRate(int statis_type, ES_TIME *es_time);//����������з����ʽ��
	int InsertResultTDJudgedRateDET(int statis_type, ES_TIME *es_time);//�������гɹ���δ������ϸ

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
	std::map<int, float> m_organ_tdtotal;//���DMS�ܷ��͵�GPMSͣ������
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

	std::multimap<int, ES_GDMSJUDGED> map_Suc_judged;//������ȷ��δ�����б�
	std::map<int, float> map_organ_judgednum;//DMS���з�������
	std::map<int, float> map_organ_judgedNoUse;//���гɹ���δ���Â���
	std::map<int, float> m_judged_result;//��Ź������з����ʽ��
	std::multimap<int,ES_GDMSJUDGED> map_judged;//�������δ����ʱ�������
	
	std::vector<string> dms_tdintr;
	std::vector<string> gpms_tdintr;

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
