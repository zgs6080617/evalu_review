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

	int GetDMSJudgeNum(int statis_type, ES_TIME *es_time); //Dms��������

	int GetYCsRate(int statis_type, ES_TIME *es_time);

	int GetYCRateByDay(ES_TIME *es_time);
	int GetYCRateByMonth(ES_TIME *es_time);

	//����ָʾ�����ָ��
	int GetGzzsqRateByDay(ES_TIME *es_time);
	int GetGzzsqRateByMonth(ES_TIME *es_time);

	//ң��������ȷ��
	int GetYcRateByDay(ES_TIME *es_time);
	int GetYcRateByMonth(ES_TIME *es_time);

	//ң��״̬��ʶ��ȷ��
	int GetYXRateByDay(ES_TIME *es_time);
	int GetYXRateByMonth(ES_TIME *es_time);

	//EMSת����ȷ��
	int GetEMSRateByDay(ES_TIME *es_time);
	int GetEMSRateByMonth(ES_TIME *es_time);

	int InsertResultTZRate(int statis_type, ES_TIME *es_time);
	int InsertResultTdintimeRate(int statis_type, ES_TIME *es_time);//������
	int InsertResultDmstdpubRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDRate(int statis_type, ES_TIME *es_time);
	int InsertResultTDFailureRate(int statis_type, ES_TIME *es_time);

	int GetDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMSͣ�����з�����zgs
	int GetGDMSJudgedFailureNum(int statis_type,ES_TIME *es_time);//DMSͣ�����з�����zgs ��ͣ�緶Χ
	int GetGDMSJudged(int statis_type, ES_TIME *es_time);
	int GetDmsNoUseJudged(int statis_type, ES_TIME *es_time);
	int GetGDMSJudged_norelease(int statis_type, ES_TIME *es_time,int type);//
	int GetGDMSJudged_release(int statis_type, ES_TIME *es_time);//
	int SetRightJudgedDetType(int statis_type, ES_TIME *es_time);
	int GetRightJudgedNum(int statis_type, ES_TIME *es_time);
	int GetRightJudgedNum_Indicator(int statis_type, ES_TIME *es_time); //����������ȷ�ʷ��ӹ�ָ��澯
	int GetRightJudgedNum_Trans(int statis_type, ES_TIME *es_time); //����������ȷ�ʷ���ͻ������ר����澯
	int GetRightJudgedNum_Fa(int statis_type, ES_TIME *es_time); //����������ȷ�ʷ���FA��ĸ�ߡ�ȫվ��澯

	int GetRightJudgedNumIntime(int statis_type, ES_TIME *es_time);//dmsͣ��������ȷδ��������ʱ�� lcm 20160107
	int InsertResultTDJudgedRate(int statis_type, ES_TIME *es_time);//����������з����ʽ��
	int InsertResultTDJudgedRateDET(int statis_type, ES_TIME *es_time);//�������гɹ���δ������ϸ

	int GetJudgedNoUseDet(int statis_type, ES_TIME *es_time);
	int GetJudgedAll(int statis_type, ES_TIME *es_time);//DMSͣ����������lcm 20150805
	int GetJudgedAll_Indicator(int statis_type, ES_TIME *es_time);//DMSͣ������������ָ��澯
	int GetJudgedAll_Trans(int statis_type, ES_TIME *es_time);//DMSͣ����������ͻ������ר����澯
	int GetJudgedAll_Fa(int statis_type, ES_TIME *es_time);//DMSͣ������������FA��ĸ�ߡ�ȫվ��澯

	int GetStringCount(string &str,char *ivalue);
	int MatchTdintr();
	int SplitDmsStr(char *dms_str,const char *dms_ivalue,int type = 0);
	int SplitGpmsStr(char *gpms_str,const char *gpms_ivalue);
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

	std::map<int,float> m_organ_dmsjudged; //���DMS������


	std::map<int, float> m_tzmatch_result;
	std::map<int, float> m_tzsuccess_result;
	std::map<int, float> m_xsd_result;
	std::map<int, float> m_intime_result;
	std::map<int, float> m_confirm_result;
	std::map<int, float> m_failure_result;//

	//����ָʾ��ָ��
	std::map<int,float> m_cover_result;
	std::map<int,float> m_shake_result;
	std::map<int,float> m_misoperation_result;
	std::map<int,float> m_missreport_result;

	//��ң������
	std::map<int,float> m_twocover_result;

	//��ң������
	std::map<int,float> m_threecover_result;

	//ң��������ȷ��
	std::map<int,float> m_yc_result;

	//ң��״̬��ʶ��ȷ��
	std::map<int,float> m_yx_result;

	//EMSת����ȷ��
	std::map<int,float> m_ems_result;

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
	std::map<int, float> map_organ_judgedNoUse_Indicator;//���гɹ���δ���Â���
	std::map<int, float> map_organ_judgedNoUse_Trans;//���гɹ���δ���Â���
	std::map<int, float> map_organ_judgedNoUse_Fa;//���гɹ���δ���Â���
	std::map<int, float> map_organ_judgedNoUse_intime;//���гɹ���δ���Â���(��ʱ) lcm 20160107
	std::map<int, float> m_judged_result;//��Ź������з����ʽ��
	std::multimap<int,ES_GDMSJUDGED> map_judged;//�������δ����ʱ�������
	
	std::map<int, float> map_organ_judgedsum;//DMS��������lcm 20150805
	std::map<int, float> map_organ_judgedsum_Indicator;//DMS��������lcm 20150805
	std::map<int, float> map_organ_judgedsum_Trans;//DMS��������lcm 20150805
	std::map<int, float> map_organ_judgedsum_Fa;//DMS��������lcm 20150805
	std::map<int, float> m_judgedright_result;//���������ȷ�ʽ��lcm 20150805
	std::map<int, float> m_judgedright_result_Indicator;//���������ȷ�ʽ��lcm 20150805
	std::map<int, float> m_judgedright_result_Trans;//���������ȷ�ʽ��lcm 20150805
	std::map<int, float> m_judgedright_result_Fa;//���������ȷ�ʽ��lcm 20150805

	std::vector<string> dms_tdintr;
	std::vector<string> gpms_tdintr;
	
	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
