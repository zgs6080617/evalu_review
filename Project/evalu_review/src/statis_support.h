/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_support.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : ����ͳ��ͷ�ļ�
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

	//��������ϸͳ��
	int GetCorrective(ES_TIME *es_time, int _type);

	/// @brief ����������ָ�����
	int GetCorrectRate(int statis_type, ES_TIME *es_time);
	/// @brief �����������������
	int InsertData(int statis_type, ES_TIME *es_time);
	/// @brief ���
	void Clear();
	/// @brief ��ȡ����������������
	int GetDMSCorrectiveByDay(ES_TIME *es_time);
	/// @brief ����������������ָ��
	int CaculateCorrectRateForDay(int statis_type, ES_TIME *es_time);
	/// @brief ����������������ָ��
	int CaculateCorrectRateForMonth(int statis_type, ES_TIME *es_time);

private:
	CDci *m_oci;
	ErrorInfo err_info;

public:
	std::map<int, ES_Corrective> m_organ_corrective;
	std::multimap<int, ES_RESULT> m_corrective_result;
};

#endif
