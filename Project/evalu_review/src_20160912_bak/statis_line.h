#pragma once

#ifndef _STATIS_LINE
#define _STATIS_LINE


#include <iostream>
#include <vector>
#include <map>
#include <algorithm>
#include <limits>

#include "es_define.h"
#include "dcisg.h"
#include "datetime.h"

class STATISLINES
{
public:

	STATISLINES() {

		m_oci = new CDci;
		m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	}

	~STATISLINES() {

		m_oci->DisConnect(&err_info);
	}

	int Operator(int statis_type,ES_TIME *es_time);
protected:
private:
	void Clear();
	/************************************************************************/
	/* ��ȡdms_line������·��ϸ                                               */ 
	/************************************************************************/
	int ReadDmsLinesByDay(ES_TIME *es_time);
	int ReadDmsLinesByMonth(ES_TIME *es_time);

	/************************************************************************/
	/* ������·�豸������                                                    */ 
	/************************************************************************/
	int CaculateLineRate();


	/************************************************************************/
	/* ������                                                              */ 
	/************************************************************************/
	int InsertResultData(int statis_type, ES_TIME *es_time);

public:

protected:
private:
	CDci *m_oci;
	ErrorInfo err_info;

	/*
	 *	��·������
	 */
	std::map<int,float> m_mlines;

	/*
	 *	�ҽӹ�ָ��·����
	 */
	std::map<int,float> m_mindacator;

	/*
	 *	�ҽӶ�ң�ն���·����
	 */
	std::map<int,float> m_mtwoterminal;

	/*
	 *	�ҽ���ң�ն���·����
	 */
	std::map<int,float> m_mthreeterminal;

	/*
	 *	��·��ָ�����ʽ��
	 */
	std::map<int,float> m_mindacator_result;

	/*
	 *	��·��ң�ն˸����ʽ��
	 */
	std::map<int,float> m_mtwo_result;

	/*
	 *	��·��ң�ն˸����ʽ��
	 */
	std::map<int,float> m_mthree_result;
};

#endif

