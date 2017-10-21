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
	/* 读取dms_line当日线路明细                                               */ 
	/************************************************************************/
	int ReadDmsLinesByDay(ES_TIME *es_time);
	int ReadDmsLinesByMonth(ES_TIME *es_time);

	/************************************************************************/
	/* 计算线路设备覆盖率                                                    */ 
	/************************************************************************/
	int CaculateLineRate();


	/************************************************************************/
	/* 插入结果                                                              */ 
	/************************************************************************/
	int InsertResultData(int statis_type, ES_TIME *es_time);

public:

protected:
private:
	CDci *m_oci;
	ErrorInfo err_info;

	/*
	 *	线路总条数
	 */
	std::map<int,float> m_mlines;

	/*
	 *	挂接故指线路条数
	 */
	std::map<int,float> m_mindacator;

	/*
	 *	挂接二遥终端线路条数
	 */
	std::map<int,float> m_mtwoterminal;

	/*
	 *	挂接三遥终端线路条数
	 */
	std::map<int,float> m_mthreeterminal;

	/*
	 *	线路故指覆盖率结果
	 */
	std::map<int,float> m_mindacator_result;

	/*
	 *	线路二遥终端覆盖率结果
	 */
	std::map<int,float> m_mtwo_result;

	/*
	 *	线路三遥终端覆盖率结果
	 */
	std::map<int,float> m_mthree_result;
};

#endif

