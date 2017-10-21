/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_log.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 计算总得分
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _ES_SCORE_H_
#define _ES_SCORE_H_
#include <iostream>
#include <map>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class ESScore
{
public:
	ESScore();
	~ESScore();
	
	int GetWeight();
	int GetScore();
	int GetMark(int statis_type, ES_TIME *es_time);

	int CaculateMark();
	int GetK(const string &code,float v,float &k);

	int InsertNum(int statis_type, ES_TIME *es_time);

	int InsertPointScore(int statis_type, ES_TIME *es_time);

	int InsertScore(int statis_type, ES_TIME *es_time);

	void Clear();

private:
	CDci *m_oci;
	ErrorInfo err_info;

	map<string, float> m_code_weight;
	multimap<string, ES_SCORE> m_code_score;
	multimap<string, ES_MARK> m_code_mark;
	multimap<string, ES_MARK> m_code_pointmark;

	map<string, float>::iterator itr_code_weight;
	multimap<string, ES_SCORE>::iterator itr_code_score;
	multimap<string, ES_MARK>::iterator itr_code_mark;
	multimap<string, ES_MARK>::iterator itr_code_pointmark;
};

#endif
