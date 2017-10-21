/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_terminal.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 终端相关统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_TERMINAL_H_
#define _STATIS_TERMINAL_H_
#include <iostream>
#include "dcisg.h"
#include "es_define.h"

class StatisTerminal
{
public:
	StatisTerminal(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisTerminal();

	int GetTerminalRunningRate(int statis_type, ES_TIME *es_time);

private:
	int GetTerminalsByDay(ES_TIME *es_time);
	int GetOfflineByDay(ES_TIME *es_time);
	int GetYKAccTimesByDay(ES_TIME *es_time);
	int GetYKTimesByDay(ES_TIME *es_time);
	int GetCanYKTimesByDay(ES_TIME *es_time);
	int GetYXAccTimesByDay(ES_TIME *es_time);
	int GetYXTimesByDay(ES_TIME *es_time);

	/*
	 *	故指信息
	 */
	int GetindicationsByDay(ES_TIME *es_time);
	int GetindicationOfflineByDay(ES_TIME *es_time);
	/*
	 *	二遥终端
	 */
	int GettworemotesByDay(ES_TIME *es_time);
	int GettworemoteOfflineByDay(ES_TIME *es_time);
	/*
	 *	三遥终端
	 */
	int GetthreeremotesByDay(ES_TIME *es_time);
	int GetthreeremoteOfflineByDay(ES_TIME *es_time);

	int GetTerminalsByMonth(ES_TIME *es_time);
	int GetOfflineByMonth(ES_TIME *es_time);
	int GetTotalTimeByMonth(ES_TIME *es_time);
	int GetYKAccTimesByMonth(ES_TIME *es_time);
	int GetYKTimesByMonth(ES_TIME *es_time);
	int GetCanYKTimesByMonth(ES_TIME *es_time);
	int GetYXAccTimesByMonth(ES_TIME *es_time);
	int GetYXTimesByMonth(ES_TIME *es_time);

	/*
	 *	故指信息
	 */
	int GetindicationsByMonth(ES_TIME *es_time);
	int GetindicationOfflineByMonth(ES_TIME *es_time);
	int GetindicationTotalTimeByMonth(ES_TIME *es_time);
	/*
	 *	二遥终端
	 */
	int GettworemotesByMonth(ES_TIME *es_time);
	int GettworemoteOfflineByMonth(ES_TIME *es_time);
	int GettworemoteTotalTimeByMonth(ES_TIME *es_time);
	/*
	 *	三遥终端
	 */
	int GetthreeremotesByMonth(ES_TIME *es_time);
	int GetthreeremoteOfflineByMonth(ES_TIME *es_time);
	int GetthreeremoteTotalTimeByMonth(ES_TIME *es_time);

	int GetTerminalRate(int statis_type, ES_TIME *es_time);

	int InsertResultOnlineRate(int statis_type, ES_TIME *es_time);
	int InsertResultYKAccRate(int statis_type, ES_TIME *es_time);
	int InsertResultYKUsingRate(int statis_type, ES_TIME *es_time);
	int InsertResultYXAccRate(int statis_type, ES_TIME *es_time);

	//int GetWeight();
	void Clear();

private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::map<std::string, ES_DATIMES> m_num_terminal;
	std::map<int, float> m_organ_terminal;

	std::map<std::string, ES_DATIMES> m_num_indicator;
	std::map<int, float> m_organ_indicator;

	std::map<std::string, ES_DATIMES> m_num_tworemote;
	std::map<int, float> m_organ_tworemote;

	std::map<std::string, ES_DATIMES> m_num_threeremote;
	std::map<int, float> m_organ_threeremote;

	std::map<std::string, ES_DATIMES> m_num_offline;
	std::map<int, float> m_organ_offline;
	std::map<int, float> m_organ_total;

	std::map<std::string, ES_DATIMES> m_num_indicatoroffline;
	std::map<int, float> m_organ_indicatoroffline;
	std::map<int, float> m_organ_indicatortotal;

	std::map<std::string, ES_DATIMES> m_num_tworemoteoffline;
	std::map<int, float> m_organ_tworemoteoffline;
	std::map<int, float> m_organ_tworemotetotal;

	std::map<std::string, ES_DATIMES> m_num_threeremoteoffline;
	std::map<int, float> m_organ_threeremoteoffline;
	std::map<int, float> m_organ_threeremotetotal;

	std::map<std::string, ES_DATIMES> m_cb_ykacc;
	std::map<int, float> m_organ_ykacc;

	std::map<std::string, ES_DATIMES> m_cb_yk;
	std::map<int, float> m_organ_yk;

	std::map<int, float> m_organ_canyk;

	std::map<int, float> m_organ_yxacc;
	std::map<int, float> m_organ_yx;

	std::multimap<int, ES_RESULT> m_online_result;
	std::multimap<int, ES_RESULT> m_indicatoronline_result; //故指
	std::multimap<int, ES_RESULT> m_tworemoteonline_result; //二遥
	std::multimap<int, ES_RESULT> m_threeremoteonline_result; //三遥
	std::multimap<int, ES_RESULT> m_ykacc_result;
	std::map<int, float> m_ykusing_result;
	std::map<int, float> m_yxacc_result;

	/*std::map<int, float> m_ykusing_score;
	std::map<int, float> m_yxacc_score;

	std::map<string, float> m_code_weight;*/

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
