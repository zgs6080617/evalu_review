/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dms.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 主站运行率统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_MODEL_H_
#define _STATIS_MODEL_H_
#include <iostream>
#include "dcisg.h"
#include "es_define.h"

class StatisModel
{
public:
	StatisModel(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisModel();

	int GetDMSModelRate(int statis_type, ES_TIME *es_time);
	int GetModelInfo(ES_TIME *es_time);
	int GetModelProcess(ES_TIME *es_time);
	int ModelCheck(ES_TIME *es_time);
	int getHHStatus(ES_TIME *es_time);
	int DeleteHH(ES_TIME *es_time);

private:
	int GetGPZWsByDay(ES_TIME *es_time);
	int GetGPTotalsByDay(ES_TIME *es_time);
	int GetDevtpgoodByDay(ES_TIME *es_time);
	int GetDevicesByDay(ES_TIME *es_time);
	int GetLoopnumByDay(ES_TIME *es_time);
	int GetTpdvgoodByDay(ES_TIME *es_time);//
	int GetTpdvtotalByDay(ES_TIME *es_time);//
	int GetAutoRnumByDay(ES_TIME *es_time);//
	int GetAutoSnumByDay(ES_TIME *es_time);//

	int GetDevtpgoodByMonth(ES_TIME *es_time);//
	int GetDevicesByMonth(ES_TIME *es_time);//
	int GetLoopnumByMonth(ES_TIME *es_time);//
	int GetTpdvgoodByMonth(ES_TIME *es_time);//
	int GetTpdvtotalByMonth(ES_TIME *es_time);//

	int GetModelByMonth(ES_TIME *es_time);

	int DeleteModel(ES_TIME *es_time);
	
	//更新自动成图数据
	int UpdateAutograph(ES_TIME *es_time);//
	//插入自动成图率数据
	//int InsertAutoData(ES_TIME *es_time);

	int GetModelRate(int statis_type, ES_TIME *es_time);
	int GetTPRate(int statis_type, ES_TIME *es_time);//

	int InsertResultGPZWRate(int statis_type, ES_TIME *es_time);
	int InsertResultTPgoodRate(int statis_type, ES_TIME *es_time);
	int InsertResultToMonth(ES_TIME *es_time);//
	int InsertResultAutoGraph(int statis_type, ES_TIME *es_time);//

	//int GetWeight();
	void Clear();

private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::map<int, float> m_organ_gpzw;
	std::map<ES_ORGAN_GP, float> m_gp_gpzw;
	std::map<int, float> m_organ_devtpgood;
	std::map<int, float> m_organ_devices;
	std::map<int, float> m_organ_loopnum;
	std::map<int, float> m_organ_tpdvgood;//
	std::map<int, float> m_organ_tpdvtotal;//
	std::map<int, float> m_organ_rnum;
	std::map<int, float> m_organ_snum;

	std::vector<ES_HH> v_hh;



	std::map<int, float> m_organ_gp;
	std::map<ES_ORGAN_GP, float> m_gp_gp;
	std::map<int, float> m_topogood_result;
	std::map<int, float> m_topogood2_result;//
	std::map<int, float> m_topogood3_result;//
	std::map<int, float> m_autograph_result;

	/*std::map<int, float> m_topogood_score;
	std::map<int, float> m_topogood2_score;//
	std::map<int, float> m_topogood3_score;//
	std::map<int, float> m_autograph_score;*/

	//std::map<string, float> m_code_weight;


	std::multimap<int, ES_RESULT> m_gpzw_result;

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};
#endif
