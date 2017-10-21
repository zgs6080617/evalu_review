/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dgpms.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 设备类相关统计头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_DGPMS_H_
#define _STATIS_DGPMS_H_
#include <iostream>
#include "dcisg.h"
#include "es_define.h"

class StatisDGPMS
{
public:
	StatisDGPMS(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisDGPMS();

	int GetGMSGPMSInteractiveRate(int statis_type, ES_TIME *es_time);
	int GetDGPMSProcess(ES_TIME *es_time);

private:
	int GetDMSLinesByDay(ES_TIME *es_time);
	int GetGPMSLinesByDay(ES_TIME *es_time);
	int GetDMSCBsByDay(ES_TIME *es_time);
	int GetGPMSCBsByDay(ES_TIME *es_time);
	int GetDMSDISsByDay(ES_TIME *es_time);
	int GetGPMSDISsByDay(ES_TIME *es_time);
	int GetGpRightsByDay(ES_TIME *es_time);//g
	int GetDMSGpDevsByDay(ES_TIME *es_time);//g
	int GetGPMSGpDevsByDay(ES_TIME *es_time);//g
	int GetDMSGPMSGpDevsByDay(ES_TIME *es_time);//g
	int GetSyncRightDevsByDay(ES_TIME *es_time);//状态置位总数设备数
	int GetDMSStatusDevsByDay(ES_TIME *es_time);//状态置位设备总数
	int GetGPMSStatusDevsByDay(ES_TIME *es_time);//GPMS状态置位数
	int GetDMSLDsByDay(ES_TIME *es_time);
	int GetGPMSLDsByDay(ES_TIME *es_time);
	int GetDMSBUSsByDay(ES_TIME *es_time);//
	int GetGPMSBUSsByDay(ES_TIME *es_time);//
	int GetDMSSTsByDay(ES_TIME *es_time);//
	int GetGPMSSTsByDay(ES_TIME *es_time);//


	int GetGMSGPMSByMonth(ES_TIME *es_time);

	int GetDMSGPMSRate(int statis_type, ES_TIME *es_time);

	int InsertResultLinesRate(int statis_type, ES_TIME *es_time);
	int InsertResultCBsRate(int statis_type, ES_TIME *es_time);
	int InsertResultDISsRate(int statis_type, ES_TIME *es_time);
	int InsertResultInfoRate(int statis_type, ES_TIME *es_time);
	int InsertResultStatusRate(int statis_type, ES_TIME *es_time);
	int InsertResultLDsRate(int statis_type, ES_TIME *es_time);
	int InsertResultBUSsRate(int statis_type, ES_TIME *es_time);//
	int InsertResultSTsRate(int statis_type, ES_TIME *es_time);//

	/*zgs*/
	//int GetWeight();
	int GetDMSCorrectiveByDay(ES_TIME *es_time);
	int GetDMSCorrectiveByMonth(ES_TIME *es_time);
	int GetDMSCorrectiveRate(int statis_type, ES_TIME *es_time);
	int InsertResultCorrectivesRate(int statis_type, ES_TIME *es_time);

	int GetAverageRate(int organ_code,float result);


	void Clear();

private:
	CDci *m_oci;
	ErrorInfo err_info;
	
	std::map<int, float> m_organ_dlines;
	std::map<int, float> m_organ_gplines;


	std::map<int, float> m_organ_dcbs;
	std::map<std::string, ES_DATIMES> m_dv_dcbs;
	std::map<int, float> m_organ_gpcbs;
	std::map<std::string, ES_DATIMES> m_dv_gpcbs;

	std::map<int, float> m_organ_ddiss;
	std::map<std::string, ES_DATIMES> m_dv_ddiss;
	std::map<int, float> m_organ_gpdiss;
	std::map<std::string, ES_DATIMES> m_dv_gpdiss;

	std::map<int, float> m_organ_dlds;
	std::map<std::string, ES_DATIMES> m_dv_dlds;
	std::map<int, float> m_organ_gplds;
	std::map<std::string, ES_DATIMES> m_dv_gplds;

	std::map<int, float> m_organ_rights;
	std::map<ES_ORGAN_GP, float> m_gp_rights;

	std::map<int, float> m_organ_dgpdevs;
	std::map<ES_ORGAN_GP, float> m_gp_dgpdevs;

	std::map<int, float> m_organ_gpgpdevs;
	std::map<ES_ORGAN_GP, float> m_gp_gpgpdevs;

	std::map<int, float> m_organ_dgpgpdevs;
	std::map<ES_ORGAN_GP, float> m_gp_dgpgpdevs;

	std::map<int, float> m_organ_sync;
	std::map<std::string, ES_DATIMES> m_dv_sync;
	
	std::map<int, float> m_organ_dsdevs;
	std::map<std::string, ES_DATIMES> m_dv_dsdevs;

	std::map<int, float> m_organ_gpsdevs;
	std::map<std::string, ES_DATIMES> m_dv_gpsdevs;

	std::map<int, float> m_organ_dmsbus;//
	std::map<int, float> m_organ_gpmsbus;//

	std::map<int, float> m_organ_dmsst;//
	std::map<int, float> m_organ_gpmsst;//

	std::map<int, ES_RESULT> m_lines_result;
	std::multimap<int, ES_RESULT> m_cbs_result;
	std::multimap<int, ES_RESULT> m_diss_result;
	std::multimap<int, ES_RESULT> m_info_result;
	std::multimap<int, ES_RESULT> m_status_result;
	std::multimap<int, ES_RESULT> m_lds_result;
	std::map<int, float> m_bus_result;//
	std::map<int, float> m_st_result;//

	std::map<int,float> m_dev_result; //完整率平均值

	std::map<int,ES_Corrective> m_organ_corrective;//zgs
	std::multimap<int,ES_RESULT> m_corrective_result;//zgs

	/*std::map<string, float> m_code_weight;
	std::map<int, float> m_bus_score;//
	std::map<int, float> m_st_score;*///


	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif
