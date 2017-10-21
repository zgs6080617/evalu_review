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
	/// @brief 调用执行文件，解析模型 */
	int ModelCheck(ES_TIME *es_time);
	/// @brief 获取删除供DeleteHH */
	int getHHStatus(ES_TIME *es_time);
	int DeleteHH(ES_TIME *es_time);
	/// @brief 升级 */
	int getHHStatusForUp(ES_TIME *es_time);
	int DeleteHHForUp(ES_TIME *es_time);

private:
	int GetGPZWsByDay(ES_TIME *es_time);
	/// @brief 未应用 */
	int GetGPTotalsByDay(ES_TIME *es_time);
	/// @brief 获取拓扑正确设备数通过加和各条馈线 */
	int GetDevtpgoodByDay(ES_TIME *es_time);
	/// @brief 获取拓扑设备总数通过加和各条馈线 */
	int GetDevicesByDay(ES_TIME *es_time);
	/// @brief 获取合环设备总数通过加和各条馈线 */
	int GetLoopnumByDay(ES_TIME *es_time);
	/// @brief 获取拓扑正确率为100的馈线条数 */
	int GetTpdvgoodByDay(ES_TIME *es_time);//
	/// @brief 获取馈线总条数 */
	int GetTpdvtotalByDay(ES_TIME *es_time);//
	int GetAutoRnumByDay(ES_TIME *es_time);//
	int GetAutoSnumByDay(ES_TIME *es_time);//

	/// @brief 获取本月拓扑正确的设备总数 */
	int GetDevtpgoodByMonth(ES_TIME *es_time);//
	/// @brief 获取本月设备总数 */
	int GetDevicesByMonth(ES_TIME *es_time);//
	/// @brief 获取本月合环设备总数 */
	int GetLoopnumByMonth(ES_TIME *es_time);//
	/// @brief 获取本月拓扑正确率指标为100的馈线条数 */
	int GetTpdvgoodByMonth(ES_TIME *es_time);//
	/// @brief 获取本月馈线总条数 */
	int GetTpdvtotalByMonth(ES_TIME *es_time);//
	/// @brief 计算自动成图 */
	int GetModelByMonth(ES_TIME *es_time);

	int DeleteModel(ES_TIME *es_time);
	
	//更新自动成图数据
	int UpdateAutograph(ES_TIME *es_time);//
	//插入自动成图率数据
	//int InsertAutoData(ES_TIME *es_time);

	int GetModelRate(int statis_type, ES_TIME *es_time);
	int GetTPRate(int statis_type, ES_TIME *es_time);//

	int InsertResultGPZWRate(int statis_type, ES_TIME *es_time);
	/* // @brief 插入拓扑正确率指标 */
	int InsertResultTPgoodRate(int statis_type, ES_TIME *es_time);
	/* // @brief 日表数据插入月表中(月表中存储所有馈线记录)(有用吗) */
	int InsertResultToMonth(ES_TIME *es_time);//
	int InsertResultAutoGraph(int statis_type, ES_TIME *es_time);//

	//int GetWeight();
	void Clear();

	/// @brief 新增升级后拓扑计算 */
	/* // @brief 获取拓扑正确设备数通过加和各条馈线 */
	int GetDevtpgoodByDayForUp(ES_TIME *es_time);
	/* // @brief 获取拓扑设备总数通过加和各条馈线 */
	int GetDevicesByDayForUp(ES_TIME *es_time);
	/* // @brief 获取合环设备总数通过加和各条馈线 */
	int GetLoopnumByDayForUp(ES_TIME *es_time);
	/* // @brief 获取拓扑正确率为100的馈线条数 */
	int GetTpdvgoodByDayForUp(ES_TIME *es_time);//
	/* // @brief 获取馈线总条数 */
	int GetTpdvtotalByDayForUp(ES_TIME *es_time);//
	/* // @brief 计算指标值 */
	int CaculateTopogoodRate();
	/* // @brief 插入指标值 */
	int InsertTopogoodData(int statis_type, ES_TIME *es_time);

	/* // @brief 获取本月拓扑正确的设备总数 */
	int GetDevtpgoodByMonthForUp(ES_TIME *es_time);//
	/* // @brief 获取本月设备总数 */
	int GetDevicesByMonthForUp(ES_TIME *es_time);//
	/* // @brief 获取本月合环设备总数 */
	int GetLoopnumByMonthForUp(ES_TIME *es_time);//
	/* // @brief 获取本月拓扑正确率指标为100的馈线条数 */
	int GetTpdvgoodByMonthForUp(ES_TIME *es_time);//
	/* // @brief 获取本月馈线总条数 */
	int GetTpdvtotalByMonthForUp(ES_TIME *es_time);//

private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::map<int, float> m_organ_gpzw;
	std::map<ES_ORGAN_GP, float> m_gp_gpzw;
	/* // @detail 存放拓扑正确设备数 */
	std::map<int, float> m_organ_devtpgood; 
	/* // @detail 存放拓扑设备总数 */
	std::map<int, float> m_organ_devices;
	/* // @detail 存放合环设备总数 */
	std::map<int, float> m_organ_loopnum;
	/* // @detail 存储指标值为100的馈线数 */
	std::map<int, float> m_organ_tpdvgood;//
	/* // @detail 存放馈线总数 */
	std::map<int, float> m_organ_tpdvtotal;//
	std::map<int, float> m_organ_rnum;
	std::map<int, float> m_organ_snum;

	std::vector<ES_HH> v_hh;
	std::vector<ES_HH> m_vEsHh;

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

	/* // @brief 升级模型数据存储 */
	/* // @detail 存放拓扑正确设备数 */
	std::map<int, float> m_mOrganCode2DevtpgoodNum;
	/* // @detail 存放拓扑设备总数 */
	std::map<int, float> m_mOrganCode2DevicesNum;
	/* // @detail 存放合环设备总数 */
	std::map<int, float> m_mOrganCode2LoopNum;
	/* // @detail 存储指标值为100的馈线数 */
	std::map<int, float> m_mOrganCode2TpdvgoodNum;
	/* // @detail 存放馈线总数 */
	std::map<int, float> m_mOrganCode2TpdvtotalNum;
	/* // @detail 拓扑正确率按设备 */
	std::map<int, float> m_mOrganCode2TpGoodRate;
	/* // @detail 拓扑正确率按馈线 */
	std::map<int, float> m_mOrganCode2TpGood2Rate;//
	/* // @detail 拓扑正确率按合环设备 */
	std::map<int, float> m_mOrganCode2TpGood3Rate;//
};
#endif
