/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dms.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : ��վ������ͳ��ͷ�ļ�
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
	/// @brief ����ִ���ļ�������ģ�� */
	int ModelCheck(ES_TIME *es_time);
	/// @brief ��ȡɾ����DeleteHH */
	int getHHStatus(ES_TIME *es_time);
	int DeleteHH(ES_TIME *es_time);
	/// @brief ���� */
	int getHHStatusForUp(ES_TIME *es_time);
	int DeleteHHForUp(ES_TIME *es_time);

private:
	int GetGPZWsByDay(ES_TIME *es_time);
	/// @brief δӦ�� */
	int GetGPTotalsByDay(ES_TIME *es_time);
	/// @brief ��ȡ������ȷ�豸��ͨ���Ӻ͸������� */
	int GetDevtpgoodByDay(ES_TIME *es_time);
	/// @brief ��ȡ�����豸����ͨ���Ӻ͸������� */
	int GetDevicesByDay(ES_TIME *es_time);
	/// @brief ��ȡ�ϻ��豸����ͨ���Ӻ͸������� */
	int GetLoopnumByDay(ES_TIME *es_time);
	/// @brief ��ȡ������ȷ��Ϊ100���������� */
	int GetTpdvgoodByDay(ES_TIME *es_time);//
	/// @brief ��ȡ���������� */
	int GetTpdvtotalByDay(ES_TIME *es_time);//
	int GetAutoRnumByDay(ES_TIME *es_time);//
	int GetAutoSnumByDay(ES_TIME *es_time);//

	/// @brief ��ȡ����������ȷ���豸���� */
	int GetDevtpgoodByMonth(ES_TIME *es_time);//
	/// @brief ��ȡ�����豸���� */
	int GetDevicesByMonth(ES_TIME *es_time);//
	/// @brief ��ȡ���ºϻ��豸���� */
	int GetLoopnumByMonth(ES_TIME *es_time);//
	/// @brief ��ȡ����������ȷ��ָ��Ϊ100���������� */
	int GetTpdvgoodByMonth(ES_TIME *es_time);//
	/// @brief ��ȡ�������������� */
	int GetTpdvtotalByMonth(ES_TIME *es_time);//
	/// @brief �����Զ���ͼ */
	int GetModelByMonth(ES_TIME *es_time);

	int DeleteModel(ES_TIME *es_time);
	
	//�����Զ���ͼ����
	int UpdateAutograph(ES_TIME *es_time);//
	//�����Զ���ͼ������
	//int InsertAutoData(ES_TIME *es_time);

	int GetModelRate(int statis_type, ES_TIME *es_time);
	int GetTPRate(int statis_type, ES_TIME *es_time);//

	int InsertResultGPZWRate(int statis_type, ES_TIME *es_time);
	/* // @brief ����������ȷ��ָ�� */
	int InsertResultTPgoodRate(int statis_type, ES_TIME *es_time);
	/* // @brief �ձ����ݲ����±���(�±��д洢�������߼�¼)(������) */
	int InsertResultToMonth(ES_TIME *es_time);//
	int InsertResultAutoGraph(int statis_type, ES_TIME *es_time);//

	//int GetWeight();
	void Clear();

	/// @brief �������������˼��� */
	/* // @brief ��ȡ������ȷ�豸��ͨ���Ӻ͸������� */
	int GetDevtpgoodByDayForUp(ES_TIME *es_time);
	/* // @brief ��ȡ�����豸����ͨ���Ӻ͸������� */
	int GetDevicesByDayForUp(ES_TIME *es_time);
	/* // @brief ��ȡ�ϻ��豸����ͨ���Ӻ͸������� */
	int GetLoopnumByDayForUp(ES_TIME *es_time);
	/* // @brief ��ȡ������ȷ��Ϊ100���������� */
	int GetTpdvgoodByDayForUp(ES_TIME *es_time);//
	/* // @brief ��ȡ���������� */
	int GetTpdvtotalByDayForUp(ES_TIME *es_time);//
	/* // @brief ����ָ��ֵ */
	int CaculateTopogoodRate();
	/* // @brief ����ָ��ֵ */
	int InsertTopogoodData(int statis_type, ES_TIME *es_time);

	/* // @brief ��ȡ����������ȷ���豸���� */
	int GetDevtpgoodByMonthForUp(ES_TIME *es_time);//
	/* // @brief ��ȡ�����豸���� */
	int GetDevicesByMonthForUp(ES_TIME *es_time);//
	/* // @brief ��ȡ���ºϻ��豸���� */
	int GetLoopnumByMonthForUp(ES_TIME *es_time);//
	/* // @brief ��ȡ����������ȷ��ָ��Ϊ100���������� */
	int GetTpdvgoodByMonthForUp(ES_TIME *es_time);//
	/* // @brief ��ȡ�������������� */
	int GetTpdvtotalByMonthForUp(ES_TIME *es_time);//

private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::map<int, float> m_organ_gpzw;
	std::map<ES_ORGAN_GP, float> m_gp_gpzw;
	/* // @detail ���������ȷ�豸�� */
	std::map<int, float> m_organ_devtpgood; 
	/* // @detail ��������豸���� */
	std::map<int, float> m_organ_devices;
	/* // @detail ��źϻ��豸���� */
	std::map<int, float> m_organ_loopnum;
	/* // @detail �洢ָ��ֵΪ100�������� */
	std::map<int, float> m_organ_tpdvgood;//
	/* // @detail ����������� */
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

	/* // @brief ����ģ�����ݴ洢 */
	/* // @detail ���������ȷ�豸�� */
	std::map<int, float> m_mOrganCode2DevtpgoodNum;
	/* // @detail ��������豸���� */
	std::map<int, float> m_mOrganCode2DevicesNum;
	/* // @detail ��źϻ��豸���� */
	std::map<int, float> m_mOrganCode2LoopNum;
	/* // @detail �洢ָ��ֵΪ100�������� */
	std::map<int, float> m_mOrganCode2TpdvgoodNum;
	/* // @detail ����������� */
	std::map<int, float> m_mOrganCode2TpdvtotalNum;
	/* // @detail ������ȷ�ʰ��豸 */
	std::map<int, float> m_mOrganCode2TpGoodRate;
	/* // @detail ������ȷ�ʰ����� */
	std::map<int, float> m_mOrganCode2TpGood2Rate;//
	/* // @detail ������ȷ�ʰ��ϻ��豸 */
	std::map<int, float> m_mOrganCode2TpGood3Rate;//
};
#endif
