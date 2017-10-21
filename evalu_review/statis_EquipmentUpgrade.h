#ifndef _EQUIPMENTUPGRADE_
#define _EQUIPMENTUPGRADE_

#include <iostream>
#include <string>
#include <vector>
#include <map>

#include "datetime.h"
#include "es_log.h"
#include "es_define.h"

class EquipmentUpgrade
{
public:
	EquipmentUpgrade();
	~EquipmentUpgrade();

	void Operator(int statis_type, ES_TIME *es_time);
protected:
private:
	void Clear();
	/// @brief ��ȡ�����ݷ���
	int GetMolecularByDay();
	int GetMolecularByDayForTrans();
	int GetMolecularByDayForCb();
	int GetMolecularByDayForDis();
	int GetMolecularByDayForSubs();
	int GetMolecularByDayForBus();
	int GetMolecularByDayForStatus();
	/// @brief ��ȡ�����ݷ�ĸ
	int GetDenominatorByDay();
	int GetDenominatorByDayForTrans();
	int GetDenominatorByDayForCb();
	int GetDenominatorByDayForDis();
	int GetDenominatorByDayForSubs();
	int GetDenominatorByDayForBus();
	int GetDenominatorByDayForStatus();
	/// @brief ������ָ��ֵ
	int GetRateByDay();
	/// @brief ��ȡ��ָ��ֵ */
	int GetRateByMonth(ES_TIME *es_time);
	/// @brief ����ָ��ֵ
	int InsertResult(int statis_type, ES_TIME *es_time);
	/// @brief ������ϸ */
	int GenerateDetailByProcedure(ES_TIME *es_time);
	/// @brief ȥ���豸̨���ظ� */
	int DeleteDevRepetitionByProcedure();
	/// @brief ����ƽ����ȡ�� */
	int GetAverageRate(int organ_code,float result);
	/// @brief ����ID��Ӧ������������ӦPREDEVID */
	int GetCorrForDevUpByProcedure();
	/* // @brief ʵ�ִ洢����DGPMSPROCESS_UP(�洢�������ʽ�ֱ��ִ��SQL���ʽ��ͺܶ�),��ȡ������ȷ��ָ����ϸ */
	int GetUpdateWrongDetail(ES_TIME *es_time);

public:
protected:
private:
	CDci *m_oci;
	ErrorInfo err_info;

	/// @brief ָ�����ݷ���
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Trans;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Cb;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Dis;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Subs;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Bus;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Status;
	/// @brief ָ�����ݷ��� */
	std::map<int, float> m_mOrganCode2Molecular_Trans;
	std::map<int, float> m_mOrganCode2Molecular_Cb;
	std::map<int, float> m_mOrganCode2Molecular_Dis;
	std::map<int, float> m_mOrganCode2Molecular_Subs;
	std::map<int, float> m_mOrganCode2Molecular_Bus;
	std::map<int, float> m_mOrganCode2Molecular_Status;
	/// @brief ָ�����ݷ�ĸ
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Trans;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Cb;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Dis;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Subs;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Bus;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Status;
	/// @brief ָ�����ݷ�ĸ
	std::map<int, float> m_mOrganCode2Denominator_Trans;
	std::map<int, float> m_mOrganCode2Denominator_Cb;
	std::map<int, float> m_mOrganCode2Denominator_Dis;
	std::map<int, float> m_mOrganCode2Denominator_Subs;
	std::map<int, float> m_mOrganCode2Denominator_Bus;
	std::map<int, float> m_mOrganCode2Denominator_Status;
	/// @brief ָ�����ݼ�����
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Trans;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Cb;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Dis;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Subs;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Bus;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Status;
	/// @brief ƽ����ȷ�� */
	std::map<int, float> m_mOrganCode2AvgRate; //������ƽ��ֵ
};

#endif



