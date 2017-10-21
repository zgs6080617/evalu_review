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
	/// @brief 获取日数据分子
	int GetMolecularByDay();
	int GetMolecularByDayForTrans();
	int GetMolecularByDayForCb();
	int GetMolecularByDayForDis();
	int GetMolecularByDayForSubs();
	int GetMolecularByDayForBus();
	int GetMolecularByDayForStatus();
	/// @brief 获取日数据分母
	int GetDenominatorByDay();
	int GetDenominatorByDayForTrans();
	int GetDenominatorByDayForCb();
	int GetDenominatorByDayForDis();
	int GetDenominatorByDayForSubs();
	int GetDenominatorByDayForBus();
	int GetDenominatorByDayForStatus();
	/// @brief 计算日指标值
	int GetRateByDay();
	/// @brief 获取月指标值 */
	int GetRateByMonth(ES_TIME *es_time);
	/// @brief 插入指标值
	int InsertResult(int statis_type, ES_TIME *es_time);
	/// @brief 生成明细 */
	int GenerateDetailByProcedure(ES_TIME *es_time);
	/// @brief 去除设备台账重复 */
	int DeleteDevRepetitionByProcedure();
	/// @brief 计算平均争取率 */
	int GetAverageRate(int organ_code,float result);
	/// @brief 利用ID对应表更新升级表对应PREDEVID */
	int GetCorrForDevUpByProcedure();
	/* // @brief 实现存储过程DGPMSPROCESS_UP(存储过程速率较直接执行SQL速率降低很多),获取升级正确率指标明细 */
	int GetUpdateWrongDetail(ES_TIME *es_time);

public:
protected:
private:
	CDci *m_oci;
	ErrorInfo err_info;

	/// @brief 指标数据分子
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Trans;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Cb;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Dis;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Subs;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Bus;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Molecular_Status;
	/// @brief 指标数据分子 */
	std::map<int, float> m_mOrganCode2Molecular_Trans;
	std::map<int, float> m_mOrganCode2Molecular_Cb;
	std::map<int, float> m_mOrganCode2Molecular_Dis;
	std::map<int, float> m_mOrganCode2Molecular_Subs;
	std::map<int, float> m_mOrganCode2Molecular_Bus;
	std::map<int, float> m_mOrganCode2Molecular_Status;
	/// @brief 指标数据分母
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Trans;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Cb;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Dis;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Subs;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Bus;
	std::map<std::string, ES_DATIMES> m_mOrganDv2Denominator_Status;
	/// @brief 指标数据分母
	std::map<int, float> m_mOrganCode2Denominator_Trans;
	std::map<int, float> m_mOrganCode2Denominator_Cb;
	std::map<int, float> m_mOrganCode2Denominator_Dis;
	std::map<int, float> m_mOrganCode2Denominator_Subs;
	std::map<int, float> m_mOrganCode2Denominator_Bus;
	std::map<int, float> m_mOrganCode2Denominator_Status;
	/// @brief 指标数据计算结果
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Trans;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Cb;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Dis;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Subs;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Bus;
	std::multimap<int, ES_RESULT> m_mOrganCode2Result_Status;
	/// @brief 平均正确率 */
	std::map<int, float> m_mOrganCode2AvgRate; //完整率平均值
};

#endif



