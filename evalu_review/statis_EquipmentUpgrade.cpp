#include "statis_EquipmentUpgrade.h"

using namespace std;

EquipmentUpgrade::EquipmentUpgrade()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

EquipmentUpgrade::~EquipmentUpgrade()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

void EquipmentUpgrade::Operator(int statis_type, ES_TIME *es_time)
{
	if (statis_type == STATIS_DAY) {
		Clear();

		if (GetCorrForDevUpByProcedure() != 0) {
			return;
		} // 

#if 0
		if (DeleteDevRepetitionByProcedure() != 0) {
			return;
		} // 
#endif

		if ((GetMolecularByDay())
			||
			(GetDenominatorByDay())) {
			return;
		} // 

		if (GetRateByDay() == 0) {
			InsertResult(statis_type, es_time);
			GenerateDetailByProcedure(es_time);
		} // 
	} // 
	else if (statis_type == STATIS_MONTH) {
		GetRateByMonth(es_time);
	} // 
}

void EquipmentUpgrade::Clear()
{
	/// @brief 指标数据分子
	m_mOrganDv2Molecular_Trans.clear();
	m_mOrganDv2Molecular_Cb.clear();
	m_mOrganDv2Molecular_Dis.clear();
	m_mOrganDv2Molecular_Subs.clear();
	m_mOrganDv2Molecular_Bus.clear();
	m_mOrganDv2Molecular_Status.clear();
	/// @brief 指标数据分母
	m_mOrganDv2Denominator_Trans.clear();
	m_mOrganDv2Denominator_Cb.clear();
	m_mOrganDv2Denominator_Dis.clear();
	m_mOrganDv2Denominator_Subs.clear();
	m_mOrganDv2Denominator_Bus.clear();
	m_mOrganDv2Denominator_Status.clear();
	/// @brief 指标数据计算结果
	m_mOrganCode2Result_Trans.clear();
	m_mOrganCode2Result_Cb.clear();
	m_mOrganCode2Result_Dis.clear();
	m_mOrganCode2Result_Subs.clear();
	m_mOrganCode2Result_Bus.clear();
	m_mOrganCode2Result_Status.clear();
	m_mOrganCode2AvgRate.clear();
}

int EquipmentUpgrade::GetMolecularByDay()
{
	if ((GetMolecularByDayForTrans())
		||
		(GetMolecularByDayForCb())
		||
		(GetMolecularByDayForDis())
		||
		(GetMolecularByDayForBus())
		||
		(GetMolecularByDayForSubs())
		||
		(GetMolecularByDayForStatus())) {
		printf("GIS1.5 || GIS1.6 getMolecular fail\n");
		return -1;
	} // 


	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForTrans()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSLDUP A, EVALUSYSTEM.DETAIL.DMSLD B,"
		"EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID = B.LD AND A.LD=B.CORRID "
		"AND A.ORGAN_CODE = B.ORGAN_CODE AND A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Trans_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Trans_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Trans_Molecular.dv = string(tempstr);
					break;
				case 2:
					Trans_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Trans.insert(make_pair(Trans_Molecular.dv, Trans_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Trans.find(Trans_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Trans.end()) {
				float count = itr_Molecular->second;
				count += Trans_Molecular.count;
				m_mOrganCode2Molecular_Trans[Trans_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Trans.insert(make_pair(Trans_Molecular.organ_code, Trans_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForCb()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSCBUP A, "
		"EVALUSYSTEM.DETAIL.DMSCB B,EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID=B.CB AND A.CB=B.CORRID AND "
		"A.TYPE=B.TYPE AND A.TYPE=1 AND A.ORGAN_CODE = B.ORGAN_CODE AND A.ORGAN_CODE = C.ORGAN_CODE "
		"GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Cb_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					Cb_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Cb_Molecular.dv = string(tempstr);
					break;
				case 2:
					Cb_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Cb.insert(make_pair(Cb_Molecular.dv, Cb_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Cb.find(Cb_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Cb.end()) {
				float count = itr_Molecular->second;
				count += Cb_Molecular.count;
				m_mOrganCode2Molecular_Cb[Cb_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Cb.insert(make_pair(Cb_Molecular.organ_code, Cb_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForDis()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSCBUP A, "
		"EVALUSYSTEM.DETAIL.DMSCB B,EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID=B.CB AND A.CB=B.CORRID "
		"AND A.TYPE=B.TYPE AND A.TYPE=0 AND A.ORGAN_CODE = B.ORGAN_CODE AND A.ORGAN_CODE = C.ORGAN_CODE "
		"GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Dis_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					Dis_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Dis_Molecular.dv = string(tempstr);
					break;
				case 2:
					Dis_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Dis.insert(make_pair(Dis_Molecular.dv, Dis_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Dis.find(Dis_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Dis.end()) {
				float count = itr_Molecular->second;
				count += Dis_Molecular.count;
				m_mOrganCode2Molecular_Dis[Dis_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Dis.insert(make_pair(Dis_Molecular.organ_code, Dis_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForSubs()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	//站房匹配数
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSSTUP A, "
		"EVALUSYSTEM.DETAIL.DMSST B,EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID=B.DEVID AND "
		"A.DEVID=B.CORRID AND A.ORGAN_CODE=B.ORGAN_CODE "
		"AND A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Subs_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Subs_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Subs_Molecular.dv = string(tempstr);
					break;
				case 2:
					Subs_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Subs.insert(make_pair(Subs_Molecular.dv, Subs_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Subs.find(Subs_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Subs.end()) {
				float count = itr_Molecular->second;
				count += Subs_Molecular.count;
				m_mOrganCode2Molecular_Subs[Subs_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Subs.insert(make_pair(Subs_Molecular.organ_code, Subs_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForBus()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	//母线匹配数
	sprintf(sql, "SELECT A.ORGAN_CODE, C.DV, COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSBUSUP A,"
		"EVALUSYSTEM.DETAIL.DMSBUS B, EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID = B.DEVID AND "
		"A.DEVID = B.CORRID AND A.ORGAN_CODE = B.ORGAN_CODE "
		"AND A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE, C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Bus_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Bus_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Bus_Molecular.dv = string(tempstr);
					break;
				case 2:
					Bus_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Bus.insert(make_pair(Bus_Molecular.dv, Bus_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Bus.find(Bus_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Bus.end()) {
				float count = itr_Molecular->second;
				count += Bus_Molecular.count;
				m_mOrganCode2Molecular_Bus[Bus_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Bus.insert(make_pair(Bus_Molecular.organ_code, Bus_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetMolecularByDayForStatus()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Molecular;

	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM EVALUSYSTEM.DETAIL.DMSCBUP A, EVALUSYSTEM.DETAIL.DMSCB "
		"B, EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.PREDEVID = B.CB AND A.CB = B.CORRID AND A.STATUS = B.STATUS AND " 
		"A.ORGAN_CODE = B.ORGAN_CODE AND A.FLAG = 1 AND C.ORGAN_CODE = A.ORGAN_CODE GROUP BY A.ORGAN_CODE, C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Status_Molecular;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Status_Molecular.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Status_Molecular.dv = string(tempstr);
					break;
				case 2:
					Status_Molecular.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Molecular_Status.insert(make_pair(Status_Molecular.dv, Status_Molecular));

			itr_Molecular = m_mOrganCode2Molecular_Status.find(Status_Molecular.organ_code);
			if (itr_Molecular != m_mOrganCode2Molecular_Status.end()) {
				float count = itr_Molecular->second;
				count += Status_Molecular.count;
				m_mOrganCode2Molecular_Status[Status_Molecular.organ_code] = count;
			}
			else {
				m_mOrganCode2Molecular_Status.insert(make_pair(Status_Molecular.organ_code, Status_Molecular.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDay()
{
	if ((GetDenominatorByDayForTrans())
		||
		(GetDenominatorByDayForCb())
		||
		(GetDenominatorByDayForDis())
		||
		(GetDenominatorByDayForBus())
		||
		(GetDenominatorByDayForSubs())
		||
		(GetDenominatorByDayForStatus())) {
		printf("GIS1.5 || GIS1.6 getDenominator fail\n");
		return -1;
	} // 

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForTrans()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

#if 0
	sprintf(sql, "SELECT A.ORGAN_CODE,B.DV,COUNT(*) FROM (SELECT ORGAN_CODE,PREDEVID,LD FROM EVALUSYSTEM.DETAIL.DMSLDUP "
		"UNION SELECT ORGAN_CODE,LD,CORRID FROM EVALUSYSTEM.DETAIL.DMSLD) A, EVALUSYSTEM.CONFIG.ORGANDV B WHERE "
		"A.ORGAN_CODE = B.ORGAN_CODE GROUP BY A.ORGAN_CODE,B.DV;");
#endif
	/* // @detail 以DMS基于GIS1.5数据为准 */
	sprintf(sql, "SELECT A.ORGAN_CODE,B.DV,COUNT(*) FROM (SELECT ORGAN_CODE,LD,CORRID FROM EVALUSYSTEM.DETAIL.DMSLD) A, "
		"EVALUSYSTEM.CONFIG.ORGANDV B WHERE "
		"A.ORGAN_CODE = B.ORGAN_CODE GROUP BY A.ORGAN_CODE,B.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Trans_Denominator;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Trans_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Trans_Denominator.dv = string(tempstr);
					break;
				case 2:
					Trans_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Trans.insert(make_pair(Trans_Denominator.dv, Trans_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Trans.find(Trans_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Trans.end()) {
				float count = itr_Denominator->second;
				count += Trans_Denominator.count;
				m_mOrganCode2Denominator_Trans[Trans_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Trans.insert(make_pair(Trans_Denominator.organ_code, Trans_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForCb()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

#if 0
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,PREDEVID,CB FROM EVALUSYSTEM.DETAIL.DMSCBUP "
		"WHERE TYPE = 1 UNION SELECT ORGAN_CODE,CB,CORRID FROM EVALUSYSTEM.DETAIL.DMSCB "
		"WHERE TYPE=1) A,EVALUSYSTEM.CONFIG.ORGANDV C "
		"WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");
#endif
	/* // @detail 以DMS基于GIS1.5数据为准 */
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,CB,CORRID FROM EVALUSYSTEM.DETAIL.DMSCB "
		"WHERE TYPE=1) A,EVALUSYSTEM.CONFIG.ORGANDV C "
		"WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Cb_Denominator;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Cb_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Cb_Denominator.dv = string(tempstr);
					break;
				case 2:
					Cb_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Cb.insert(make_pair(Cb_Denominator.dv, Cb_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Cb.find(Cb_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Cb.end()) {
				float count = itr_Denominator->second;
				count += Cb_Denominator.count;
				m_mOrganCode2Denominator_Cb[Cb_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Cb.insert(make_pair(Cb_Denominator.organ_code, Cb_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForDis()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

#if 0
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,PREDEVID,CB FROM EVALUSYSTEM.DETAIL.DMSCBUP "
		"WHERE TYPE=0 UNION SELECT ORGAN_CODE,CB,CORRID FROM EVALUSYSTEM.DETAIL.DMSCB "
		"WHERE TYPE=0) A,EVALUSYSTEM.CONFIG.ORGANDV C "
		"WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV");
#endif
	/* // @detail 以DMS基于GIS1.5数据为准 */
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,CB,CORRID FROM EVALUSYSTEM.DETAIL.DMSCB "
		"WHERE TYPE=0) A,EVALUSYSTEM.CONFIG.ORGANDV C "
		"WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Dis_Denominator;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Dis_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Dis_Denominator.dv = string(tempstr);
					break;
				case 2:
					Dis_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Dis.insert(make_pair(Dis_Denominator.dv, Dis_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Dis.find(Dis_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Dis.end()) {
				float count = itr_Denominator->second;
				count += Dis_Denominator.count;
				m_mOrganCode2Denominator_Dis[Dis_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Dis.insert(make_pair(Dis_Denominator.organ_code, Dis_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForSubs()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

	//站房总数
#if 0
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,PREDEVID,DEVID FROM "
		"EVALUSYSTEM.DETAIL.DMSSTUP UNION SELECT ORGAN_CODE,DEVID,CORRID FROM EVALUSYSTEM.DETAIL.DMSST) A"
		",EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");
#endif
	/* // @detail 以DMS基于GIS1.5数据为准 */
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,DEVID,CORRID FROM EVALUSYSTEM.DETAIL.DMSST) A"
		",EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Subs_Denominator;;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++)
		{
			for (int col = 0; col < col_num; col++)
			{
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					Subs_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Subs_Denominator.dv = string(tempstr);
					break;
				case 2:
					Subs_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Subs.insert(make_pair(Subs_Denominator.dv, Subs_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Subs.find(Subs_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Subs.end()) {
				float count = itr_Denominator->second;
				count += Subs_Denominator.count;
				m_mOrganCode2Denominator_Subs[Subs_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Subs.insert(make_pair(Subs_Denominator.organ_code, Subs_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForBus()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

#if 0
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE,PREDEVID,DEVID FROM "
		"EVALUSYSTEM.DETAIL.DMSBUSUP UNION SELECT ORGAN_CODE, DEVID,CORRID FROM EVALUSYSTEM.DETAIL.DMSBUS) A"
		",EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");
#endif
	/* // @detail 以DMS基于GIS1.5数据为准 */
	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT ORGAN_CODE, DEVID,CORRID FROM EVALUSYSTEM.DETAIL.DMSBUS) A"
		",EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.ORGAN_CODE = C.ORGAN_CODE GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Bus_Denominator;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Bus_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Bus_Denominator.dv = string(tempstr);
					break;
				case 2:
					Bus_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Bus.insert(make_pair(Bus_Denominator.dv, Bus_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Bus.find(Bus_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Bus.end()) {
				float count = itr_Denominator->second;
				count += Bus_Denominator.count;
				m_mOrganCode2Denominator_Bus[Bus_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Bus.insert(make_pair(Bus_Denominator.organ_code, Bus_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetDenominatorByDayForStatus()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	map<int, float>::iterator itr_Denominator;

	sprintf(sql, "SELECT A.ORGAN_CODE,C.DV,COUNT(*) FROM (SELECT A.ORGAN_CODE,A.PREDEVID,A.CB FROM "
		"EVALUSYSTEM.DETAIL.DMSCBUP A,EVALUSYSTEM.DETAIL.DMSCB B WHERE A.ORGAN_CODE=B.ORGAN_CODE AND "
		"A.PREDEVID=B.CB AND A.CB=B.CORRID) A,EVALUSYSTEM.CONFIG.ORGANDV C WHERE A.ORGAN_CODE = C.ORGAN_CODE "
		"GROUP BY A.ORGAN_CODE,C.DV;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DATIMES Status_Denominator;
		char *tempstr = (char *)malloc(300);

		for (int rec = 0; rec < rec_num; rec++) {

			for (int col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					Status_Denominator.organ_code = *(int *)(tempstr);
					break;
				case 1:
					Status_Denominator.dv = string(tempstr);
					break;
				case 2:
					Status_Denominator.count = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganDv2Denominator_Status.insert(make_pair(Status_Denominator.dv, Status_Denominator));

			itr_Denominator = m_mOrganCode2Denominator_Status.find(Status_Denominator.organ_code);
			if (itr_Denominator != m_mOrganCode2Denominator_Status.end()) {
				float count = itr_Denominator->second;
				count += Status_Denominator.count;
				m_mOrganCode2Denominator_Status[Status_Denominator.organ_code] = count;
			}
			else {
				m_mOrganCode2Denominator_Status.insert(make_pair(Status_Denominator.organ_code, Status_Denominator.count));
			}
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int EquipmentUpgrade::GetRateByDay()
{
	map<string, ES_DATIMES>::iterator itr_MolecularRate;
	map<string, ES_DATIMES>::iterator itr_2DenominatorRate;
	map<int, float>::iterator itr_MolecularCount;
	map<int, float>::iterator itr_2DenominatorCount;

	/// @detail 配变 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Trans.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Trans.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Trans.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Trans.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Trans.insert(make_pair(DIM_DMS, es_result));

		GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Trans.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Trans.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Trans.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Trans.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Trans.insert(make_pair(DIM_LINE, es_result));
	}

	/// @detail 开关 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Cb.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Cb.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Cb.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Cb.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Cb.insert(make_pair(DIM_DMS, es_result));

		GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Cb.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Cb.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Cb.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Cb.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Cb.insert(make_pair(DIM_LINE, es_result));
	}

	/// @detail 刀闸 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Dis.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Dis.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Dis.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Dis.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Dis.insert(make_pair(DIM_DMS, es_result));

		GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Dis.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Dis.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Dis.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Dis.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Dis.insert(make_pair(DIM_LINE, es_result));
	}

	/// @detail 站房 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Subs.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Subs.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Subs.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Subs.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Subs.insert(make_pair(DIM_DMS, es_result));

		GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Subs.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Subs.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Subs.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Subs.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Subs.insert(make_pair(DIM_LINE, es_result));
	}

	/// @detail 母线 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Bus.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Bus.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Bus.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Bus.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Bus.insert(make_pair(DIM_DMS, es_result));

		GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Bus.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Bus.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Bus.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Bus.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Bus.insert(make_pair(DIM_LINE, es_result));
	}

	/// @detail 状态 */
	//按主站
	itr_2DenominatorCount = m_mOrganCode2Denominator_Status.begin();
	for (; itr_2DenominatorCount != m_mOrganCode2Denominator_Status.end(); itr_2DenominatorCount++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_2DenominatorCount->first;

		itr_MolecularCount = m_mOrganCode2Molecular_Status.find(itr_2DenominatorCount->first);
		if (itr_MolecularCount != m_mOrganCode2Molecular_Status.end()) {

			if (itr_MolecularCount->second <= itr_2DenominatorCount->second)
				es_result.m_fresult = itr_MolecularCount->second / itr_2DenominatorCount->second;
			else
				es_result.m_fresult = itr_2DenominatorCount->second / itr_MolecularCount->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Status.insert(make_pair(DIM_DMS, es_result));

		//GetAverageRate(es_result.m_ntype, es_result.m_fresult);
	}

	//按馈线
	itr_2DenominatorRate = m_mOrganDv2Denominator_Status.begin();
	for (; itr_2DenominatorRate != m_mOrganDv2Denominator_Status.end(); itr_2DenominatorRate++) {
		ES_RESULT es_result;
		es_result.m_strtype = itr_2DenominatorRate->first;
		es_result.m_ntype = itr_2DenominatorRate->second.organ_code;

		itr_MolecularRate = m_mOrganDv2Molecular_Status.find(itr_2DenominatorRate->first);
		if (itr_MolecularRate != m_mOrganDv2Molecular_Status.end()) {

			if (itr_MolecularRate->second.count <= itr_2DenominatorRate->second.count)
				es_result.m_fresult = itr_MolecularRate->second.count / itr_2DenominatorRate->second.count;
			else
				es_result.m_fresult = itr_2DenominatorRate->second.count / itr_MolecularRate->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2Result_Status.insert(make_pair(DIM_LINE, es_result));
	}

	return 0;
}

int EquipmentUpgrade::GetRateByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char tmp_name[MAX_NAME_LEN];
	string table_name;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_MONTH, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);

	if (tmp_byear != byear || tmp_bmon != bmonth) {
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}

	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='dmsbreakersup' "
		"or code='pmsbreakersup' or code='dmsdissup' or code='pmsdissup' or code='dmsldsup' or code='pmsldsup' "
		"or code='dmsstsup' or code='pmsstsup' or code='dmsbusup' or code='dmsbussup' or code='pmsbussup' "
		"or code='dmszwnumup' or code='pmszenumup' or code='devictbrighup') and dimname='无' group by code,organ_code,datatype",
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='breakfineup' or "
		"code='disfineup' or code='devictbup' or code='ldfineup' or code='busfineup' "
		"or code='substionfineup' or code='devfineup') and dimname='无'  and datatype != '数值' "
		"group by code,organ_code,datatype",
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	return 0;
}

int EquipmentUpgrade::InsertResult(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth, lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];
	std::multimap<int, ES_RESULT>::iterator itr_rate;
	std::map<int, float>::iterator itr_avg;
	std::map<int, float>::iterator itr_molecular;
	std::map<int, float>::iterator itr_denominator;

	col_num = 6;
	//英文标识
	strcpy(col_attr[0].col_name, "CODE");
	col_attr[0].data_type = DCI_STR;
	col_attr[0].data_size = 20;

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_CODE");
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);

	//数据值
	strcpy(col_attr[2].col_name, "VALUE");
	col_attr[2].data_type = DCI_FLT;
	col_attr[2].data_size = sizeof(float);

	//数据单位
	strcpy(col_attr[3].col_name, "DATATYPE");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 10;

	//分解维度名称
	strcpy(col_attr[4].col_name, "DIMNAME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 10;

	//分解维度取值
	strcpy(col_attr[5].col_name, "DIMVALUE");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	for (int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if (statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d", year, month, day);
	else
		sprintf(table_name, "month_%04d%02d", year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	/// @detail 配变按主站 */
	rec_num = m_mOrganCode2Result_Trans.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Trans.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "ldfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 配变分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Trans.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Trans.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "dmsldsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 配变分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Trans.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Trans.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "pmsldsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关按主站 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Cb.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Cb.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "breakfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Cb.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Cb.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Cb.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "dmsbreakersup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Cb.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Cb.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Cb.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "pmsbreakersup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸按主站 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Dis.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Dis.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "disfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Dis.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Dis.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Dis.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "dmsdissup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Dis.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Dis.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Dis.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "pmsdissup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 母线按主站 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Bus.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Bus.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "busfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 母线分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Bus.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Bus.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Bus.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "dmsbussup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 母线分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Bus.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Bus.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Bus.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "pmsbussup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 站房按主站 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Subs.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Subs.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "substionfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 站房分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Subs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Subs.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Subs.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "dmsstsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 站房分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Subs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Subs.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Subs.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "pmsstsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态按主站 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Status.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Status.find(DIM_DMS);
	for (int i = 0; i < rec_num;++i) {
		memcpy(m_pdata + offset, "devictbup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态分子 */
	offset = 0;
	rec_num = m_mOrganCode2Molecular_Status.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_molecular = m_mOrganCode2Molecular_Status.begin();
	for (; itr_molecular != m_mOrganCode2Molecular_Status.end(); itr_molecular++) {
		memcpy(m_pdata + offset, "devictbrighup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_molecular->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_molecular->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态分母 */
	offset = 0;
	rec_num = m_mOrganCode2Denominator_Status.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_denominator = m_mOrganCode2Denominator_Status.begin();
	for (; itr_denominator != m_mOrganCode2Denominator_Status.end(); itr_denominator++) {
		memcpy(m_pdata + offset, "dmszwnumup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_denominator->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_denominator->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 平均正确率按主站 */
	offset = 0;
	rec_num = m_mOrganCode2AvgRate.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_avg = m_mOrganCode2AvgRate.begin();
	for (; itr_avg != m_mOrganCode2AvgRate.end();++itr_avg) {
		memcpy(m_pdata + offset, "devfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_avg->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_avg->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @brief 按馈线 */
	map<string, ES_DATIMES>::iterator itr_feeder_molecular;
	map<string, ES_DATIMES>::iterator itr_feeder_denominator;
	/// @detail 配变按馈线 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Trans.count(DIM_LINE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Trans.find(DIM_LINE);
	for (int i = 0; i < rec_num; ++i) {
		memcpy(m_pdata + offset, "ldfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_rate->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 配变分子 */
	offset = 0;
	rec_num = m_mOrganDv2Molecular_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_molecular = m_mOrganDv2Molecular_Trans.begin();
	for (; itr_feeder_molecular != m_mOrganDv2Molecular_Trans.end(); itr_feeder_molecular++) {
		memcpy(m_pdata + offset, "dmsldsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_molecular->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 配变分母 */
	offset = 0;
	rec_num = m_mOrganDv2Denominator_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_denominator = m_mOrganDv2Denominator_Trans.begin();
	for (; itr_feeder_denominator != m_mOrganDv2Denominator_Trans.end(); itr_feeder_denominator++) {
		memcpy(m_pdata + offset, "pmsldsup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_denominator->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关按馈线 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Cb.count(DIM_LINE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Cb.find(DIM_LINE);
	for (int i = 0; i < rec_num; ++i) {
		memcpy(m_pdata + offset, "breakfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_rate->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关分子 */
	offset = 0;
	rec_num = m_mOrganDv2Molecular_Cb.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_molecular = m_mOrganDv2Molecular_Cb.begin();
	for (; itr_feeder_molecular != m_mOrganDv2Molecular_Cb.end(); itr_feeder_molecular++) {
		memcpy(m_pdata + offset, "dmsbreakersup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_molecular->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 开关分母 */
	offset = 0;
	rec_num = m_mOrganDv2Denominator_Cb.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_denominator = m_mOrganDv2Denominator_Cb.begin();
	for (; itr_feeder_denominator != m_mOrganDv2Denominator_Cb.end(); itr_feeder_denominator++) {
		memcpy(m_pdata + offset, "pmsbreakersup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_denominator->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸按馈线 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Dis.count(DIM_LINE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Dis.find(DIM_LINE);
	for (int i = 0; i < rec_num; ++i) {
		memcpy(m_pdata + offset, "disfineup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_rate->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸分子 */
	offset = 0;
	rec_num = m_mOrganDv2Denominator_Dis.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_molecular = m_mOrganDv2Denominator_Dis.begin();
	for (; itr_feeder_molecular != m_mOrganDv2Denominator_Dis.end(); itr_feeder_molecular++) {
		memcpy(m_pdata + offset, "dmsdissup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_molecular->first.c_str() , col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 刀闸分母 */
	offset = 0;
	rec_num = m_mOrganDv2Denominator_Dis.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_denominator = m_mOrganDv2Denominator_Dis.begin();
	for (; itr_feeder_denominator != m_mOrganDv2Denominator_Dis.end(); itr_feeder_denominator++) {
		memcpy(m_pdata + offset, "pmsdissup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_denominator->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态按馈线 */
	offset = 0;
	rec_num = m_mOrganCode2Result_Status.count(DIM_LINE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_rate = m_mOrganCode2Result_Status.find(DIM_LINE);
	for (int i = 0; i < rec_num; ++i) {
		memcpy(m_pdata + offset, "devictbup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_rate->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_rate->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		++itr_rate;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态分子 */
	offset = 0;
	rec_num = m_mOrganDv2Molecular_Status.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_molecular = m_mOrganDv2Molecular_Status.begin();
	for (; itr_feeder_molecular != m_mOrganDv2Molecular_Status.end(); itr_feeder_molecular++) {
		memcpy(m_pdata + offset, "devictbrighup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_molecular->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_molecular->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	/// @detail 状态分母 */
	offset = 0;
	rec_num = m_mOrganDv2Denominator_Status.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	itr_feeder_denominator = m_mOrganDv2Denominator_Status.begin();
	for (; itr_feeder_denominator != m_mOrganDv2Denominator_Status.end(); itr_feeder_denominator++) {
		memcpy(m_pdata + offset, "dmszwnumup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_feeder_denominator->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_feeder_denominator->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	return 0;
}

int EquipmentUpgrade::GenerateDetailByProcedure(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret;
	char sql[MAX_SQL_LEN];

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	//删除当日数据防止重复(明细数据)
	sprintf(sql, "call evalusystem.result.DELETEDATA_UP('%04d-%02d-%02d')", byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

#if 0
	sprintf(sql, "call evalusystem.result.DGPMSPROCESS_UP('%04d%02d%02d')", byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}
#endif
	if (GetUpdateWrongDetail(es_time) != 0) {
		return -1;
	} // 

	return 0;
}

int EquipmentUpgrade::DeleteDevRepetitionByProcedure()
{
	int ret;
	char sql[MAX_SQL_LEN];

	//删除当日数据防止重复(明细数据)
	sprintf(sql, "call evalusystem.detail.CHECK_DMS_MODEL()");

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

#if 0
	/// @detail 执行到DMSCBUP去重时,数据库就会崩溃(暂时去除),由入库时实现去重复 */
	sprintf(sql, "call evalusystem.detail.CHECK_DMSUP_MODEL()");

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}
#endif

	return 0;
}

int EquipmentUpgrade::GetAverageRate(int organ_code,float result)
{
	map<int, float>::iterator it = m_mOrganCode2AvgRate.find(organ_code);
	if (it != m_mOrganCode2AvgRate.end()) {
		it->second += result / 5;
	}
	else {
		m_mOrganCode2AvgRate.insert(make_pair(organ_code, result / 5));
	}

	return 0;
}

int EquipmentUpgrade::GetCorrForDevUpByProcedure()
{
	int ret;
	char sql[MAX_SQL_LEN];

	//插入1.5数据对应1.6ID与1.6数据对应1.6ID
	sprintf(sql, "call evalusystem.detail.INSERT_CORR_DEVUP()");

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	return 0;
}

int EquipmentUpgrade::GetUpdateWrongDetail(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret;
	char sql[MAX_SQL_LEN];

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	/* // @detail 开关不一致 */
	sprintf(sql, "insert into evalusystem.result.cbwzup(organ_code,dv,devid,devidup,type,devname,reason,count_time,updatetime) "
		"( select detail.organ_code,organdv.dv,	detail.predevid,detail.cb,detail.type,detail.name,detail.tra,"
		"to_date('%04d-%02d-%02d', 'YYYY-MM-DD'),sysdate from (select organ_code,"
		"dv,	cb,corrid predevid,type,name,'DMS系统升级后没有该记录或开关已删除' tra from evalusystem.detail.dmscb a "
		"where not exists (select * from evalusystem.detail.dmscbup b where a.cb = b.predevid and "
		"a.organ_code = b.organ_code and a.type = b.type ) ) detail,evalusystem.config.organdv organdv	where "
		"detail.organ_code = organdv.organ_code	); ", byear,bmonth,bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	/* // @detail 配变不一致 */
	sprintf(sql, "insert into evalusystem.result.ldwzup(organ_code,dv,devid,devidup,devname,reason,count_time,updatetime) " 
		"( select detail.organ_code,organdv.dv,detail.predevid,detail.ld,detail.name,detail.tra,"
		"to_date('%04d-%02d-%02d', 'YYYY-MM-DD'),sysdate from ( "
		"select organ_code,dv,ld,corrid predevid,name,'DMS系统升级后没有该记录或配变已删除' tra from evalusystem.detail.dmsld a "	
		"where not exists (select * from evalusystem.detail.dmsldup b where a.ld = b.predevid and b.oper != 2 and "
		"a.organ_code = b.organ_code) and a.oper != 2 ) detail,evalusystem.config.organdv organdv where "
		"detail.organ_code = organdv.organ_code); ", byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	/* // @detail 母线不一致 */
	sprintf(sql, "insert into evalusystem.result.buswzup(organ_code,dv,devid,devidup,devname,reason,count_time,updatetime) "
		"( select organ_code,dv,devid,corrid predevid,name,'DMS系统升级后没有该记录或母线已删除',to_date('%04d-%02d-%02d', 'YYYY-MM-DD'),"
		"sysdate from 	evalusystem.detail.dmsbus a where	not exists (select * from evalusystem.detail.dmsbusup b "
		"where a.devid = b.predevid and a.organ_code = b.organ_code and b.oper != 2) and a.oper != 2); ", 
		byear, bmonth, bday, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	/* // @detail 变电站不一致 */
	sprintf(sql, "insert into evalusystem.result.stwzup(organ_code,dv,devid,devidup,devname,reason,count_time,updatetime) "
		"( select organ_code,dv,devid,corrid,name,'DMS系统升级后没有该记录或站房已删除',	to_date('%04d-%02d-%02d', 'YYYY-MM-DD'),"
		"sysdate from evalusystem.detail.dmsst a where not exists (select	* from evalusystem.detail.dmsstup b "
		"where a.devid = b.predevid and a.organ_code = b.organ_code and b.oper != 2 ) and a.oper != 2 ); ",
		byear, bmonth, bday, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	return 0;
}

