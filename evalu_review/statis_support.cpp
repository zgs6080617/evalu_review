/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_support.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 辅助统计实现
 * History
 * ===============================================
 * 2015-01-28  guoqiuye create
 * ===============================================
*******************************************************************************/
#include "statis_support.h"
#include <iostream>
#include "datetime.h"

using namespace std;

StatisSupport::StatisSupport()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisSupport::~StatisSupport()
{
	m_oci->DisConnect(&err_info);
}

int StatisSupport::GetCorrective(ES_TIME *es_time, int _type)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret;
	char sql[MAX_SQL_LEN];

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if (_type == 0) {
		sprintf(sql, "call evalusystem.result.CORRECTIVEPROCESS('%04d-%02d-%02d')", byear, bmonth, bday);
	}
	else {
		sprintf(sql,"call evalusystem.result.CORRECTIVEPROCESS_WHITHOUT_TOPO('%04d-%02d-%02d')", byear, bmonth, bday);
	}
	//cout<<sql<<endl;
 	ret = m_oci->ExecSingle(sql,&err_info);
 	if(!ret) {
 		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
 		return -1;
 	}

	return 0;
}

int StatisSupport::GetCorrectRate(int statis_type, ES_TIME *es_time)
{
	int ret;

	Clear();

	if (statis_type == STATIS_DAY) {

		ret = GetDMSCorrectiveByDay(es_time);
		if (ret < 0) {
			printf("GetDMSCorrectiveByDay fail.\n");
			return -1;
		}

		ret = CaculateCorrectRateForDay(statis_type, es_time);
		if (ret < 0) {
			printf("CaculateCorrectRate fail.\n");
			return -1;
		} // 

		ret = InsertData(statis_type, es_time);
		if (ret < 0) {
			printf("InsertData fail.\n");
			return -1;
		} // 
	}
	else {
		ret = CaculateCorrectRateForMonth(statis_type,es_time);
		if (ret < 0) {
			printf("CaculateCorrectRateForMonth fail.\n");
			return -1;
		} // 
	}

	return 0;
}

int StatisSupport::InsertData(int statis_type, ES_TIME *es_time)
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
	std::multimap<int, ES_RESULT>::iterator itr_cor_result;

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

	//两周未整改率按主站
	rec_num = m_corrective_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cor_result = m_corrective_result.find(DIM_DMS);
	for (int i = 0; i < rec_num; i++) {
		memcpy(m_pdata + offset, "rectification", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cor_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cor_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_cor_result++;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			// 			plog->WriteLog(2, "配电站房数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else {
		// 		plog->WriteLog(1, "配电站房数完整率当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}

	FREE(m_pdata);

	//两周未整改设备个数
	offset = 0;
	rec_num = m_organ_corrective.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, ES_Corrective>::iterator itr_organ_judged = m_organ_corrective.begin();
	for (; itr_organ_judged != m_organ_corrective.end(); itr_organ_judged++) {
		memcpy(m_pdata + offset, "nrec", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judged->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judged->second.confailnum, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "DMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else {
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}

	FREE(m_pdata);

	//两周前错误设备总数
	offset = 0;
	rec_num = m_organ_corrective.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judged = m_organ_corrective.begin();
	for (; itr_organ_judged != m_organ_corrective.end(); itr_organ_judged++) {
		memcpy(m_pdata + offset, "wrongnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judged->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judged->second.failnum, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "GPMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else {
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

void StatisSupport::Clear()
{
	m_organ_corrective.clear();
	m_corrective_result.clear();
}

int StatisSupport::GetDMSCorrectiveByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	int top_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	int year, month, day;
	int byear, bmonth, bday;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	//未整改设备数
	sprintf(sql, "select organ.organ_code,decode(cro.newnum,null,0,cro.newnum),decode(cro.oldnum,null,0,cro.oldnum) from "
		"(select a.organ_code co,a.cb+a.dis+a.st+a.bus+a.ld+a.tz+a.topo newnum,b.cb+b.dis+b.st+b.bus+b.ld+b.tz+b.topo oldnum "
		"from evalusystem.result.CORRECTIVE a,evalusystem.result.CORRECTIVE_DENOMINATOR b where a.organ_code = b.organ_code and a.count_time = b.count_time"
		" and a.count_time = to_date('%04d-%02d-%02d','YYYY-MM-DD')) cro right join evalusystem.config.organ organ on(cro.co = organ.organ_code) where 1=1"
		"order by organ.organ_code;", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_Corrective dfail;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name, "/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSCBs_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout << "open file error" << endl;
#endif
		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col)
				{
				case 0:
					dfail.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dfail.confailnum = *(int *)(tempstr);
					break;
				case 2:
					dfail.failnum = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_corrective.insert(make_pair(dfail.organ_code, dfail));
#ifdef DEBUG
			outfile << ++line << "\torgan_code:" << dfail.organ_code << "\tconfailnum:" << dfail.confailnum << "\tfailnum:" << dfail.failnum << endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisSupport::CaculateCorrectRateForDay(int statis_type, ES_TIME *es_time)
{
	//两周指标整改率
	map<int, ES_Corrective>::iterator itr_organ_corrective;

	//按主站
	itr_organ_corrective = m_organ_corrective.begin();
	for (; itr_organ_corrective != m_organ_corrective.end(); itr_organ_corrective++) {
		ES_RESULT es_result;

		es_result.m_ntype = itr_organ_corrective->first;
		if (itr_organ_corrective->second.confailnum != 0 && itr_organ_corrective->second.failnum != 0) {
			es_result.m_fresult = 1 - (itr_organ_corrective->second.confailnum / itr_organ_corrective->second.failnum);
		}
		else if (itr_organ_corrective->second.confailnum == 0) {
			es_result.m_fresult = 1.0;
		}
		else {
			es_result.m_fresult = 0.0;
		}

		m_corrective_result.insert(make_pair(DIM_DMS, es_result));
	}

	return 0;
}

int StatisSupport::CaculateCorrectRateForMonth(int statis_type, ES_TIME *es_time)
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
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='nrec' and dimname='无' group by code,organ_code,datatype",
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='wrongnum' and dimname='无' group by "
		"code,organ_code,datatype", byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='rectification' "
		"and dimname='无'  and datatype != '数值' group by code,organ_code,datatype",
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql, &err_info);
	if (!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n", err_info.error_no, err_info.error_info, sql);
		return -1;
	}

	return 0;
}

