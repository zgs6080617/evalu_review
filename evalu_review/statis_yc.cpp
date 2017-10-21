/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_yc.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 用采相关统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <map>
#include "statis_yc.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisYC::StatisYC(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisYC::~StatisYC()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

int StatisYC::GetYCRate(int statis_type, ES_TIME *es_time)
{
	int ret;

	Clear();
	//GetWeight();

	if(statis_type == STATIS_DAY)
	{
		
#if 0
		ret = GetPpnumByDay(es_time);
		if(ret < 0)
		{
			printf("GetPpnumByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetTqnumByDay(es_time);
		if(ret < 0)
		{
			printf("GetTqnumByDay fail.\n");
			goto FAILED_RET;
		}

		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetYCsRate fail.\n");
			goto FAILED_RET;
		}
#endif
#if 0
		ret = GetYCRateByDay(es_time);
		if(ret < 0)
		{
			printf("GetYCRateByDay fail.\n");
			goto FAILED_RET;
		}
#endif
		ret = GetGzzsqRateByDay(es_time);
		if(ret < 0)
		{
			printf("GetGzzsqRateByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYcRateByDay(es_time);
		if(ret < 0)
		{
			printf("GetYXRateByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXRateByDay(es_time);
		if(ret < 0)
		{
			printf("GetYXRateByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetEMSRateByDay(es_time);
		if(ret < 0)
		{
			printf("GetEMSRateByDay fail.\n");
			goto FAILED_RET;
		}
		ret = InsertResultTZRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultTZRate fail.\n");
			goto FAILED_RET;
		}
		/*ret = InsertResultTdintimeRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = InsertResultDmstdpubRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultDmstdpubRate fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdIntimeNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdIntimeNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdconfirmNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdconfirmNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdTotalNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdTotalNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetDMSFailureNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetDMSFailureNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetGPMSFailureNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetGPMSFailureNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetGDMSJudgedFailureNum(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetGDMSJudgedFailureNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetJudgedNoUseDet(statis_type,es_time);
		if(ret < 0 )
		{
			printf("GetJudgedNoUseDet fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Indicator(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Trans(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Fa(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNumIntime(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNumIntime fail.\n");
			goto FAILED_RET;
		}
		ret = GetJudgedAll(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Indicator(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Trans(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Fa(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetYCsRate fail.\n");
			goto FAILED_RET;
		}
	}
	else
	{
		/*ret = GetYCMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;*/
#if 0
		ret = GetYCRateByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYCRateByMonth fail.\n");
			goto FAILED_RET;
		}
#endif
		ret = GetGzzsqRateByMonth(es_time);
		if(ret < 0)
		{
			printf("GetGzzsqRateByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYcRateByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYcRateByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXRateByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYXRateByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetEMSRateByMonth(es_time);
		if(ret < 0)
		{
			printf("GetEMSRateByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = InsertResultTZRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultTZRate fail.\n");
			goto FAILED_RET;
		}
		/*ret = InsertResultTdintimeRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = InsertResultDmstdpubRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultDmstdpubRate fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdIntimeNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdIntimeNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdconfirmNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdconfirmNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetTdTotalNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTdTotalNum fail.\n");
			goto FAILED_RET;
		}

		ret = GetDMSFailureNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetDMSFailureNum fail.\n");
			goto FAILED_RET;
		}

		ret = GetGPMSFailureNum(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetGPMSFailureNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetGDMSJudgedFailureNum(statis_type,es_time);
		if (ret < 0)
		{
			printf("GetGDMSJudgedFailureNum fail.\n");
			goto FAILED_RET;
		}

		ret = GetRightJudgedNum(statis_type,es_time);
		if (ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Indicator(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Trans(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNum_Fa(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNum fail.\n");
			goto FAILED_RET;
		}
		ret = GetRightJudgedNumIntime(statis_type,es_time);
		if(ret < 0)
		{
			printf("GetRightJudgedNumIntime fail.\n");
			goto FAILED_RET;
		}
		ret = GetJudgedAll(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Indicator(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Trans(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetJudgedAll_Fa(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;
		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetYCsRate fail.\n");
			goto FAILED_RET;
		}
	}

	ret = InsertResultTDRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultTDRate fail.\n");
		goto FAILED_RET;
	}

#if 0
	ret = InsertResultTDFailureRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultTDFailureRate fail.\n");
		goto FAILED_RET;
	}
#endif

	ret = InsertResultTDJudgedRate(statis_type,es_time);
	if(ret < 0)
	{
		printf("InsertResultTDJudgedRate fail.\n");
		goto FAILED_RET;
	}

	return 0;

FAILED_RET:
	ES_ERROR_INFO es_err;
	es_err.statis_type = STATIS_YC;
	es_err.es_time.year = es_time->year;
	es_err.es_time.month = es_time->month;
	es_err.es_time.day = es_time->day;
#ifdef DEBUG_DATE
	es_err.es_time.year = 2014;
	es_err.es_time.day = 14;
	es_err.es_time.month = 6;
#endif

	int num = m_type_err->count(statis_type);
	multimap<int, ES_ERROR_INFO>::iterator itr_type_err = m_type_err->find(statis_type);
	for (int i = 0; i < num; i++)
	{
		if(itr_type_err->second == es_err)
		{
			return -1;
		}

		itr_type_err++;
	}
	m_type_err->insert(make_pair(statis_type, es_err));
	return -1;
}

int StatisYC::GetYCProcess(ES_TIME *es_time)
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

	sprintf(sql, "call evalusystem.result.YCPROCESS('%04d%02d%02d 23:59:59','%04d%02d%02d 23:59:59')",
		byear, bmonth, bday, year, month, day);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "DMS与用采信息交互数据处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisYC::GetPpnumByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,count(tq) from (select d.organ_code,d.tq from evalusystem.detail.dmstq d,"
		"evalusystem.detail.yctq y where d.organ_code=y.organ_code and d.tq=y.tq and "
		"d.send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"d.send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"y.send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"y.send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')) group by organ_code",
		byear, bmonth, bday, year, month, day, byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Ppnum_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_ppnum.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采匹配总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetTqnumByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,total,sendnum from evalusystem.detail.tqnum where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')",
		byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Tqnum_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.total = *(int *)(tempstr);
					break;
				case 2:
					yc.sendnum = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_cjtotal.insert(make_pair(yc.organ_code, yc.total));
			m_organ_sendnum.insert(make_pair(yc.organ_code, yc.sendnum));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\tcjtotal:"<<yc.total<<"\tsendnum:"<<yc.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取采集点和发送点总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYCMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN];
	char tmp_sql[MAX_SQL_LEN];
	char tmp_name[MAX_NAME_LEN];
	string table_name;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_MONTH, year, month ,day, &byear, &bmonth, &bday);

	//int days = bday;//GetDaysInMonth(byear, bmonth);
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = days; i >= 1; i--)
	{
		sprintf(tmp_name, "day_%04d%02d%02d", byear, bmonth, i);
		// 		sprintf(tmp_sql,"select * from evalusystem.result.%s where code='linefine' or code='dmslines' "
		// 			"or code='pmslines' or code='breakfine' or code='dmsbreakers' or code='pmsbreakers' or "
		// 			"code='gpinfrigh' or code='gpinfdevs' or code='dmsgpnum' or code='pmsgpnum' or "
		// 			"code='allgpnum' or code='devictb' or code='devictbrigh' or code='dmszwnum' or "
		// 			"code='pmszenum' or code='allzwnum' or code='ldfine' or code='dmslds' or code='pmslds'",tmp_name);
		sprintf(tmp_sql,"select * from evalusystem.result.%s where code='tzmatch' or code='ppnum' or "
			"code='cjtotal' or code='tzsuccess' or code='sendnum'",tmp_name);

		ret = m_oci->ReadData(tmp_sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
		if(ret == 1)
		{
			if(rec_num > 0)
			{
				table_name = tmp_name;
				m_oci->FreeReadData(col_attr, col_num, buffer);
				break;
			}
			else
			{
				if(i == 1)
					table_name = "no_data";
				m_oci->FreeColAttrData(col_attr, col_num);
				continue;
			}
		}
		else
		{
			printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS与用采信息交互月统计失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			return -1;
		}		
	}

	if(table_name != "no_data")
	{
		// 		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,updatetime,"
		// 			"statistime,dimname,dimvalue) select code,organ_code,value,datatype,updatetime,statistime,dimname,"
		// 			"dimvalue from evalusystem.result.%s where code='linefine' or code='dmslines' or "
		// 			"code='pmslines' or code='breakfine' or code='dmsbreakers' or code='pmsbreakers' or code='gpinfrigh' "
		// 			"or code='gpinfdevs' or code='dmsgpnum' or code='pmsgpnum' or code='allgpnum' or code='devictb' or "
		// 			"code='devictbrigh' or code='dmszwnum' or code='pmszenum' or code='allzwnum' or code='ldfine' or "
		// 			"code='dmslds' or code='pmslds'", byear, bmonth, table_name.c_str());
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,updatetime,"
			"statistime,dimname,dimvalue) select code,organ_code,value,datatype,updatetime,statistime,dimname,"
			"dimvalue from evalusystem.result.%s where code='tzmatch' or code='ppnum' or code='cjtotal' or "
			"code='tzsuccess' or code='sendnum'", byear, bmonth, table_name.c_str());

		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
			// 			plog->WriteLog(2, "DMS与用采信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			return -1;
		}
	}

	return 0;
}

int StatisYC::GetYCsRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	map<string, float>::iterator itr_code_weight;

	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);
#if 0
	//台账匹配率
	map<int, float>::iterator itr_organ_ppnum;
	map<int, float>::iterator itr_organ_cjtotal;

	//按主站
	itr_organ_cjtotal = m_organ_cjtotal.begin();
	for(; itr_organ_cjtotal != m_organ_cjtotal.end(); itr_organ_cjtotal++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_cjtotal->first;
		itr_organ_ppnum = m_organ_ppnum.find(itr_organ_cjtotal->first);
		if(itr_organ_ppnum != m_organ_ppnum.end())
		{
			if(itr_organ_ppnum->second <= itr_organ_cjtotal->second)
				es_result.m_fresult = itr_organ_ppnum->second / itr_organ_cjtotal->second;
			else
				es_result.m_fresult = itr_organ_cjtotal->second / itr_organ_ppnum->second;
		}
		else
			es_result.m_fresult = 0;

		m_tzmatch_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	//数据成功率
	map<int, float>::iterator itr_organ_sendnum;

	//按主站
	itr_organ_ppnum = m_organ_ppnum.begin();
	for(; itr_organ_ppnum != m_organ_ppnum.end(); itr_organ_ppnum++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_ppnum->first;
		itr_organ_sendnum = m_organ_sendnum.find(itr_organ_ppnum->first);
		if(itr_organ_sendnum != m_organ_sendnum.end())
		{
			//80:15分钟一个点，去除0~4点，共80个点
			//3:三相
			float send_num = itr_organ_sendnum->second;
			float ppnum = itr_organ_ppnum->second * 80 * 3;
			if(send_num <= ppnum)
				es_result.m_fresult = send_num / ppnum;
			else
				es_result.m_fresult = ppnum / send_num;
		}
		else
			es_result.m_fresult = 0;

		m_tzsuccess_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}
#endif
	//停电信息发布及时率
	//按主站
	map<int, float>::iterator itr_organ_tdtotal = m_organ_tdtotal.begin();
	for(; itr_organ_tdtotal != m_organ_tdtotal.end(); itr_organ_tdtotal++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_tdtotal->first;
		map<int, float>::iterator itr_organ_intime = m_organ_intime.find(itr_organ_tdtotal->first);
		if(itr_organ_intime != m_organ_intime.end())
		{
			if(itr_organ_intime->second == 0 && itr_organ_tdtotal->second == 0)
			{
				es_result.m_fresult = 1.0;
			}
			else if(itr_organ_intime->second == 0 || itr_organ_tdtotal->second == 0)
			{
				es_result.m_fresult = 0.0;
			}
			else
				es_result.m_fresult = itr_organ_intime->second / itr_organ_tdtotal->second;
		}
		else
			es_result.m_fresult = 0;

		m_intime_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_tdtotal->first;
		itr_code_weight = m_code_weight.find("tdintime");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_intime_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//停电信息发布确认率
	//按主站
	itr_organ_tdtotal = m_organ_tdtotal.begin();
	for(; itr_organ_tdtotal != m_organ_tdtotal.end(); itr_organ_tdtotal++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_tdtotal->first;
		map<int, float>::iterator itr_organ_confirm = m_organ_confirm.find(itr_organ_tdtotal->first);
		if(itr_organ_confirm != m_organ_confirm.end())
		{
			if(itr_organ_confirm->second == 0 && itr_organ_tdtotal->second == 0)
			{
				es_result.m_fresult = 1.0;
			}
			else if(itr_organ_confirm->second == 0 || itr_organ_tdtotal->second == 0)
			{
				es_result.m_fresult = 0.0;
			}
			else
				es_result.m_fresult = itr_organ_confirm->second / itr_organ_tdtotal->second;
		}
		else
			es_result.m_fresult = 0;

		m_confirm_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_tdtotal->first;
		itr_code_weight = m_code_weight.find("tdconfirm");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_confirm_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//停电故障率
	//按主站
	map<int, float>::iterator itr_organ_gpmsfailure = m_organ_gpmsfailure.begin();
	for(; itr_organ_gpmsfailure != m_organ_gpmsfailure.end(); itr_organ_gpmsfailure++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_gpmsfailure->first;
		//map<int, float>::iterator itr_organ_dmsfailure = m_organ_dmsfailure.find(itr_organ_gpmsfailure->first);
		map<int, float>::iterator itr_organ_dmsfailure = m_organ_tdtotal.find(itr_organ_gpmsfailure->first);
		//if(itr_organ_dmsfailure != m_organ_dmsfailure.end())
		if(itr_organ_dmsfailure != m_organ_tdtotal.end())
		{
			if(itr_organ_gpmsfailure->second == 0 && itr_organ_dmsfailure->second == 0)
			{
				es_result.m_fresult = 1.0;
			}
			else if(itr_organ_gpmsfailure->second == 0 || itr_organ_dmsfailure->second == 0)
			{
				es_result.m_fresult = 0.0;
			}
			else
				es_result.m_fresult = itr_organ_dmsfailure->second / itr_organ_gpmsfailure->second;
		}
		else
			es_result.m_fresult = 0;

		m_failure_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_gpmsfailure->first;
		itr_code_weight = m_code_weight.find("tdfailure");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_failure_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//停电研判发布率
	//按主站
	map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse.begin();
	for(; itr_organ_nouseno != map_organ_judgedNoUse.end(); itr_organ_nouseno++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_nouseno->first;
		map<int, float>::iterator itr_organ_nouseno_intime = map_organ_judgedNoUse_intime.find(itr_organ_nouseno->first); //add by 20160107 for new tdjudged
		//cout << "organ_code:find:" << itr_organ_dmsjudged->first << endl;
		if(itr_organ_nouseno_intime != map_organ_judgedNoUse_intime.end())
		{
			if(itr_organ_nouseno->second == 0)
			{
				es_result.m_fresult = 1.0;
				//printf("organ_code:%d,tdjudged:%d\n",itr_organ_nouseno->first,1);
			}
			else
			{
				es_result.m_fresult =  itr_organ_nouseno_intime->second / itr_organ_nouseno->second;
				//change by 20160107 for new tdjudged
#ifdef _DEBUG
				printf("organ_code:%d,tdjudged:%f,%f,%f\n",itr_organ_nouseno->first,es_result.m_fresult,itr_organ_nouseno_intime->second,itr_organ_nouseno->second);
#endif
			}

			
			//cout << "in:" << "\t1:" << itr_organ_nouseno->second << "\t2:" << itr_organ_dmsjudged->second << endl;
		}
		else
		{
			es_result.m_fresult = 0.0;
			//printf("organ_code:%d,tdjudged:%d\n",itr_organ_nouseno->first,0);
		}

		m_judged_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_dmsjudged->first;
		itr_code_weight = m_code_weight.find("tdjudged");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_judged_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}
	
	//停电研判正确率 lcm 20150805
	//按主站
	//itr_organ_nouseno = map_organ_judgedNoUse.begin();
	map<int, float>::iterator itr_organ_dmsjudgedsum = map_organ_judgedsum.begin();
	for(; itr_organ_dmsjudgedsum != map_organ_judgedsum.end(); itr_organ_dmsjudgedsum++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_dmsjudgedsum->first;
		map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse.find(itr_organ_dmsjudgedsum->first);
		if(itr_organ_nouseno != map_organ_judgedNoUse.end())
		{
			if (itr_organ_dmsjudgedsum->second == 0)
				es_result.m_fresult = 1.0;
			else
				es_result.m_fresult = itr_organ_nouseno->second/itr_organ_dmsjudgedsum->second;
#ifdef _DEBUG
			cout << "in:organ_code:" << itr_organ_dmsjudgedsum->first << "\tresult:" << es_result.m_fresult << "\t1:" << itr_organ_nouseno->second << "\t2:" << itr_organ_dmsjudgedsum->second << endl;
#endif
		}
		else
			es_result.m_fresult = 0.0;
		m_judgedright_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	itr_organ_dmsjudgedsum = map_organ_judgedsum_Indicator.begin();
	for(; itr_organ_dmsjudgedsum != map_organ_judgedsum_Indicator.end(); itr_organ_dmsjudgedsum++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_dmsjudgedsum->first;
		map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse_Indicator.find(itr_organ_dmsjudgedsum->first);
		if(itr_organ_nouseno != map_organ_judgedNoUse_Indicator.end())
		{
			if (itr_organ_dmsjudgedsum->second == 0)
				es_result.m_fresult = 1.0;
			else
				es_result.m_fresult = itr_organ_nouseno->second/itr_organ_dmsjudgedsum->second;
#ifdef _DEBUG
			cout << "in:organ_code:" << itr_organ_dmsjudgedsum->first << "\tresult:" << es_result.m_fresult << "\t1:" << itr_organ_nouseno->second << "\t2:" << itr_organ_dmsjudgedsum->second << endl;
#endif
		}
		else
			es_result.m_fresult = 0.0;
		m_judgedright_result_Indicator.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	itr_organ_dmsjudgedsum = map_organ_judgedsum_Trans.begin();
	for(; itr_organ_dmsjudgedsum != map_organ_judgedsum_Trans.end(); itr_organ_dmsjudgedsum++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_dmsjudgedsum->first;
		map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse_Trans.find(itr_organ_dmsjudgedsum->first);
		if(itr_organ_nouseno != map_organ_judgedNoUse_Trans.end())
		{
			if (itr_organ_dmsjudgedsum->second == 0)
				es_result.m_fresult = 1.0;
			else
				es_result.m_fresult = itr_organ_nouseno->second/itr_organ_dmsjudgedsum->second;
#ifdef _DEBUG
			cout << "in:organ_code:" << itr_organ_dmsjudgedsum->first << "\tresult:" << es_result.m_fresult << "\t1:" << itr_organ_nouseno->second << "\t2:" << itr_organ_dmsjudgedsum->second << endl;
#endif
		}
		else
			es_result.m_fresult = 0.0;
		m_judgedright_result_Trans.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	itr_organ_dmsjudgedsum = map_organ_judgedsum_Fa.begin();
	for(; itr_organ_dmsjudgedsum != map_organ_judgedsum_Fa.end(); itr_organ_dmsjudgedsum++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_dmsjudgedsum->first;
		map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse_Fa.find(itr_organ_dmsjudgedsum->first);
		if(itr_organ_nouseno != map_organ_judgedNoUse_Fa.end())
		{
			if (itr_organ_dmsjudgedsum->second == 0)
				es_result.m_fresult = 1.0;
			else
				es_result.m_fresult = itr_organ_nouseno->second/itr_organ_dmsjudgedsum->second;
#ifdef _DEBUG
			cout << "in:organ_code:" << itr_organ_dmsjudgedsum->first << "\tresult:" << es_result.m_fresult << "\t1:" << itr_organ_nouseno->second << "\t2:" << itr_organ_dmsjudgedsum->second << endl;
#endif
		}
		else
			es_result.m_fresult = 0.0;
		m_judgedright_result_Fa.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}
	
	return 0;
}

int StatisYC::InsertResultTZRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];
	std::map<int, float>::iterator itr_tzmatch_result;
	std::map<int, float>::iterator itr_tzsuccess_result;
	std::map<int, float>::iterator itr_xsd_result;
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

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//台账匹配率按主站
	#if 0
rec_num = m_tzmatch_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzmatch_result = m_tzmatch_result.begin();
	for(; itr_tzmatch_result != m_tzmatch_result.end(); itr_tzmatch_result++)
	{
		memcpy(m_pdata + offset, "tzmatch", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzmatch_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzmatch_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "台账匹配率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "台账匹配率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
#endif
	//得分
	/*offset = 0;
	rec_num = m_tzmatch_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzmatch_result = m_tzmatch_score.begin();
	for(; itr_tzmatch_result != m_tzmatch_score.end(); itr_tzmatch_result++)
	{
		memcpy(m_pdata + offset, "tzmatchs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzmatch_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzmatch_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "台账匹配率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "台账匹配率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/
#if 0
	//匹配点总数按主站
	offset = 0;
	rec_num = m_organ_ppnum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_ppnum = m_organ_ppnum.begin();
	for(; itr_organ_ppnum != m_organ_ppnum.end(); itr_organ_ppnum++)
	{
		memcpy(m_pdata + offset, "ppnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_ppnum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_ppnum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "匹配点总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "匹配点总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//采集点总数按主站
	offset = 0;
	rec_num = m_organ_cjtotal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_cjtotal = m_organ_cjtotal.begin();
	for(; itr_organ_cjtotal != m_organ_cjtotal.end(); itr_organ_cjtotal++)
	{
		memcpy(m_pdata + offset, "cjtotal", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_cjtotal->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_cjtotal->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "采集点总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "采集点总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
#endif
	//数据成功率按主站
#if 0
	offset = 0;
	rec_num = m_tzsuccess_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_tzsuccess_result.begin();
	for(; itr_tzsuccess_result != m_tzsuccess_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "tzsuccess", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//小水电采集成功率
	offset = 0;
	rec_num = m_xsd_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_xsd_result = m_xsd_result.begin();
	for(; itr_xsd_result != m_xsd_result.end(); itr_xsd_result++)
	{
		memcpy(m_pdata + offset, "xsdcltrate", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_xsd_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_xsd_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
	}
	FREE(m_pdata);
#endif
	//故障指示器覆盖率
	offset = 0;
	rec_num = m_cover_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_cover_result.begin();
	for(; itr_tzsuccess_result != m_cover_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "cover", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//二遥设备覆盖率
	offset = 0;
	rec_num = m_twocover_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_twocover_result.begin();
	for(; itr_tzsuccess_result != m_twocover_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "twocover", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//三遥设备覆盖率
	offset = 0;
	rec_num = m_threecover_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_threecover_result.begin();
	for(; itr_tzsuccess_result != m_threecover_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "threecover", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//故障指示器抖动率
	offset = 0;
	rec_num = m_shake_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_shake_result.begin();
	for(; itr_tzsuccess_result != m_shake_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "shake", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//故障指示器误动率
	offset = 0;
	rec_num = m_misoperation_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_misoperation_result.begin();
	for(; itr_tzsuccess_result != m_misoperation_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "misoperation", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//故障指示器漏报率
	offset = 0;
	rec_num = m_missreport_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_missreport_result.begin();
	for(; itr_tzsuccess_result != m_missreport_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "missreport", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥测数据正确率
	offset = 0;
	rec_num = m_yc_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_yc_result.begin();
	for(; itr_tzsuccess_result != m_yc_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "ycrt", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥信状态辨识正确率
	offset = 0;
	rec_num = m_yx_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_yx_result.begin();
	for(; itr_tzsuccess_result != m_yx_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "ztgj", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//EMS转发正确率
	offset = 0;
	rec_num = m_ems_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_ems_result.begin();
	for(; itr_tzsuccess_result != m_ems_result.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "emsrt", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_tzsuccess_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_tzsuccess_result = m_tzsuccess_score.begin();
	for(; itr_tzsuccess_result != m_tzsuccess_score.end(); itr_tzsuccess_result++)
	{
		memcpy(m_pdata + offset, "tzsuccesss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_tzsuccess_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "数据成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "数据成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/
#if 0
	//发送点总数按主站
	offset = 0;
	rec_num = m_organ_sendnum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_sendnum = m_organ_sendnum.begin();
	for(; itr_organ_sendnum != m_organ_sendnum.end(); itr_organ_sendnum++)
	{
		memcpy(m_pdata + offset, "sendnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_sendnum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_sendnum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "发送点总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "发送点总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
#endif

	return 0;
}

void StatisYC::Clear()
{
	m_organ_ppnum.clear();
	m_organ_cjtotal.clear();
	m_organ_sendnum.clear();
	m_tzmatch_result.clear();
	m_tzsuccess_result.clear();
	m_xsd_result.clear();
	m_organ_intime.clear();
	m_organ_confirm.clear();
	m_organ_tdtotal.clear();
	m_intime_result.clear();
	m_confirm_result.clear();
	m_organ_dmsfailure.clear();
	m_organ_gpmsfailure.clear();
	m_failure_result.clear();
	map_Suc_judged.clear();
	map_organ_judgednum.clear();
	map_organ_judgedNoUse.clear();
	map_organ_judgedNoUse_Indicator.clear();
	map_organ_judgedNoUse_Trans.clear();
	map_organ_judgedNoUse_Fa.clear();
	map_organ_judgedNoUse_intime.clear();
	m_judged_result.clear();
	map_judged.clear();
	m_judgedright_result.clear();
	m_judgedright_result_Indicator.clear();
	m_judgedright_result_Trans.clear();
	m_judgedright_result_Fa.clear();
	map_organ_judgedsum.clear();
	map_organ_judgedsum_Indicator.clear();
	map_organ_judgedsum_Trans.clear();
	map_organ_judgedsum_Fa.clear();

	m_cover_result.clear();
	m_shake_result.clear();
	m_misoperation_result.clear();
	m_missreport_result.clear();

	m_twocover_result.clear();
	m_threecover_result.clear();

	m_yc_result.clear();
	m_yx_result.clear();
	m_ems_result.clear();

	/*m_tzmatch_score.clear();
	m_tzsuccess_score.clear();
	m_intime_score.clear();
	m_confirm_score.clear();
	m_failure_score.clear();
	m_judged_score.clear();
	m_code_weight.clear();*/
}

int StatisYC::GetYCRateByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//float floattemp = 0.0;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ.organ_code,decode(a.tz,NULL,0,a.tz),decode(a.cj,NULL,0,a.cj),decode(a.xsd_rate,NULL,0,a.xsd_rate) from "
		"(select distinct organ_code,tz,cj,xsd_rate from evalusystem.detail.ycrate where "
		"count_time=to_date('%d-%d-%d 0:0:0','YYYY-MM-DD HH24:MI:SS')) a right join evalusystem.config.organ organ on(organ.organ_code = a.organ_code);", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					/*floattemp = *(double *)(tempstr);
					if (floattemp < 0.9) {
						tq.total = 0.99;
					}*/
					tq.total = *(double *)(tempstr);
					break;
				case 2:
					tq.sendnum = *(double *)(tempstr);
					break;
				case 3:
					tq.threenum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_tzmatch_result.insert(make_pair(tq.organ_code, tq.total));
			m_tzsuccess_result.insert(make_pair(tq.organ_code, tq.sendnum));
			m_xsd_result.insert(make_pair(tq.organ_code,tq.threenum));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.sendnum*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/


#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYCRateByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//因为设置了两个免考核标志位，因此需要将匹配率和成功率分开
	//台账匹配率
	sprintf(sql, "select organ_code,avg(tz) from evalusystem.detail.ycrate where " 
		"count_time like '%04d-%02d%%' and tzflag !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_tzmatch_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);


	//采集成功率
	//float floattemp = 0.0;
	/*sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from "
		"(select count(*) c, organ_code from ( (select organ_code from evalusystem.detail.ycrate where "
		"count_time like '%04d-%02d%%' and cjflag !=1  and cj < 0.9) union all "
		"(select organ_code from evalusystem.detail.ycrate where "
		"count_time like '%04d-%02d%%' and xsd_rateflag !=1  and xsd_rate < 0.95 ) group by organ_code) ) a "
		"right join config.organ organ on(a.organ_code = organ.organ_code)", byear, bmonth,byear,bmonth);*/ //结果中会少，不知是sql问题还是处理逻辑问题
	sprintf(sql,"select organ.organ_code,decode(a.c,NULL,0,a.c) from "
		"(select organ_code,count(*) c from detail.ycrate where cj < 0.9 and cjflag !=1 and count_time like '%04d-%02d%%' group by organ_code) a "
		"right join config.organ organ on(a.organ_code = organ.organ_code);",byear,bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					//floattemp = *(int*)(tempstr); // 每有一天低于0.9，减0.01

					tq.sendnum = *(int*)(tempstr)*0.01;
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map<int,float>::iterator it_tz = m_tzsuccess_result.find(tq.organ_code);
			if (it_tz != m_tzsuccess_result.end()) {

				it_tz->second -= tq.sendnum;
			}
			else {

				m_tzsuccess_result.insert(make_pair(tq.organ_code,(1-tq.sendnum)));
			}

			it_tz = m_xsd_result.find(tq.organ_code);
			if (it_tz != m_xsd_result.end()) {

				it_tz->second -= tq.sendnum;
			}
			else {

				m_xsd_result.insert(make_pair(tq.organ_code,(1-tq.sendnum)));
			}

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//小水电采集成功率
	sprintf(sql,"select organ.organ_code,decode(a.c,NULL,0,a.c) from "
		"(select organ_code,count(*) c from detail.ycrate where xsd_rate < 0.95 and xsd_rateflag !=1 and count_time like '%04d-%02d%%' group by organ_code) a "
		"right join config.organ organ on(a.organ_code = organ.organ_code);",byear,bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					//floattemp = *(int*)(tempstr); // 每有一天低于0.9，减0.01

					tq.sendnum = *(int*)(tempstr)*0.01;
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map<int,float>::iterator it_tz = m_tzsuccess_result.find(tq.organ_code);
			if (it_tz != m_tzsuccess_result.end()) {

				it_tz->second -= tq.sendnum;
			}
			else {

				m_tzsuccess_result.insert(make_pair(tq.organ_code,(1-tq.sendnum)));
			}

			it_tz = m_xsd_result.find(tq.organ_code);
			if (it_tz != m_xsd_result.end()) {

				it_tz->second -= tq.sendnum;
			}
			else {

				m_xsd_result.insert(make_pair(tq.organ_code,(1-tq.sendnum)));
			}

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}


int StatisYC::GetGzzsqRateByDay( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ.organ_code,decode(a.cover_rate,null,0,a.cover_rate),decode(a.shake_rate,null,0,a.shake_rate),"
		"decode(a.misoperation_rate,null,0,a.misoperation_rate),decode(a.missreport_rate,null,0,a.missreport_rate),"
		"decode(a.tworatiocover_rate,null,0,a.misoperation_rate),decode(a.threeratiocover_rate,null,0,a.missreport_rate) from "
		"(select organ_code,cover_rate,shake_rate,misoperation_rate,missreport_rate,tworatiocover_rate,threeratiocover_rate from evalusystem.detail.gzzsq where "
		"count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD HH24:MI:SS')) a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);"
		, byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_GZZSQ gzzsq_temp;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gzzsq_temp.organ_code = *(int *)(tempstr);
					break;
				case 1:
					gzzsq_temp.cover_rate = *(double *)(tempstr);
					break;
				case 2:
					gzzsq_temp.shake_rate = *(double *)(tempstr);
					break;
				case 3:
					gzzsq_temp.misoperation_rate = *(double *)(tempstr);
					break;
				case 4:
					gzzsq_temp.missreport_rate = *(double *)(tempstr);
					break;
				case 5:
					gzzsq_temp.tworatiocover_rate = *(double*)(tempstr);
					break;
				case 6:
					gzzsq_temp.threeratiocover_rate = *(double*)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_cover_result.insert(make_pair(gzzsq_temp.organ_code, gzzsq_temp.cover_rate));
			m_shake_result.insert(make_pair(gzzsq_temp.organ_code, gzzsq_temp.shake_rate));
			m_misoperation_result.insert(make_pair(gzzsq_temp.organ_code, gzzsq_temp.misoperation_rate));
			m_missreport_result.insert(make_pair(gzzsq_temp.organ_code, gzzsq_temp.missreport_rate));
			m_twocover_result.insert(make_pair(gzzsq_temp.organ_code,gzzsq_temp.tworatiocover_rate));
			m_threecover_result.insert(make_pair(gzzsq_temp.organ_code,gzzsq_temp.threeratiocover_rate));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.sendnum*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/


#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<gzzsq_temp.organ_code<<"\t:"<<gzzsq_temp.total<<"\t:"<<gzzsq_temp.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetGzzsqRateByMonth( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//故障指示器覆盖率
	sprintf(sql, "select organ_code,avg(COVER_RATE) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and cover_rateflag !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_cover_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);


	//二遥设备覆盖率
	sprintf(sql, "select organ_code,avg(TWORATIOCOVER_RATE) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and TWORATIOCOVER_RATEFLAG !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_twocover_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//三遥设备覆盖率
	sprintf(sql, "select organ_code,avg(THREERATIOCOVER_RATE) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and THREERATIOCOVER_RATEFLAG !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_threecover_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//故障指示器抖动率
	sprintf(sql, "select organ_code,avg(shake_rate) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and shake_rateflag !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.sendnum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_shake_result.insert(make_pair(tq.organ_code, tq.sendnum));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//故障指示器误动率
	sprintf(sql, "select organ_code,avg(misoperation_rate) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and misoperation_rateflag !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.sendnum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_misoperation_result.insert(make_pair(tq.organ_code, tq.sendnum));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//故障指示器漏报率
	sprintf(sql, "select organ_code,avg(missreport_rate) from evalusystem.detail.gzzsq where " 
		"count_time like '%04d-%02d%%' and missreport_rateflag !=1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.sendnum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_missreport_result.insert(make_pair(tq.organ_code, tq.sendnum));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYcRateByDay( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ.organ_code,a.yc_right from "
		"(select organ_code,yc_right from evalusystem.detail.ycrt where count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD HH24:MI:SS')) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_YCRT ycrt_temp;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					ycrt_temp.organ_code = *(int *)(tempstr);
					break;
				case 1:
					ycrt_temp.ycrt = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_yc_result.insert(make_pair(ycrt_temp.organ_code, ycrt_temp.ycrt));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.sendnum*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/


#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<ycrt_temp.organ_code<<"\t:"<<ycrt_temp.total<<"\t:"<<ycrt_temp.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYcRateByMonth( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//因为设置了两个免考核标志位，因此需要将匹配率和成功率分开
	//遥信状态辨识正确率
	sprintf(sql, "select organ.organ_code,a.av from "
		"(select organ_code,avg(yc_right) av from evalusystem.detail.ycrt where count_time like '%04d-%02d%%' and YC_RATEFLAG !=1 group by organ_code) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)",byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_yc_result.insert(make_pair(tq.organ_code, tq.total));
#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYXRateByDay( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ.organ_code,a.yx_rt from"
		"(select organ_code,yx_rt from evalusystem.detail.yxrt where count_time=to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS')) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)",byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_YCRT ycrt_temp;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					ycrt_temp.organ_code = *(int *)(tempstr);
					break;
				case 1:
					ycrt_temp.ycrt = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_yx_result.insert(make_pair(ycrt_temp.organ_code, ycrt_temp.ycrt));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<ycrt_temp.organ_code<<"\t:"<<ycrt_temp.total<<"\t:"<<ycrt_temp.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetYXRateByMonth( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//因为设置了两个免考核标志位，因此需要将匹配率和成功率分开
	//遥信状态辨识正确率
	sprintf(sql, "select organ.organ_code,a.yx from "
		"(select organ_code,avg(yx_rt) yx from evalusystem.detail.yxrt where count_time like '%04d-%02d%%' "
		"and yx_flag !=1 group by organ_code) a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_yx_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetEMSRateByDay( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ.organ_code,a.emszfrt from "
		"(select organ_code,emszfrt from evalusystem.detail.emsrt where count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD HH24:MI:SS')) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_YCRT ycrt_temp;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					ycrt_temp.organ_code = *(int *)(tempstr);
					break;
				case 1:
					ycrt_temp.ycrt = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_ems_result.insert(make_pair(ycrt_temp.organ_code, ycrt_temp.ycrt));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<ycrt_temp.organ_code<<"\t:"<<ycrt_temp.total<<"\t:"<<ycrt_temp.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetEMSRateByMonth( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	std::map<string, float>::iterator itr_code_weight;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//因为设置了两个免考核标志位，因此需要将匹配率和成功率分开
	//遥信状态辨识正确率
	sprintf(sql, "select organ.organ_code,a.ems from "
		"(select organ_code,avg(emszfrt) ems from evalusystem.detail.emsrt where "
		"count_time like '%04d-%02d%%' and emszfrtflag !=1 group by organ_code) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_TQ tq;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Rate_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					tq.organ_code = *(int *)(tempstr);
					break;
				case 1:
					tq.total = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_ems_result.insert(make_pair(tq.organ_code, tq.total));

			/*itr_code_weight = m_code_weight.find("tzmatch");
			if(itr_code_weight != m_code_weight.end())
				m_tzmatch_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzmatch_score.insert(make_pair(tq.organ_code,0));

			itr_code_weight = m_code_weight.find("tzsuccess");
			if(itr_code_weight != m_code_weight.end())
				m_tzsuccess_score.insert(make_pair(tq.organ_code,tq.total*itr_code_weight->second));
			else
				m_tzsuccess_score.insert(make_pair(tq.organ_code,0));*/

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<tq.organ_code<<"\t:"<<tq.total<<"\t:"<<tq.sendnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS和用采指标值失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

//未应用
int StatisYC::InsertResultTdintimeRate(int statis_type, ES_TIME *es_time)
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

	if(statis_type == STATIS_DAY)
		sprintf(sql, "insert into evalusystem.result.day_%04d%02d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'tdintime',a.organ_code,"
		"round(cast(a.num as float)/cast(b.num as float),4),'百分比',sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),"
		"'无','无' from (select organ_code,count(*) num from evalusystem.detail.tdinfo where "
		"datediff(minute,dmstd,ddaff)<=15 and send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmstd is not null and ddaff "
		"is not null group by organ_code) a,(select organ_code,count(*) num from evalusystem.detail.tdinfo where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code) b "
		"where a.organ_code=b.organ_code", byear, bmonth, bday, byear, bmonth, bday, byear, bmonth,
		bday, year, month, day, byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'tdintime',a.organ_code,"
		"round(cast(a.num as float)/cast(b.num as float),4),'百分比',sysdate,"
		"to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (select organ_code,count(*) num from "
		"evalusystem.detail.tdinfo where datediff(minute,dmstd,ddaff)<=15 and count_time like '%04d-%02d%%' "
		"and dmstd is not null and ddaff is not null group by organ_code) a,(select organ_code,count(*) num "
		"from evalusystem.detail.tdinfo where count_time like '%04d-%02d%%' group by organ_code) b where "
		"a.organ_code=b.organ_code", byear, bmonth, byear, bmonth, bday, byear, bmonth, byear, bmonth);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "停电信息发布及时率处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	//得分
	/*if(statis_type == STATIS_DAY)
		sprintf(sql, "insert into evalusystem.result.day_%04d%02d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'tdintimes',a.organ_code,"
		"round(cast(a.num as float)/cast(b.num as float),4),'数值',sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),"
		"'无','无' from (select organ_code,count(*) num from evalusystem.detail.tdinfo where "
		"datediff(minute,dmstd,ddaff)<=15 and send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmstd is not null and ddaff "
		"is not null group by organ_code) a,(select organ_code,count(*) num from evalusystem.detail.tdinfo where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code) b "
		"where a.organ_code=b.organ_code", byear, bmonth, bday, byear, bmonth, bday, byear, bmonth,
		bday, year, month, day, byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'tdintimes',a.organ_code,"
		"round(cast(a.num as float)/cast(b.num as float),4),'数值',sysdate,"
		"to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (select organ_code,count(*) num from "
		"evalusystem.detail.tdinfo where datediff(minute,dmstd,ddaff)<=15 and count_time like '%04d-%02d%%' "
		"and dmstd is not null and ddaff is not null group by organ_code) a,(select organ_code,count(*) num "
		"from evalusystem.detail.tdinfo where count_time like '%04d-%02d%%' group by organ_code) b where "
		"a.organ_code=b.organ_code", byear, bmonth, byear, bmonth, bday, byear, bmonth, byear, bmonth);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "停电信息发布及时率处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}*/
	return 0;
}

int StatisYC::InsertResultDmstdpubRate(int statis_type, ES_TIME *es_time)
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

	if(statis_type == STATIS_DAY)
		sprintf(sql, "insert into evalusystem.result.day_%04d%02d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'dmstdpub',organ_code,"
		"round(decode(tdtrans-wrongtrans,0,0,cast(reltrans as float)/cast((tdtrans-wrongtrans) as float)),4),'百分比',sysdate,"
		"to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from evalusystem.detail.dmstd where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')",
		byear, bmonth, bday, byear, bmonth, bday, byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select 'dmstdpub',"
		"a.organ_code,round(decode(total,0,0,cast(pub as float)/cast(total as float)),4),'百分比',sysdate,"
		"to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (select organ_code,sum(reltrans) pub "
		"from evalusystem.detail.dmstd where count_time like '%04d-%02d%%' group by organ_code) a,"
		"(select organ_code,sum(tdtrans-wrongtrans) total from evalusystem.detail.dmstd where "
		"count_time like '%04d-%02d%%' group by organ_code) b where a.organ_code=b.organ_code",
		byear, bmonth, byear, bmonth, bday, byear, bmonth, byear, bmonth);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "DMS停电信息发布率处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisYC::GetTdIntimeNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);
	/*
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"datediff(minute,dmstd,ddaff)<=15 and dmstd is not null and ddaff is not null and "
			"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and (sendtype != 2 or sendtype is null) group by organ_code", 
			byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo "
			"where datediff(minute,dmstd,ddaff)<=15 and count_time like '%04d-%02d%%' "
			"and dmstd is not null and ddaff is not null  and (sendtype != 2 or sendtype is null)  group by organ_code", byear, bmonth);
			*/
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(abc.num,NULL,0,abc.num) from "
			"(select a.organ_code,count(*) num from (select distinct(tdnum),organ_code from evalusystem.detail.tdinfo "
			"where datediff(minute,dmstd,ddaff)<=15 and dmstd is not null and ddaff is not null and "
			"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and (sendtype != 2 or sendtype is null)) a, "
			"(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
			"from evalusystem.detail.gpms_powercut_det where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmsid is not null) b where a.organ_code = b.organ_code and "
			"a.tdnum = b.dmsid group by a.organ_code ) abc right join evalusystem.config.organ organ on(organ.organ_code = abc.organ_code);",
			byear, bmonth, bday, year, month, day,byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select organ.organ_code,decode(abc.num,NULL,0,abc.num) from("
			"select a.organ_code,count(*) num from (select organ_code,tdnum from evalusystem.detail.tdinfo "
			"where datediff(minute,dmstd,ddaff)<=15 and count_time like '%04d-%02d%%' "
			"and dmstd is not null and ddaff is not null  and (sendtype != 2 or sendtype is null)) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
			"from evalusystem.detail.gpms_powercut_det where count_time like '%04d-%02d%%' and dmsid is not null and flag != 1) b where a.organ_code = b.organ_code and "
			"a.tdnum = b.dmsid group by a.organ_code) abc right join evalusystem.config.organ organ on(organ.organ_code = abc.organ_code);",
			byear, bmonth, byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Tdintime_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_intime.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取停电发布合格数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetTdconfirmNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"datediff(minute,dmstd,dmsff)<=15 and dmstd is not null and dmsff is not null and "
			"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and (sendtype != 2 or sendtype is null)  group by organ_code", 
			byear, bmonth, bday, year, month, day);//ddaff
	else
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"datediff(minute,dmstd,dmsff)<=15 and dmstd is not null and dmsff is not null "
			"and count_time like '%04d-%02d%%' and (sendtype != 2 or sendtype is null)  group by organ_code", byear, bmonth);//ddaff

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Tdconfirm_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_confirm.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取停电确认合格数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetTdTotalNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
	//为停电信息统计添加右连接，add by lcm 20150902
		/*
		sprintf(sql, "select a.organ_code,count(*) from (select distinct(tdnum),organ_code from evalusystem.detail.tdinfo "
			"where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and (sendtype != 2 or sendtype is null)) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
			"from evalusystem.detail.gpms_powercut_det where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmsid != '') b where a.organ_code = b.organ_code and "
			"a.tdnum = b.dmsid group by a.organ_code;", byear, bmonth, bday, year, month, day,byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select a.organ_code,count(*) from (select organ_code,tdnum from evalusystem.detail.tdinfo "
		"where count_time like '%04d-%02d%%' and (sendtype != 2 or sendtype is null) and flag != 1) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
		"from evalusystem.detail.gpms_powercut_det where count_time like '%04d-%02d%%' and dmsid != '' and flag != 1) b where a.organ_code = b.organ_code and "
		"a.tdnum = b.dmsid group by a.organ_code;", byear, bmonth, byear, bmonth);
		*/
		
		sprintf(sql,"select organ.organ_code,decode(abc.c,NULL,0,abc.c) from("
			"select a.organ_code,count(*) c from (select distinct(tdnum),organ_code from evalusystem.detail.tdinfo "
			"where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and (sendtype != 2 or sendtype is null)) a,(select distinct organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
			"from evalusystem.detail.gpms_powercut_det where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmsid is not null) b where a.organ_code = b.organ_code and "
			"a.tdnum = b.dmsid group by a.organ_code) abc right join evalusystem.config.organ organ on(organ.organ_code = abc.organ_code);", 
			byear, bmonth, bday, year, month, day,byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select organ.organ_code,decode(abc.c,NULL,0,abc.c) from("
		"select a.organ_code,count(*) c from (select distinct organ_code,tdnum from evalusystem.detail.tdinfo "
		"where count_time like '%04d-%02d%%' and (sendtype != 2 or sendtype is null) and flag != 1) a,(select distinct organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid "
		"from evalusystem.detail.gpms_powercut_det where count_time like '%04d-%02d%%' and dmsid is not null and flag != 1) b where a.organ_code = b.organ_code and "
		"a.tdnum = b.dmsid group by a.organ_code) abc right join evalusystem.config.organ organ on(organ.organ_code = abc.organ_code);",
		byear, bmonth, byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Tdtotal_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_tdtotal.insert(make_pair(yc.organ_code, yc.online));
#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取停电总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::InsertResultTDRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];
	std::map<int, float>::iterator itr_intime_result;
	std::map<int, float>::iterator itr_confirm_result;

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

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//停电信息发布及时率按主站
	rec_num = m_intime_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_intime_result = m_intime_result.begin();
	for(; itr_intime_result != m_intime_result.end(); itr_intime_result++)
	{
		memcpy(m_pdata + offset, "tdintime", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_intime_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_intime_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电信息发布及时率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电信息发布及时率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_intime_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_intime_result = m_intime_score.begin();
	for(; itr_intime_result != m_intime_score.end(); itr_intime_result++)
	{
		memcpy(m_pdata + offset, "tdintimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_intime_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_intime_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电信息发布及时率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电信息发布及时率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//停电信息发布确认率按主站
	offset = 0;
	rec_num = m_confirm_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_confirm_result = m_confirm_result.begin();
	for(; itr_confirm_result != m_confirm_result.end(); itr_confirm_result++)
	{
		memcpy(m_pdata + offset, "tdconfirm", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_confirm_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_confirm_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电信息发布确认率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电信息发布确认率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_confirm_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_confirm_result = m_confirm_score.begin();
	for(; itr_confirm_result != m_confirm_score.end(); itr_confirm_result++)
	{
		memcpy(m_pdata + offset, "tdconfirms", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_confirm_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_confirm_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电信息发布确认率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电信息发布确认率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	return 0;
}

int StatisYC::GetDMSFailureNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"count_time='%04d-%02d-%02d 00:00:00' and (sendtype != 2 or sendtype is null) group by organ_code", 
			byear, bmonth, bday);
	else
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"count_time like '%04d-%02d%%' and  (sendtype != 2 or sendtype is null) and flag != 1 group by organ_code", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DmsFailure_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_dmsfailure.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetGPMSFailureNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code,count(*) c from evalusystem.detail.gpms_powercut_det where "
			"count_time='%04d-%02d-%02d 00:00:00' group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.organ_code)",
			byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code,count(*) c from evalusystem.detail.gpms_powercut_det where "
			"count_time like '%04d-%02d%%' and flag != 1 group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.organ_code)",
			byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsFailure_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_gpmsfailure.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}


int StatisYC::GetDMSJudgeNum( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql,"select organ.organ_code,count(*) from (select organ_code,distinct(tdnum) from evalusystem.detail.tdinfo "
	"where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') ) a "
	"right join evalusystem.config.organ organ on(organ.organ_code = a.organ_code)", 
			byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select organ.organ_code,count(*) from (select organ_code,distinct(tdnum) from evalusystem.detail.tdinfo where count_time like '%04d-%02d%%' ) a "
		"right join evalusystem.config.organ organ on(organ.organ_code = a.organ_code)",
		byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_Tdtotal_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_dmsjudged.insert(make_pair(yc.organ_code, yc.online)); //应该换一个结构体 // 存储所有研判，是否要去除误上送等
#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取停电总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::InsertResultTDFailureRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];
	std::map<int, float>::iterator itr_failure_result;
	std::map<int, float>::iterator itr_organ_dmsfailure;
	std::map<int, float>::iterator itr_organ_gpmsfailure;

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

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//停电故障率按主站
	rec_num = m_failure_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_failure_result = m_failure_result.begin();
	for(; itr_failure_result != m_failure_result.end(); itr_failure_result++)
	{
		memcpy(m_pdata + offset, "tdfailure", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_failure_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_failure_result = m_failure_score.begin();
	for(; itr_failure_result != m_failure_score.end(); itr_failure_result++)
	{
		memcpy(m_pdata + offset, "tdfailures", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//DMS停电故障数按主站
	offset = 0;
	rec_num = m_organ_tdtotal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	/*itr_organ_dmsfailure = m_organ_dmsfailure.begin();
	for(; itr_organ_dmsfailure != m_organ_dmsfailure.end(); itr_organ_dmsfailure++)*/
	itr_organ_dmsfailure = m_organ_tdtotal.begin();
	for(; itr_organ_dmsfailure != m_organ_tdtotal.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "dmsfailure", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS停电故障数按主站
	offset = 0;
	rec_num = m_organ_gpmsfailure.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_gpmsfailure = m_organ_gpmsfailure.begin();
	for(; itr_organ_gpmsfailure != m_organ_gpmsfailure.end(); itr_organ_gpmsfailure++)
	{
		memcpy(m_pdata + offset, "gpmsfailure", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "GPMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisYC::InsertResultTDJudgedRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];
	std::map<int, float>::iterator itr_failure_result;
	std::map<int, float>::iterator itr_organ_dmsfailure;
	std::map<int, float>::iterator itr_organ_gpmsfailure;
	std::map<int, float>::iterator itr_judgedright_result;
	std::map<int, float>::iterator itr_organ_judgedsum;

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

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//停电研判发布率按主站
	rec_num = m_judged_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_failure_result = m_judged_result.begin();
	for(; itr_failure_result != m_judged_result.end(); itr_failure_result++)
	{
		memcpy(m_pdata + offset, "tdjudged", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_judged_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_failure_result = m_judged_score.begin();
	for(; itr_failure_result != m_judged_score.end(); itr_failure_result++)
	{
		memcpy(m_pdata + offset, "tdjudgeds", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_failure_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//DMS停电研判发布数按主站
	offset = 0;
	rec_num = map_organ_judgednum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgednum.begin();
	for(; itr_organ_dmsfailure != map_organ_judgednum.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judgedfailure", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判正确未发布数按主站
	offset = 0;
	rec_num = map_organ_judgedNoUse.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgedNoUse.begin();
	for(; itr_organ_dmsfailure != map_organ_judgedNoUse.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judgednouse", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判正确未发布数按主站
	offset = 0;
	rec_num = map_organ_judgedNoUse_Indicator.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgedNoUse_Indicator.begin();
	for(; itr_organ_dmsfailure != map_organ_judgedNoUse_Indicator.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judgednousein", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判正确未发布数按主站
	offset = 0;
	rec_num = map_organ_judgedNoUse_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgedNoUse_Trans.begin();
	for(; itr_organ_dmsfailure != map_organ_judgedNoUse_Trans.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judgednousetran", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判正确未发布数按主站
	offset = 0;
	rec_num = map_organ_judgedNoUse_Fa.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgedNoUse_Fa.begin();
	for(; itr_organ_dmsfailure != map_organ_judgedNoUse_Fa.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judgednousefa", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
	
	//DMS停电研判正确未发布数（及时）按主站  //lcm add by 20160107
	offset = 0;
	rec_num = map_organ_judgedNoUse_intime.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_dmsfailure = map_organ_judgedNoUse_intime.begin();
	for(; itr_organ_dmsfailure != map_organ_judgedNoUse_intime.end(); itr_organ_dmsfailure++)
	{
		memcpy(m_pdata + offset, "judged_intime", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsfailure->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
	
		//停电研判正确率按主站
	offset = 0;
	rec_num = m_judgedright_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_judgedright_result = m_judgedright_result.begin();
	for(; itr_judgedright_result != m_judgedright_result.end(); itr_judgedright_result++)
	{
		memcpy(m_pdata + offset, "judgedright", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->second, col_attr[2].data_size);
		//printf("organ:%d,%f\n",itr_judgedright_result->first,itr_judgedright_result->second);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//停电研判正确率按主站
	offset = 0;
	rec_num = m_judgedright_result_Indicator.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_judgedright_result = m_judgedright_result_Indicator.begin();
	for(; itr_judgedright_result != m_judgedright_result_Indicator.end(); itr_judgedright_result++)
	{
		memcpy(m_pdata + offset, "judgedrightin", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->second, col_attr[2].data_size);
		//printf("organ:%d,%f\n",itr_judgedright_result->first,itr_judgedright_result->second);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//停电研判正确率按主站
	offset = 0;
	rec_num = m_judgedright_result_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_judgedright_result = m_judgedright_result_Trans.begin();
	for(; itr_judgedright_result != m_judgedright_result_Trans.end(); itr_judgedright_result++)
	{
		memcpy(m_pdata + offset, "judgedrightran", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->second, col_attr[2].data_size);
		//printf("organ:%d,%f\n",itr_judgedright_result->first,itr_judgedright_result->second);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//停电研判正确率按主站
	offset = 0;
	rec_num = m_judgedright_result_Fa.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_judgedright_result = m_judgedright_result_Fa.begin();
	for(; itr_judgedright_result != m_judgedright_result_Fa.end(); itr_judgedright_result++)
	{
		memcpy(m_pdata + offset, "judgedrightfa", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_judgedright_result->second, col_attr[2].data_size);
		//printf("organ:%d,%f\n",itr_judgedright_result->first,itr_judgedright_result->second);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
	
	//DMS停电研判总数数按主站
	offset = 0;
	rec_num = map_organ_judgedsum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judgedsum = map_organ_judgedsum.begin();
	for(; itr_organ_judgedsum != map_organ_judgedsum.end(); itr_organ_judgedsum++)
	{
		memcpy(m_pdata + offset, "judgedsum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判总数数按主站
	offset = 0;
	rec_num = map_organ_judgedsum_Indicator.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judgedsum = map_organ_judgedsum_Indicator.begin();
	for(; itr_organ_judgedsum != map_organ_judgedsum_Indicator.end(); itr_organ_judgedsum++)
	{
		memcpy(m_pdata + offset, "judgedsumin", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判总数数按主站
	offset = 0;
	rec_num = map_organ_judgedsum_Trans.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judgedsum = map_organ_judgedsum_Trans.begin();
	for(; itr_organ_judgedsum != map_organ_judgedsum_Trans.end(); itr_organ_judgedsum++)
	{
		memcpy(m_pdata + offset, "judgedsumtran", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS停电研判总数数按主站
	offset = 0;
	rec_num = map_organ_judgedsum_Fa.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judgedsum = map_organ_judgedsum_Fa.begin();
	for(; itr_organ_judgedsum != map_organ_judgedsum_Fa.end(); itr_organ_judgedsum++)
	{
		memcpy(m_pdata + offset, "judgedsumfa", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_judgedsum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		//cout << "organ_code:" << itr_organ_dmsfailure->first << "\tnouse2:" << itr_organ_dmsfailure->second << endl;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS停电故障数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS停电故障数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

//InsertResultTDJudgedRateDET old
/*int StatisYC::InsertResultTDJudgedRateDET(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num = 0;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[8];
	char sql[MAX_SQL_LEN];
	std::multimap<int, ES_GDMSJUDGED>::iterator itr_organ_judgednousedet;

	col_num = 8;
	//英文标识
	strcpy(col_attr[0].col_name, "ORGAN_CODE");
	col_attr[0].data_type = DCI_INT;
	col_attr[0].data_size = sizeof(int);

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_NAME");
	col_attr[1].data_type = DCI_STR;
	col_attr[1].data_size = 50;

	strcpy(col_attr[2].col_name, "DMSID");
	col_attr[2].data_type = DCI_STR;
	col_attr[2].data_size = 50;

	strcpy(col_attr[3].col_name, "GPMSID");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 50;

	strcpy(col_attr[4].col_name, "DMSJUDGED_TIME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 50;

	strcpy(col_attr[5].col_name, "GPMSJUDGED_TIME");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	strcpy(col_attr[6].col_name, "DMSJUDGED");
	col_attr[6].data_type = DCI_STR;
	col_attr[6].data_size = 4000;
	
	strcpy(col_attr[7].col_name, "JUDGED_TYPE");
	col_attr[7].data_type = DCI_INT;
	col_attr[7].data_size = 4;

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	sprintf(sql, "insert into evalusystem.result.DMSJUDGED_OLD(organ_code,organ_name,count_time,dmsid,gpmsid,dmsjudged_time,gpmsjudged_time,tdintr,judged_type"
		") values(:1,:2,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:3,:4,to_date(:5,'YYYY-MM-DD HH24:MI:SS'),to_date(:6,'YYYY-MM-DD HH24:MI:SS'),:7,:8);",
		year,month,day);

	//停电研判发布率按主站
	//rec_num = map_judged.size();
	itr_organ_judgednousedet = map_judged.begin();
	for(; itr_organ_judgednousedet != map_judged.end(); itr_organ_judgednousedet++)
	{
		if(itr_organ_judgednousedet->second.type == 0 && itr_organ_judgednousedet->second.gpmsid.size() != 0)
		{
			rec_num++;
		}
	}
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}
	itr_organ_judgednousedet = map_judged.begin();
	for(; itr_organ_judgednousedet != map_judged.end(); itr_organ_judgednousedet++)
	{
		if(itr_organ_judgednousedet->second.type == 0 && itr_organ_judgednousedet->second.gpmsid.size() != 0)
		{
			memcpy(m_pdata + offset,&itr_organ_judgednousedet->first, col_attr[0].data_size);
			offset += col_attr[0].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.organ_name.c_str(), col_attr[1].data_size);
			offset += col_attr[1].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.dmsid.c_str(), col_attr[2].data_size);
			offset += col_attr[2].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.gpmsid.c_str(), col_attr[3].data_size);
			offset += col_attr[3].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.DMSJudgedTime.c_str(), col_attr[4].data_size);
			offset += col_attr[4].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.GPMSJudgedTime.c_str(), col_attr[5].data_size);
			offset += col_attr[5].data_size;
			memcpy(m_pdata + offset, itr_organ_judgednousedet->second.DMSJudged.c_str(), col_attr[6].data_size);
			offset += col_attr[6].data_size;
			memcpy(m_pdata + offset, &(itr_organ_judgednousedet->second.judged_type), col_attr[7].data_size);
			offset += col_attr[7].data_size;
		}
	}

	if(rec_num > 0)
	{
	printf("rec_num:%d\n",rec_num);
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}
*/
int StatisYC::InsertResultTDJudgedRateDET(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num = 0;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[11];
	char sql[MAX_SQL_LEN];
	std::multimap<int, ES_GDMSJUDGED>::iterator itr_organ_judgednousedet;

	col_num = 11;
	//英文标识
	strcpy(col_attr[0].col_name, "ORGAN_CODE");
	col_attr[0].data_type = DCI_INT;
	col_attr[0].data_size = sizeof(int);

	strcpy(col_attr[1].col_name, "DMSID");
	col_attr[1].data_type = DCI_STR;
	col_attr[1].data_size = 50;

	strcpy(col_attr[2].col_name, "FAULT_NO");
	col_attr[2].data_type = DCI_STR;
	col_attr[2].data_size = 50;

	strcpy(col_attr[3].col_name, "DMSJUDGED_TIME");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 50;

	strcpy(col_attr[4].col_name, "GPMSJUDGED_TIME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 50;

	strcpy(col_attr[5].col_name, "DMS_TDINTR");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 8000;
	
	strcpy(col_attr[6].col_name, "GPMS_TDINTR");
	col_attr[6].data_type = DCI_STR;
	col_attr[6].data_size = 4000;
	
	strcpy(col_attr[7].col_name, "TYPE");
	col_attr[7].data_type = DCI_INT;
	col_attr[7].data_size = sizeof(int);
	
	strcpy(col_attr[8].col_name, "JUDGED_TYPE");
	col_attr[8].data_type = DCI_INT;
	col_attr[8].data_size = sizeof(int);

	strcpy(col_attr[9].col_name, "SEND_FLAG");
	col_attr[9].data_type = DCI_INT;
	col_attr[9].data_size = sizeof(int);

	strcpy(col_attr[10].col_name, "DMSDAFF_TIME");
	col_attr[10].data_type = DCI_STR;
	col_attr[10].data_size = 50;
	
	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;
	memset(sql,0x00,sizeof(sql));
	
	sprintf(sql, "delete from evalusystem.result.DMSJUDGED where count_time = '%04d-%2d-%2d';",
		year,month,day);
	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
	memset(sql,0x00,sizeof(sql));
	sprintf(sql, "insert into evalusystem.result.DMSJUDGED(organ_code,count_time,dmsid,fault_no,dmsjudged_time,gpmsjudged_time,tdintr,gpms_tdintr,type,judged_type"
		",send_flag,dmsdaff_time) values(:1,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:2,:3,to_date(:4,'YYYY-MM-DD HH24:MI:SS'),to_date(:5,'YYYY-MM-DD HH24:MI:SS'),:6,:7,:8,:9,:10,to_date(:11,'YYYY-MM-DD HH24:MI:SS'));",
		year,month,day);

	//停电研判未发布数量
	rec_num = map_judged.size();
	itr_organ_judgednousedet = map_judged.begin();
//	printf("%d\n",map_judged.size());
	//printf("111%d\n",rec_num);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}
	itr_organ_judgednousedet = map_judged.begin();
	for(; itr_organ_judgednousedet != map_judged.end(); itr_organ_judgednousedet++)
	{
		memcpy(m_pdata + offset,&itr_organ_judgednousedet->first, col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.dmsid.c_str(), col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.fault_no.c_str(), col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.DMSJudgedTime.c_str(), col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.GPMSJudgedTime.c_str(), col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.DMSJudged.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.GPMSJudged.c_str(), col_attr[6].data_size);
		offset += col_attr[6].data_size;
		memcpy(m_pdata + offset, &(itr_organ_judgednousedet->second.type), col_attr[7].data_size);
		offset += col_attr[7].data_size;
		memcpy(m_pdata + offset, &(itr_organ_judgednousedet->second.judged_type), col_attr[8].data_size);
		offset += col_attr[8].data_size;
		memcpy(m_pdata + offset, &(itr_organ_judgednousedet->second.sendtype), col_attr[9].data_size);
		//printf("sendtype:%d\n",itr_organ_judgednousedet->second.sendtype);
		offset += col_attr[9].data_size;
		memcpy(m_pdata + offset, itr_organ_judgednousedet->second.DMSDdaffTime.c_str(), col_attr[10].data_size);
		offset += col_attr[10].data_size;
	}

	if(rec_num > 0)
	{
		//printf("rec_num:%d\n",rec_num);
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "停电故障率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "停电故障率主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	memset(sql,0x00,sizeof(sql));
	//添加停电研判是否无效标志位，gpms发布时间比dms弹窗时间晚30分钟以上则无效  20160229
	sprintf(sql, "call evalusystem.result.CHECK_TDJUDGED_TYPE('%04d-%02d-%02d');", year, month, day);
	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
	return 0;
}

int StatisYC::GetGDMSJudgedFailureNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time='%04d-%02d-%02d 00:00:00' and (sendtype = 1 or sendtype is null) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null) and flag != 1 group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map_organ_judgednum.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

//DMS研判成功比GPMS发布早对应关系(未做一一对应) old
/*int StatisYC::GetGDMSJudged(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;

	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret, num;
	int rec_num;
	int col_num;
	ub4 longsize;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	memset(sql,0,sizeof(sql));
	//DMS、GPMS停电故障
	sprintf(sql, "select a.organ_code,a.tdnum,b.gpmsid,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr,a.judged_type"
		" from (select organ_code,tdnum,judged_time,tdintr,judged_type from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 2 and flag != 1) a,(select organ_code,dmsid,gpmsid,judged_time,tdintr "
		"from detail.gpms_powercut_det where dmsid = '' and count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') "
		"and DMSID = '' and flag != 1) b where a.organ_code = b.organ_code and datediff(second,a.judged_time,b.judged_time) >= 0;",
		year,month,day,year,month,day);

	ret = m_oci->ReadData(sql,&rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_GDMSJUDGED gdmsJudged;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_Judged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			//memset(&gdmsJudged,0,sizeof(gdmsJudged));
			gdmsJudged.type = 0;
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr,0x00,10000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gdmsJudged.organ_code = *(int *)(tempstr);
					break;
				case 1:
					gdmsJudged.dmsid = string(tempstr);
					break;
				case 2:
					gdmsJudged.gpmsid = string(tempstr);
					break;
				case 3:
					gdmsJudged.DMSJudgedTime = string(tempstr);
					break;
				case 4:
					gdmsJudged.GPMSJudgedTime = string(tempstr);
					break;
				case 5:
					gdmsJudged.DMSJudged = string(tempstr);
					break;
				case 6:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				case 7:
					gdmsJudged.judged_type = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << gdmsJudged.organ_code << "\tdmstdintr:" << gdmsJudged.DMSJudged << "\tgpmstdintr:" << gdmsJudged.GPMSJudged  << "\tDMSID:" << gdmsJudged.dmsid << endl;

			//if (count_if(gdmsJudged.DMSJudged.begin(),gdmsJudged.DMSJudged.end(),';')-1 ==  count_if(gdmsJudged.GPMSJudged.begin(),gdmsJudged.GPMSJudged.end(),','))
			if(GetStringCount(gdmsJudged.DMSJudged,";") == GetStringCount(gdmsJudged.GPMSJudged,",")+1)
			{
				
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}


#ifdef DEBUG
			//outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}
*/
//DMS研判未发布但范围正确对应关系(未做一一对应)   new
int StatisYC::GetGDMSJudged_norelease(int statis_type, ES_TIME *es_time,int type)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;

	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret, num;
	int rec_num;
	int col_num;
	ub4 longsize;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	memset(sql,0,sizeof(sql));
	//DMS、GPMS停电故障
	if(type == 0)
	{
		sprintf(sql, "select a.organ_code,a.tdnum,b.fault_no,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr,a.judged_type,a.sendtype,to_char(a.ddaff,'YYYY-MM-DD HH24:MI:SS') "
			"from (select organ_code,tdnum,ddaff,judged_time,tdintr,judged_type,sendtype from evalusystem.detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') "
			"  and (sendtype = 1 or sendtype = 2) and flag != 1) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid,fault_no,judged_time,tdintr "
			"from evalusystem.detail.gpms_powercut_det where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') "
			"and flag != 1) b where a.organ_code = b.organ_code and a.tdnum = b.dmsid;",
			year,month,day,year,month,day);
		//= to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')   year,month,day,year,month,day)
		//sprintf(sql, "select a.organ_code,a.tdnum,b.gpmsid,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr "
		//	"from (select organ_code,tdnum,judged_time,tdintr from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		//	"  and sendtype = 2 and flag != 1 and judged_time = '2015-12-19 21:57:41.0') a,(select organ_code,dmsid,gpmsid,judged_time,tdintr "
		//	"from detail.gpms_powercut_det where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') "
		//	"and flag != 1 and dmsid = '泉州_1244') b where a.organ_code = b.organ_code;",
		//	2015,12,19,2015,12,19);
	}
	else
	{
		sprintf(sql, "select a.organ_code,a.tdnum,b.fault_no,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr,a.judged_type,a.sendtype,to_char(a.ddaff,'YYYY-MM-DD HH24:MI:SS') "
			"from (select organ_code,tdnum,ddaff,judged_time,tdintr,judged_type,sendtype from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')  "
			" and (sendtype = 1 or sendtype = 2) and flag != 1) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid,fault_no,judged_time,tdintr "
			"from detail.gpms_powercut_det where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')  "
			"and flag != 1) b where a.organ_code = b.organ_code and a.tdnum = b.dmsid and datediff(second,a.judged_time,b.judged_time) >= 0;",
			year,month,day,year,month,day);
	}
	//= to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')  year,month,day,year,month,day);
	ret = m_oci->ReadData(sql,&rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_GDMSJUDGED gdmsJudged;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_Judged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			//memset(&gdmsJudged,0,sizeof(gdmsJudged));
			gdmsJudged.type = 0;
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr,0x00,10000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gdmsJudged.organ_code = *(int *)(tempstr);
					break;
				case 1:
					gdmsJudged.dmsid = string(tempstr);
					break;
				case 2:
					gdmsJudged.fault_no = string(tempstr);
					break;
				case 3:
					gdmsJudged.DMSJudgedTime = string(tempstr);
					break;
				case 4:
					gdmsJudged.GPMSJudgedTime = string(tempstr);
					break;
				case 5:
					gdmsJudged.DMSJudged = string(tempstr);
					break;
				case 6:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				case 7:
					gdmsJudged.judged_type = *(int *)(tempstr);
					break;
				case 8:
					gdmsJudged.sendtype = atoi(tempstr);
					break;
				case 9:
					gdmsJudged.DMSDdaffTime = string(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			char buf_dms[10000];
			char buf_gpms[4000];

			sprintf(buf_dms,"%s",gdmsJudged.DMSJudged.c_str());
			sprintf(buf_gpms,"%s",gdmsJudged.GPMSJudged.c_str());

			SplitDmsStr(buf_dms,";");
			SplitGpmsStr(buf_gpms,",");
			int m_ret = MatchTdintr();

			//int m_ret = MatchTdintr(buf_dms,";",buf_gpms,",");
			/* // @detail 只匹配ID,停电范围完全匹配 */
			if(m_ret == 1 && type == 0)
			{
				gdmsJudged.type = 1;
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}
			/* // @detail 只匹配ID,停电范围近似匹配 */
			else if (m_ret == 2 && type == 0)
			{
				gdmsJudged.type = 2;
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}
			/* // @detail 匹配ID,且DMS时间早,停电范围完全匹配 */
			else if (m_ret == 1 && type == 1)
			{
				gdmsJudged.type = 3;
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}
			/* // @detail 匹配ID,且DMS时间早,停电范围近似匹配 */
			else if (m_ret == 2 && type == 1)
			{
				gdmsJudged.type = 4;
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}


#ifdef DEBUG
			//outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetGDMSJudged_release(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;

	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);
	
	int ret, num;
	int rec_num;
	int col_num;
	ub4 longsize;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	memset(sql,0,sizeof(sql));
	//DMS、GPMS停电故障
	sprintf(sql, "select a.organ_code,a.tdnum,b.fault_no,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr,a.judged_type,a.sendtype "
		"from (select organ_code,tdnum,judged_time,tdintr,judged_type,sendtype from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 1 and flag != 1) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1) dmsid,fault_no,judged_time,tdintr "
			"from evalusystem.detail.gpms_powercut_det where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') and dmsid is not null and flag != 1) b where a.organ_code = b.organ_code and a.tdnum = b.dmsid;",
		year,month,day,year,month,day);

	ret = m_oci->ReadData(sql,&rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_GDMSJUDGED gdmsJudged;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_Judged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			//memset(&gdmsJudged,0,sizeof(gdmsJudged));
			gdmsJudged.type = 0;
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr,0x00,10000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gdmsJudged.organ_code = *(int *)(tempstr);
					break;
				case 1:
					gdmsJudged.dmsid = string(tempstr);
					break;
				case 2:
					gdmsJudged.fault_no = string(tempstr);
					break;
				case 3:
					gdmsJudged.DMSJudgedTime = string(tempstr);
					break;
				case 4:
					gdmsJudged.GPMSJudgedTime = string(tempstr);
					break;
				case 5:
					gdmsJudged.DMSJudged = string(tempstr);
					break;
				case 6:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				case 7:
					gdmsJudged.judged_type = *(int *)(tempstr);
					break;
				case 8:
					gdmsJudged.sendtype = *(int*)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			gdmsJudged.type = 1;
			map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			


#ifdef DEBUG
			//outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

//DMS研判成功比DMS手动发布早对应关系(未做一一对应)//未应用
/*int StatisYC::GetDmsNoUseJudged(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;

	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret, num;
	int rec_num;
	ub4 longsize;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//DMS、GPMS停电故障
	sprintf(sql, "select a.organ_code,a.tdnum,b.tdnum,to_char(a.judged_time,'YYYY-MM-DD HH24:MI:SS'),to_char(b.judged_time,'YYYY-MM-DD HH24:MI:SS'),a.tdintr,b.tdintr,a.judged_type "
		" from (select organ_code,tdnum,judged_time,tdintr,judged_type from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 2 and flag != 1 and tdintr !='') a,(select organ_code,tdnum,judged_time,tdintr from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 0 and flag != 1 and tdintr !='') b where a.organ_code = b.organ_code and datediff(second,a.judged_time,b.judged_time) >= 0;",
		year,month,day,year,month,day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_GDMSJUDGED gdmsJudged;
		char *tempstr = (char *)malloc(15000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_Judged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			gdmsJudged.type = 0;
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0x00 , 15000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gdmsJudged.organ_code = *(int *)(tempstr);
					break;
				case 1:
					gdmsJudged.dmsid = string(tempstr);
					break;
				case 2:
					gdmsJudged.gpmsid = string(tempstr);
					break;
				case 3:
					gdmsJudged.DMSJudgedTime = string(tempstr);
					break;
				case 4:
					gdmsJudged.GPMSJudgedTime = string(tempstr);
					break;
				case 5:
					gdmsJudged.DMSJudged = string(tempstr);
					break;
				case 6:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				case 7:
					gdmsJudged.judged_type = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << gdmsJudged.organ_code << "\tdmstdintr:" << gdmsJudged.DMSJudged << "\tgpmstdintr:" << gdmsJudged.GPMSJudged  << "\tDMSID:" << gdmsJudged.dmsid << endl;

			//if (count_if(gdmsJudged.DMSJudged.begin(),gdmsJudged.DMSJudged.end(),'[') ==  count_if(gdmsJudged.GPMSJudged.begin(),gdmsJudged.GPMSJudged.end(),'['))
			if(GetStringCount(gdmsJudged.DMSJudged,";") == GetStringCount(gdmsJudged.GPMSJudged,";"))
			{
				map_judged.insert(make_pair(gdmsJudged.organ_code,gdmsJudged));
			}


#ifdef DEBUG
			//outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}
*/

int StatisYC::SetRightJudgedDetType(int statis_type, ES_TIME *es_time)
{
	if(map_judged.size() == 0)
		return 0;

	int count = 0;
	vector<string> vec_tmp;
	string::basic_string <char>::size_type indexNum1,indexNum2;
	vector<string>::iterator it_tmp;
	map<int,float>::iterator it_map_tmp;

	multimap<int,ES_GDMSJUDGED>::iterator it_Judged = map_judged.begin();
	for (;it_Judged != map_judged.end();it_Judged++)
	{
		vec_tmp.clear();

		if(it_Judged->second.DMSJudged.empty() != true)
		{

			indexNum1 = it_Judged->second.DMSJudged.find("[");
			if (indexNum1 != string::npos)
			{
				indexNum2 = it_Judged->second.DMSJudged.find("]",indexNum1);
				if (indexNum2 != string::npos)
				{
					string str = it_Judged->second.DMSJudged.substr(indexNum1,indexNum2);
					vec_tmp.push_back(str);
				}
			}
		}
		it_tmp = vec_tmp.begin();
		for (;it_tmp != vec_tmp.end();it_tmp++)
		{
			if(it_Judged->second.GPMSJudged.find(it_tmp->c_str()) == string::npos)
			{
				it_Judged->second.type = 1;
				//cout << "Judged:" << it_Judged->second.gpmsid << "\torgan_code:" << it_Judged->second.organ_code << endl;
				break;
			}
		}
		//cout << "organ_code:" << it_Judged->first <<"\tdmsid:" << it_Judged->second.dmsid << "\tgpmsid:" << it_Judged->second.gpmsid << "\tdmsjudged_time:" << it_Judged->second.DMSJudgedTime << "\tgpmsjudged_time:" << it_Judged->second.GPMSJudgedTime << "\tdmsp_tdintr:" << it_Judged->second.DMSJudged << "\tgpms_tdintr:" << it_Judged->second.GPMSJudged << "\ttype:" << it_Judged->second.type<< endl;

		/*if(it_Judged->second.type == 0)
		{
		map_Suc_judged.insert(make_pair(it_Judged->first,it_Judged->second));
		if((it_map_tmp = map_organ_judgedNoUse.find(it_Judged->first)) == map_organ_judgedNoUse.end())
		map_organ_judgedNoUse.insert(make_pair(it_Judged->first,1));
		else
		it_map_tmp->second += 1;
		}
		else
		map_organ_judgedNoUse.insert(make_pair(it_Judged->first,0));*/
	}
	return 0;
}

int StatisYC::GetRightJudgedNum(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

/*	sprintf(sql,"delete from evalusystem.result.dmsjudged a where rowid!="
		"(select max(rowid) from evalusystem.result.dmsjudged b where a.organ_code=b.organ_code and "
		"a.count_time=b.count_time and a.fault_no=b.fault_no and count_time = '%04d-%02d-%02d 00:00:00');",
		byear,bmonth,bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
*/
/*
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsjudged_time"
		" from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' order by organ_code,dmsjudged_time) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsjudged_time"
		" from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and flag != 1) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);
*/		
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' and (type = 3)  and flag != 1) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday); // type = 1更改为type=3,加入了时间判断
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and (type = 3) and flag != 1) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);


	//cout << "sql:" << sql << endl;

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DMSRightJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << yc.organ_code << "\tnouse:" << yc.online << endl;
			map_organ_judgedNoUse.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetRightJudgedNum_Indicator( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no "
		" from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' and (type = 3)  and flag != 1 and judged_type = 3) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday); // type = 1更改为type=3,加入了时间判断
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no "
		" from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and (type = 3) and flag != 1  and judged_type = 3) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);


	//cout << "sql:" << sql << endl;

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DMSRightJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << yc.organ_code << "\tnouse:" << yc.online << endl;
			map_organ_judgedNoUse_Indicator.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetRightJudgedNum_Trans( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' and (type = 3)  and flag != 1 and judged_type in(1,2,6)) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday); // type = 1更改为type=3,加入了时间判断
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and (type = 3) and flag != 1  and judged_type in(1,2,6)) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);


	//cout << "sql:" << sql << endl;

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DMSRightJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << yc.organ_code << "\tnouse:" << yc.online << endl;
			map_organ_judgedNoUse_Trans.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetRightJudgedNum_Fa( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' and (type = 3)  and flag != 1 and judged_type in(4,5)) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday); // type = 1更改为type=3,加入了时间判断
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c from (select distinct organ_code,dmsid,dmsjudged_time,fault_no"
		" from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and (type = 3) and flag != 1  and judged_type in(4,5)) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);


	//cout << "sql:" << sql << endl;

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DMSRightJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << yc.organ_code << "\tnouse:" << yc.online << endl;
			map_organ_judgedNoUse_Fa.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetRightJudgedNumIntime(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);
/*
	sprintf(sql,"delete from evalusystem.result.dmsjudged a where rowid!="
		"(select max(rowid) from evalusystem.result.dmsjudged b where a.organ_code=b.organ_code and "
		"a.count_time=b.count_time and a.fault_no=b.fault_no and count_time = '%04d-%02d-%02d 00:00:00');",
		byear,bmonth,bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
*/
/*
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c "
		"from (select distinct organ_code, dmsjudged_time from(select * from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' "
        " and datediff(minute,dmsjudged_time,gpmsjudged_time) <= 30 order by organ_code, dmsjudged_time)) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c "
		"from (select distinct organ_code,dmsjudged_time from (select * from evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and flag != 1"
        " and datediff(minute,dmsjudged_time,gpmsjudged_time) <= 30)) group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);
*/
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c "
		"from (select distinct organ_code,dmsid,dmsjudged_time,fault_no from (select * from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00' "
		" and type = 3 and judged_flag = 0 )) group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from "
		"(select organ_code,count(*) c from  evalusystem.result.dmsjudged where count_time like '%04d-%02d%%' and type = 3 and flag != 1 and "
		"judged_flag = 0 group by organ_code) a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);"
		, byear, bmonth);
	
	//cout << "sql:" << sql << endl;

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_DMSRightJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//cout << "organ_code:" << yc.organ_code << "\tnouse:" << yc.online << endl;
			map_organ_judgedNoUse_intime.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetJudgedAll(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time='%04d-%02d-%02d 00:00:00' and (sendtype = 1 or sendtype is null or sendtype = 2) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null or sendtype = 2) and flag != 1 group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map_organ_judgedsum.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetJudgedAll_Indicator( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time='%04d-%02d-%02d 00:00:00' and (sendtype = 1 or sendtype is null or sendtype = 2 ) and judged_type = 3 group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null or sendtype = 2) and flag != 1 and judged_type = 3 group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map_organ_judgedsum_Indicator.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetJudgedAll_Trans( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time='%04d-%02d-%02d 00:00:00' and (sendtype = 1 or sendtype is null or sendtype = 2) and judged_type in(1,2,6) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null or sendtype = 2) and flag != 1 and judged_type in(1,2,6) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map_organ_judgedsum_Trans.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetJudgedAll_Fa( int statis_type, ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time='%04d-%02d-%02d 00:00:00' and (sendtype = 1 or sendtype is null or sendtype = 2) and judged_type in(4,5) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null or sendtype = 2) and flag != 1 and judged_type in(4,5) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING yc;
		char *tempstr = (char *)malloc(10000);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/YC_GpmsJudged_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					yc.organ_code = *(int *)(tempstr);
					break;
				case 1:
					yc.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			map_organ_judgedsum_Fa.insert(make_pair(yc.organ_code, yc.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<yc.organ_code<<"\t:"<<yc.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS停电故障数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisYC::GetStringCount(string &str,char *ivalue)
{
	if(str.empty() == true)
		return 0;
	int count = 0;

	basic_string<char>::size_type indexch1a;

	indexch1a = str.find(ivalue);
	
	while (indexch1a != string::npos)
	{
		count++;
		indexch1a = str.find(ivalue,indexch1a+1);
	}
	return count;
}

//停电范围匹配
int StatisYC::MatchTdintr()
{
	//进行停电范围匹配
	float match_rate = 0.0;
	int dms_size = dms_tdintr.size();
	int gpms_size = gpms_tdintr.size();
	/*printf("dms_tdint			r.size:%d\n",dms_tdintr.size());
	printf("dms_tdintr			.size:%d\n",gpms_tdintr.size());*/
	
	for(vector<string>::iterator it_gpms = gpms_tdintr.begin();it_gpms != gpms_tdintr.end();it_gpms++)
	{
		for(vector<string>::iterator it_dms = dms_tdintr.begin();it_dms != dms_tdintr.end();)
		{
			if(strcmp((*it_dms).c_str(),(*it_gpms).c_str()) == 0)
			{
				//printf("%s,%s\n",(*it_dms).c_str(),(*it_gpms).c_str());
				it_dms = dms_tdintr.erase(it_dms);
			}
			else
			{
				it_dms++;
			}
		}
	}
	//printf("dms_tdintr.size():%d\n",dms_tdintr.size());
	if(dms_size == gpms_size)
	{
		if(dms_tdintr.size() != 0)
		{
			match_rate = (float)(dms_size - dms_tdintr.size()) / (gpms_size + dms_tdintr.size());
			if(match_rate >= 0.5)
			{
				printf("近似匹配成功！");
			//	printf("qqqqqqqqqqqqqqqqqqqq2222qq%s,%s\n",dms_str,gpms_str);
				return 2;
			}
			else 
			{
				return -1;
			}
		}
		else
		{
			printf("完全匹配成功！");
			//printf("qqqqqqqqqqqqqqqqqqqq2222qq%s,%s\n",dms_str,gpms_str);
			return 1;
		}
	}
	else
	{
		match_rate = (float)(dms_size - dms_tdintr.size()) / (gpms_size + dms_tdintr.size());
		if(match_rate >= 0.5)
		{
			printf("近似匹配成功！");
			//printf("qqqqqqqqqqqqqqqqqqqq2222qq%s,%s\n",dms_str,gpms_str);
			return 2;
		}
		else 
		{
			return -1;
		}
	}
	return 0;
}

int StatisYC::SplitDmsStr(char *dms_str,const char *dms_ivalue, int type)
{
	if(strlen(dms_str) ==0)
		return -1;
	if(type == 0)
		dms_tdintr.clear();
	else
		gpms_tdintr.clear();
	char tmp_str[200];
	char *pstart = NULL;
	char *pend = NULL;
	char m_tmp_str[100];
	
	//分隔dms停电范围
	char *e_str;
	char buf_dms[10000];
	memset(buf_dms,0x00,sizeof(buf_dms));
	sprintf(buf_dms,"%s",dms_str);
	
	e_str=strtok(buf_dms,dms_ivalue);
	sprintf(tmp_str,"%s",e_str);
	
	pstart = strstr(tmp_str,"[");
	pend = strstr(tmp_str,"]");
	
	memset(m_tmp_str,0,sizeof(m_tmp_str));
	if(pstart != NULL && pend != NULL)
		strncpy(m_tmp_str,pstart+1,pend - pstart - 1);
	if(type == 0)
	{
		//printf("%s\n",m_tmp_str);
		dms_tdintr.push_back(m_tmp_str);
	}
	else
	{
		//printf("%s\n",m_tmp_str);
		gpms_tdintr.push_back(m_tmp_str);
	}
	while(e_str!=NULL)
    {
		e_str=strtok(NULL,dms_ivalue);
		if(e_str!=NULL)
		{
			sprintf(tmp_str,"%s",e_str);
			pstart = strstr(tmp_str,"[");
			pend = strstr(tmp_str,"]");
			memset(m_tmp_str,0,sizeof(m_tmp_str));
			if(pstart != NULL && pend != NULL)
				strncpy(m_tmp_str,pstart+1,pend - pstart - 1);
			if(type == 0)
			{
				//printf("%s\n",m_tmp_str);
				dms_tdintr.push_back(m_tmp_str);
			}
			else
			{
				//printf("%s\n",m_tmp_str);
				gpms_tdintr.push_back(m_tmp_str);
			}
		}
    }
}

int StatisYC::SplitGpmsStr(char *gpms_str,const char *gpms_ivalue)
{
	if(strlen(gpms_str) == 0)
		return -1;
	gpms_tdintr.clear();
	char tmp_str[200];
	char *pstart = NULL;
	char *pend = NULL;
	
	char *e_str;
	//分隔gpms停电范围
	char buf_gpms[4000];
	memset(buf_gpms,0x00,sizeof(buf_gpms));
	sprintf(buf_gpms,"%s",gpms_str);
	e_str=strtok(buf_gpms,gpms_ivalue);
	sprintf(tmp_str,"%s",e_str);
	//printf("%s\n",tmp_str);
	gpms_tdintr.push_back(tmp_str);
	while(e_str!=NULL)
    {
		e_str=strtok(NULL,gpms_ivalue);
		if(e_str!=NULL)
		{
			sprintf(tmp_str,"%s",e_str);
			//printf("%s\n",tmp_str);
			gpms_tdintr.push_back(tmp_str);
		}
    }
}

int StatisYC::GetAllOrgan(map<int,float> &map_organ)
{
	map_organ.clear();

	int ret;
	ErrorInfo err_info;

	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code from evalusystem.config.organ where 1=1;");
	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		char *tempstr = (char *)malloc(30);
		int rec, col, offset = 0;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 30);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					map_organ.insert(make_pair(*(int*)(tempstr),0));
					break;
				}
				m_presult += col_attr[col].data_size;
			}
		}
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
	{
		m_oci->FreeColAttrData(col_attr, col_num);
	}
	return 0;
}

int StatisYC::GetJudgedNoUseDet(int statis_type, ES_TIME *es_time)
{
	map_judged.clear();
	if(GetGDMSJudged_norelease(statis_type,es_time,0) < 0)
		return -1;
	if(GetGDMSJudged_norelease(statis_type,es_time,1) < 0)
		return -1;
	if(GetGDMSJudged_release(statis_type,es_time) < 0)
		return -1;
	if(InsertResultTDJudgedRateDET(statis_type,es_time) < 0 )
		return -1;
	return 0;
}

/*int StatisYC::GetWeight()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;


	sprintf(sql,"select code,weight from evalusystem.config.weight;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_WEIGHT weight;
		char *tempstr = (char *)malloc(30);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/StatisYC_WEIGHT_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 30);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					weight.code = string(tempstr);
					break;
				case 1:
					weight.weight = *(double *)(tempstr)*100;
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_code_weight.insert(make_pair(weight.code, weight.weight));
#ifdef DEBUG
			outfile<<++line<<"\tcode:"<<weight.code<<"\tweight:"<<weight.weight<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS配变总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}*/

