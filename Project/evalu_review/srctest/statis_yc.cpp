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
			goto FAILED_RET;
		ret = GetTqnumByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;

		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
#endif
		ret = GetYCRateByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = InsertResultTZRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		/*ret = InsertResultTdintimeRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = InsertResultDmstdpubRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdIntimeNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdconfirmNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdTotalNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetDMSFailureNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetGPMSFailureNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetGDMSJudgedFailureNum(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetJudgedNoUseDet(statis_type,es_time);
		if(ret < 0 )
			goto FAILED_RET;

		ret = GetRightJudgedNum(statis_type,es_time);
		if(ret < 0)
			goto FAILED_RET;

		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
	}
	else
	{
		/*ret = GetYCMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;*/
		ret = GetYCRateByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = InsertResultTZRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		/*ret = InsertResultTdintimeRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = InsertResultDmstdpubRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdIntimeNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdconfirmNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetTdTotalNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetDMSFailureNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetGPMSFailureNum(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		ret = GetGDMSJudgedFailureNum(statis_type,es_time);
		if (ret < 0)
			goto FAILED_RET;

		ret = GetRightJudgedNum(statis_type,es_time);
		if (ret < 0)
			goto FAILED_RET;


		//计算
		ret = GetYCsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
	}

	ret = InsertResultTDRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;

	ret = InsertResultTDFailureRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;

	ret = InsertResultTDJudgedRate(statis_type,es_time);
	if(ret < 0)
		goto FAILED_RET;

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
	map<int, float>::iterator itr_organ_dmsjudged = map_organ_judgednum.begin();
	for(; itr_organ_dmsjudged != map_organ_judgednum.end(); itr_organ_dmsjudged++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_dmsjudged->first;
		map<int, float>::iterator itr_organ_nouseno = map_organ_judgedNoUse.find(itr_organ_gpmsfailure->first);
		if(itr_organ_nouseno != map_organ_judgedNoUse.end())
		{
			es_result.m_fresult = itr_organ_dmsjudged->second / (itr_organ_dmsjudged->second+itr_organ_nouseno->second);
		}
		else
			es_result.m_fresult = 1.0;

		m_judged_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_dmsjudged->first;
		itr_code_weight = m_code_weight.find("tdjudged");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_judged_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
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
	m_judged_result.clear();
	map_judged.clear();

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

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,tz,cj from evalusystem.detail.ycrate where "
		"count_time=to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS')", byear, bmonth, bday);

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
					tq.total = *(double *)(tempstr);
					break;
				case 2:
					tq.sendnum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_tzmatch_result.insert(make_pair(tq.organ_code, tq.total));
			m_tzsuccess_result.insert(make_pair(tq.organ_code, tq.sendnum));

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

	sprintf(sql, "select organ_code,avg(tz),avg(cj) from evalusystem.detail.ycrate where "
		"count_time like '%04d-%02d%%' group by organ_code", byear, bmonth);

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
				case 2:
					tq.sendnum = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_tzmatch_result.insert(make_pair(tq.organ_code, tq.total));
			m_tzsuccess_result.insert(make_pair(tq.organ_code, tq.sendnum));

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
		sprintf(sql, "select a.organ_code,count(*) from (select distinct(tdnum),organ_code from evalusystem.detail.tdinfo "
			"where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
			"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and (sendtype != 2 or sendtype is null)) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1,3) dmsid "
			"from evalusystem.detail.gpms_powercut_det where send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
			"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and dmsid != '') b where a.organ_code = b.organ_code and "
			"a.tdnum = b.dmsid group by a.organ_code;", byear, bmonth, bday, year, month, day,byear, bmonth, bday, year, month, day);
	else
		sprintf(sql, "select a.organ_code,count(*) from (select organ_code,tdnum from evalusystem.detail.tdinfo "
		"where count_time like '%04d-%02d%%' and (sendtype != 2 or sendtype is null)) a,(select organ_code,substr(dmsid,instr(dmsid,'_',1,1)+1,3) dmsid "
		"from evalusystem.detail.gpms_powercut_det where count_time like '%04d-%02d%%' and dmsid != '') b where a.organ_code = b.organ_code and "
		"a.tdnum = b.dmsid group by a.organ_code;", byear, bmonth, byear, bmonth);

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
			"count_time='%04d-%02d-%02d 00:00:00.0' and (sendtype != 2 or sendtype is null) group by organ_code", 
			byear, bmonth, bday);
	else
		sprintf(sql, "select organ_code,count(*) num from evalusystem.detail.tdinfo where "
			"count_time like '%04d-%02d%%' and  (sendtype != 2 or sendtype is null) group by organ_code", byear, bmonth);

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
		sprintf(sql, "select organ_code,count(*) from evalusystem.detail.gpms_powercut_det where "
			"count_time='%04d-%02d-%02d 00:00:00.0' group by organ_code", byear, bmonth, bday);
	else
		sprintf(sql, "select organ_code,count(*) from evalusystem.detail.gpms_powercut_det where "
			"count_time like '%04d-%02d%%' group by organ_code", byear, bmonth);

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
	rec_num = m_organ_dmsfailure.size();
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
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	std::multimap<int, ES_GDMSJUDGED>::iterator itr_organ_judgednousedet;

	col_num = 7;
	//英文标识
	strcpy(col_attr[0].col_name, "ORGAN_CODE");
	col_attr[0].data_type = DCI_INT;
	col_attr[0].data_size = sizeof(int);

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_NAME");
	col_attr[1].data_type = DCI_STR;
	col_attr[1].data_size = 256;

	strcpy(col_attr[3].col_name, "DMSID");
	col_attr[2].data_type = DCI_STR;
	col_attr[2].data_size = 50;

	strcpy(col_attr[4].col_name, "GPMSID");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 50;

	strcpy(col_attr[5].col_name, "DMSJUDGED_TIME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 50;

	strcpy(col_attr[5].col_name, "GPMSJUDGED_TIME");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	strcpy(col_attr[6].col_name, "DMSJUDGED");
	col_attr[6].data_type = DCI_STR;
	col_attr[6].data_size = 4000;

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	sprintf(sql, "insert into evalusystem.result.DMSJUDGED(organ_code,organ_name,count_time,dmsid,gpmsid,dmsjudged_time,gpmsjudged_time,tdintr"
		") values(:1,:2,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:3,:4,to_date(:5'YYYY-MM-DD HH24:MI:SS'),to_date(:6'YYYY-MM-DD HH24:MI:SS'),:7)",
		 year, month, day);

	//停电研判发布率按主站
	rec_num = map_judged.size();
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
		if(itr_organ_judgednousedet->second.type == 0)
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
		}
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
		"count_time='%04d-%02d-%02d 00:00:00.0' and (sendtype = 1 or sendtype is null) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)",
		byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,NULL,0,a.c) from (select organ_code co,count(*) c from evalusystem.detail.tdinfo where "
		"count_time like '%04d-%02d%%' and (sendtype = 1 or sendtype is null) group by organ_code) a right join evalusystem.config.organ organ on(organ.organ_code = a.co)", byear, bmonth);

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

//DMS研判成功比GPMS发布早对应关系(未做一一对应)
int StatisYC::GetGDMSJudged(int statis_type, ES_TIME *es_time)
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
	sprintf(sql, "select a.organ_code,a.tdnum,b.gpmsid,a.judged_time,b.judged_time,a.tdintr,b.tdintr "
		"from (select organ_code,tdnum,judged_time,tdintr from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 2) a,(select organ_code,dmsid,gpmsid,judged_time,tdintr "
		"from detail.gpms_powercut_det where dmsid = '' and count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS') "
		"and DMSID = '') b where a.organ_code = b.organ_code and datediff(second,a.judged_time,b.judged_time) >= 0;",
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
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 10000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					gdmsJudged.organ_code = *(int *)(tempstr);
					break;
				case 2:
					gdmsJudged.dmsid = string(tempstr);
					break;
				case 3:
					gdmsJudged.gpmsid = string(tempstr);
					break;
				case 4:
					gdmsJudged.DMSJudgedTime = string(tempstr);
					break;
				case 5:
					gdmsJudged.GPMSJudgedTime = string(tempstr);
					break;
				case 6:
					gdmsJudged.DMSJudged = string(tempstr);
					break;
				case 7:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//if (count_if(gdmsJudged.DMSJudged.begin(),gdmsJudged.DMSJudged.end(),';')-1 ==  count_if(gdmsJudged.GPMSJudged.begin(),gdmsJudged.GPMSJudged.end(),','))
			if(GetStringCount(gdmsJudged.DMSJudged,"[")-1 == GetStringCount(gdmsJudged.GPMSJudged,","))
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

//DMS研判成功比DMS手动发布早对应关系(未做一一对应)
int StatisYC::GetDmsNoUseJudged(int statis_type, ES_TIME *es_time)
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
	sprintf(sql, "select a.organ_code,a.tdnum,b.tdnum,a.judged_time,b.judged_time,a.tdintr,b.tdintr "
		"from (select organ_code,tdnum,judged_time,tdintr from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 2) a,(select organ_code,tdnum,judged_time,tdintr from detail.tdinfo where count_time = to_date('%d-%d-%d 00:00:00','YYYY-MM-DD HH24:MI:SS')"
		"  and sendtype = 0) b where a.organ_code = b.organ_code and datediff(second,a.judged_time,b.judged_time) >= 0;",
		year,month,day,year,month,day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
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
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 10000);
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
					/*case 5:
					gdmsJudged.DMSJudged = string(tempstr);
					break;*/
				case 5:
					gdmsJudged.GPMSJudged = string(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			//if (count_if(gdmsJudged.DMSJudged.begin(),gdmsJudged.DMSJudged.end(),'[') ==  count_if(gdmsJudged.GPMSJudged.begin(),gdmsJudged.GPMSJudged.end(),'['))
			if(GetStringCount(gdmsJudged.DMSJudged,"[")-1 == GetStringCount(gdmsJudged.GPMSJudged,","))
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
				break;
			}
		}
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

	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c "
		"from evalusystem.result.dmsjudged where count_time='%04d-%02d-%02d 00:00:00.0' group by organ_code) "
		" a right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth, bday);
	else
		sprintf(sql, "select organ.organ_code,decode(a.c,null,0,a.c) from (select organ_code,count(*) c "
		"from evalusystem.result.dmsjudged where count_time like '%%4d-%2d%%' group by organ_code) a right join "
		"evalusystem.config.organ organ on(a.organ_code = organ.organ_code);", byear, bmonth);

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
	if(GetGDMSJudged(statis_type,es_time) < 0)
		return -1;

	if(GetDmsNoUseJudged(statis_type,es_time) < 0)
		return -1;

	if(SetRightJudgedDetType(statis_type,es_time) < 0)
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

