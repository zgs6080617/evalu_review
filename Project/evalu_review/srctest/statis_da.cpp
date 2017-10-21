/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_da.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : DA相关统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <map>
#include "statis_da.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisDA::StatisDA(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisDA::~StatisDA()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

int StatisDA::GetDARunningRate(int statis_type, ES_TIME *es_time)
{
	int ret;
	
	Clear();
	//GetWeight();
	//获取投入da的dv和organ_code的关系
	ret = GetDASrcDaRelation();
	if(ret < 0)
		goto FAILED_RET;

	if(statis_type == STATIS_DAY)
	{	
#if 0
		ret = GetDAFinishTimesByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDAStartTimesByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDALinesByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDADisableTimeByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDASuccessTimesByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
#endif
		ret = GetFATouyunLines(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetFALines(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
	}
	else
	{
#if 0
		ret = GetDAFinishTimesByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDAStartTimesByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDALinesByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDADisableTimeByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDASuccessTimesByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;
#endif
		ret = GetFATouyunLines(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetFALines(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
	}
	
	//计算
	ret = GetDARate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;

	//插入数据到统计表中
#if 0
	ret = InsertResultDASuccessRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;
	ret = InsertResultDAAbleRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;
	ret = InsertResultDAAccuracyRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;
#endif
	ret = InsertResultFATouyunRate(statis_type, es_time);
	if(ret < 0)
		goto FAILED_RET;
	
	return 0;

FAILED_RET:
	ES_ERROR_INFO es_err;
	es_err.statis_type = STATIS_DA;
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

int StatisDA::GetDAFinishTimesByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select distinct(a.dv),b.organ_code,a.c from evalusystem.detail.dagc b,(select dv,count(*) c from "
		"evalusystem.detail.dagc where step=4 and result=1 and send_time > to_date('%04d-%02d-%02d 23:59:59',"
		"'YYYY-MM-DD HH24:MI:SS') and send_time < to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by dv) a where a.dv=b.dv",byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Finish_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			//memset(&da, 0, sizeof(ES_DATIMES));
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					da.dv = string(tempstr);
					break;
				case 1:
					da.organ_code = *(int *)(tempstr);
					break;
				case 2:
					da.count = (float)*(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_finish.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_finish = m_organ_finish.find(da.organ_code);
			if(itr_organ_finish != m_organ_finish.end())
			{
				float count = itr_organ_finish->second;
				count += da.count;
				m_organ_finish[da.organ_code] = count;
			}
			else
			{
				m_organ_finish.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA完成次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	
	return 0;
}

int StatisDA::GetDAStartTimesByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select distinct(a.dv),b.organ_code,a.c from evalusystem.detail.dagc b,(select dv,count(*) c from "
		"evalusystem.detail.dagc where step=0 and result=1 and send_time > to_date('%04d-%02d-%02d 23:59:59',"
		"'YYYY-MM-DD HH24:MI:SS') and send_time < to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by dv) a where a.dv=b.dv",byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Start_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			//memset(&da, 0, sizeof(ES_DATIMES));
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					da.dv = string(tempstr);
					break;
				case 1:
					da.organ_code = *(int *)(tempstr);
					break;
				case 2:
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_start.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_start = m_organ_start.find(da.organ_code);
			if(itr_organ_start != m_organ_start.end())
			{
				float count = itr_organ_start->second;
				count += da.count;
				m_organ_start[da.organ_code] = count;
			}
			else
			{
				m_organ_start.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA启动次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDALinesByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,count(*) from evalusystem.detail.dmsdv where src=0 and oper!=2 group by organ_code");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		int organ_code;
		float lines;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Lines_Day.txt");
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
					organ_code = *(int *)(tempstr);
					break;
				case 1:
					lines = (float)*(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_lines.insert(make_pair(organ_code, lines));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<organ_code<<"\tlines:"<<lines<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA线路总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDASrcDaRelation()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select distinct(dv),organ_code from evalusystem.detail.dmsdv where src=0 and oper!=2");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		int organ_code;
		string dv;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Realation_Day.txt");
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
					dv = string(tempstr);
					break;
				case 1:
					organ_code = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_srcdv_organ.insert(make_pair(dv, organ_code));
#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<dv<<"\torgan_code:"<<organ_code<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA馈线和公司关系失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDADisableTimeByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#ifdef MULTI_DATT	//每日多条投退记录
	sprintf(sql, "select dv,organ_code,to_char(gtime,'YYYY-MM-DD HH24:MI:SS'),src from (select organ_code"
		",dv,gtime,src"
		" from evalusystem.detail.datt where send_time > to_date('%04d-%02d-%02d 23:59:59', 'YYYY-MM-DD HH24:MI:SS') "
		"and send_time < to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') union select a.organ_code,a.dv,"
		"to_char(a.gtime,'YYYY-MM-DD HH24:MI:SS'),a.src from evalusystem.detail.datt a,(select dv,max(gtime) t "
		"from evalusystem.detail.datt where send_time < to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by dv) b where a.gtime=b.t and a.dv=b.dv) c order by gtime desc",
		byear, bmonth, bday, year, month, day, year, month, day);
#else	//每日单条投退记录
	sprintf(sql, "select dv,organ_code,to_char(gtime,'YYYY-MM-DD HH24:MI:SS'),src from evalusystem.detail.datt "
		"where send_time > to_date('%04d-%02d-%02d 23:59:59', 'YYYY-MM-DD HH24:MI:SS') and send_time < "
		"to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') union select a.dv,a.organ_code,"
		"to_char(a.gtime,'YYYY-MM-DD HH24:MI:SS'),a.src from evalusystem.detail.datt a,(select dv,max(gtime) t "
		"from evalusystem.detail.datt where send_time < to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by dv) b where a.gtime=b.t and a.dv=b.dv",byear, bmonth, bday, year, month, day, year, month, day);
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATT da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_DisableTime_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				if(col == 2)
					strncpy(tempstr, m_presult, 300);
				else
					memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					da.dv = string(tempstr);
					break;
				case 1:
					da.organ_code = *(int *)(tempstr);
					break;
				case 2:
					da.gtime = GetTimeTByYYYYMMDDHHMMSS(tempstr);
					break;
				case 3:
					da.src = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_datt.insert(make_pair(da.dv, da));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.gtime<<"\tsrc"<<da.src<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//DA停用时间
	map<std::string, int>::iterator itr_srcdv_organ;
	map<std::string, float>::iterator itr_dv_disable;
	map<int, float>::iterator itr_organ_disable;
	multimap<std::string, ES_DATT>::iterator itr_dv_datt;

#ifdef MULTI_DATT
	time_t begin_time, end_time;
	tm tm_temp;
	tm_temp.tm_year = byear - 1900;//es_time->year - 1900;
	tm_temp.tm_mon = bmonth - 1;//es_time->month - 1;
	tm_temp.tm_mday = bday;//es_time->day;
	tm_temp.tm_hour = 0;
	tm_temp.tm_min = 0;
	tm_temp.tm_sec = 0;
	begin_time = mktime(&tm_temp);
	end_time = begin_time + 86400;
	
	itr_srcdv_organ = m_srcdv_organ.begin();
	for(; itr_srcdv_organ != m_srcdv_organ.end(); itr_srcdv_organ++)
	{
		num = m_dv_datt.count(itr_srcdv_organ->first);
		itr_dv_datt = m_dv_datt.find(itr_srcdv_organ->first);

		if(num > 0)
		{
			string dv[num];
			int organ_code[num];
			int src[num];
			time_t gtime[num];
			int result[num];
			for(int i = 0; i < num; i++)
			{
				dv[i] = itr_dv_datt->second.dv;
				organ_code[i] = itr_dv_datt->second.organ_code;
				src[i] = itr_dv_datt->second.src;
				gtime[i] = itr_dv_datt->second.gtime;
				itr_dv_datt++;
			}

			if(num == 1)
			{
				if(src[0] == 0)
				{
					if(gtime[0] <= begin_time)
						result[0] = 0;
					else
						result[0] = gtime[0] - begin_time;
				}
				else
				{
					if(gtime[0] <= begin_time)
						result[0] = 86400;
					else
						result[0] = end_time - gtime[0];
				}
				ES_DATIMES da;
				da.organ_code = organ_code[0];
				da.dv = dv[0];
				da.count = (float)result[0]/3600.0;
				m_dv_disable.insert(make_pair(da.dv, da));
				itr_organ_disable = m_organ_disable.find(organ_code[0]);
				if(itr_organ_disable != m_organ_disable.end())
				{
					float tmp_time = itr_organ_disable->second;
					tmp_time += (float)result[0]/3600.0;
					m_organ_disable[organ_code[0]] = tmp_time;
				}
				else
					m_organ_disable.insert(make_pair(organ_code[0], (float)result[0]/3600.0));
			}
			else
			{
				int tmp_result = 0;
				for(int i = 0; i < num; i++)
				{
					if(i == 0 && src[0] == 1)
						tmp_result = end_time - gtime[0];
					else if(i == num - 1 && src[i] == 0)
						tmp_result += (gtime[i] - begin_time);
					else if(src[i] == 1)
						tmp_result += (gtime[i - 1] - gtime[i]); 
				}
				ES_DATIMES da;
				da.organ_code = organ_code[0];
				da.dv = dv[0];
				da.count = (float)tmp_result/3600.0;
				m_dv_disable.insert(make_pair(da.dv, da));
				itr_organ_disable = m_organ_disable.find(organ_code[0]);
				if(itr_organ_disable != m_organ_disable.end())
				{
					float tmp_time = itr_organ_disable->second;
					tmp_time += (float)tmp_result/3600.0;
					m_organ_disable[organ_code[0]] = tmp_time;
				}
				else
					m_organ_disable.insert(make_pair(organ_code[0], (float)tmp_result/3600.0));
			}
		}
		else if(num == 0)
		{
			ES_DATIMES da;
			da.organ_code = itr_srcdv_organ->second;
			da.dv = itr_srcdv_organ->first;
			da.count = 0;
			m_dv_disable.insert(make_pair(da.dv, da));
			itr_organ_disable = m_organ_disable.find(itr_srcdv_organ->second);
			if(itr_organ_disable == m_organ_disable.end())
				m_organ_disable.insert(make_pair(itr_srcdv_organ->second, 0));
		}
		else
			continue;
	}
#else
	itr_srcdv_organ = m_srcdv_organ.begin();
	for(; itr_srcdv_organ != m_srcdv_organ.end(); itr_srcdv_organ++)
	{
		num = m_dv_datt.count(itr_srcdv_organ->first);

		if(num > 0)
		{
			itr_dv_datt = m_dv_datt.find(itr_srcdv_organ->first);

			float result;
			if(itr_dv_datt->second.src == 0)
			{
				result = 0;
			}
			else
			{
				result = 86400;
			}

			ES_DATIMES da;
			da.organ_code = itr_dv_datt->second.organ_code;
			da.dv = itr_dv_datt->second.dv;
			da.count = result/3600.0;
			m_dv_disable.insert(make_pair(da.dv, da));
			itr_organ_disable = m_organ_disable.find(itr_dv_datt->second.organ_code);
			if(itr_organ_disable != m_organ_disable.end())
			{
				float tmp_time = itr_organ_disable->second;
				tmp_time += result/3600.0;
				m_organ_disable[itr_dv_datt->second.organ_code] = tmp_time;
			}
			else
				m_organ_disable.insert(make_pair(itr_dv_datt->second.organ_code, result/3600.0));
		}
		else if(num == 0)
		{
			ES_DATIMES da;
			da.organ_code = itr_srcdv_organ->second;
			da.dv = itr_srcdv_organ->first;
			da.count = 0;
			m_dv_disable.insert(make_pair(da.dv, da));
			itr_organ_disable = m_organ_disable.find(itr_srcdv_organ->second);
			if(itr_organ_disable == m_organ_disable.end())
				m_organ_disable.insert(make_pair(itr_srcdv_organ->second, 0));
		}
		else
			continue;
	}
#endif

	return 0;
}

int StatisDA::GetDASuccessTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "select distinct(e.da),e.organ_code,e.dv,d.cc from evalusystem.detail.dagc e,(select "
		"a.da,count(*) cc from evalusystem.detail.yxbw c,(select * from evalusystem.detail.dagc "
		"where step=0 and send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time <to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')) a,(select * from "
		"evalusystem.detail.dagc where step=4 and send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"and send_time <to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS'))	b where a.da=b.da and a.cb=c.cb "
		"and c.gtime>=a.gtime and c.gtime<=b.gtime group by a.da) d where d.da=e.da",
		byear, bmonth, bday, year, month, day, byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_ACC da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_AccuracyTime_Day.txt");
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
					da.da = string(tempstr);
					break;
				case 1:
					da.organ_code = *(int *)(tempstr);
					break;
				case 2:
					da.dv = string(tempstr);
					break;
				case 3:
					da.count = (float)*(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_DATIMES da_result;
			da_result.dv = da.dv;
			da_result.organ_code = da.organ_code;
			if(da.count == 1)
			{
				map<string, ES_DATIMES>::iterator itr_dv_success = m_dv_success.find(da.dv);
				if(itr_dv_success != m_dv_success.end())
				{
					float tmp_count = itr_dv_success->second.count;
					da_result.count = ++tmp_count;
					m_dv_success[da.dv] = da_result;
				}
				else
				{
					da_result.count = 1;
					m_dv_success.insert(make_pair(da.dv, da_result));
				}
				
				map<int, float>::iterator itr_organ_success = m_organ_success.find(da.organ_code);
				if(itr_organ_success != m_organ_success.end())
				{
					float tmp_count = itr_organ_success->second;
					m_organ_success[da.organ_code] = ++tmp_count;
				}
				else
				{
					m_organ_success.insert(make_pair(da.organ_code, 1));
				}
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tda:"<<da.da<<"\tcount"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA成功次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDAFinishTimesByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 2];
	char tmp_sql[MAX_SQL_LEN * 2];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_MONTH, year, month ,day, &byear, &bmonth, &bday);

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

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) a where a.code='datimes' and a.dimname='馈线' "
		"group by organ_code,dimvalue",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Finish_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.dv = string(tempstr);
					break;
				case 2:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_finish.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_finish = m_organ_finish.find(da.organ_code);
			if(itr_organ_finish != m_organ_finish.end())
			{
				float count = itr_organ_finish->second;
				count += da.count;
				m_organ_finish[da.organ_code] = count;
			}
			else
			{
				m_organ_finish.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA月完成次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDAStartTimesByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 2];
	char tmp_sql[MAX_SQL_LEN * 2];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

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

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='dastatimes' and dimname='馈线' "
		"group by organ_code,dimvalue",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_Start_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.dv = string(tempstr);
					break;
				case 2:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_start.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_start = m_organ_start.find(da.organ_code);
			if(itr_organ_start != m_organ_start.end())
			{
				float count = itr_organ_start->second;
				count += da.count;
				m_organ_start[da.organ_code] = count;
			}
			else
			{
				m_organ_start.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA月启动次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDADisableTimeByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 2];
	char tmp_sql[MAX_SQL_LEN * 2];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

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

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='datytime' and dimname='馈线' "
		"group by organ_code,dimvalue",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_DisableTime_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.dv = string(tempstr);
					break;
				case 2:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_disable.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_disable = m_organ_disable.find(da.organ_code);
			if(itr_organ_disable != m_organ_disable.end())
			{
				float count = itr_organ_disable->second;
				count += da.count;
				m_organ_disable[da.organ_code] = count;
			}
			else
			{
				m_organ_disable.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetDASuccessTimesByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 2];
	char tmp_sql[MAX_SQL_LEN * 2];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

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

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='darigtimes' and dimname='馈线' "
		"group by organ_code,dimvalue",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DA_SuccessTime_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.dv = string(tempstr);
					break;
				case 2:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_success.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_success = m_organ_success.find(da.organ_code);
			if(itr_organ_success != m_organ_success.end())
			{
				float count = itr_organ_success->second;
				count += da.count;
				m_organ_success[da.organ_code] = count;
			}
			else
			{
				m_organ_success.insert(make_pair(da.organ_code, da.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DA月成功次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	
	return 0;
}

int StatisDA::InsertResultDASuccessRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_suc_result;
	map<string, int>::iterator itr_srcdv_organ;

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
	
	//DA成功率按主站
	rec_num = m_suc_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_suc_result = m_suc_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "dasucess", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_suc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_suc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_suc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA成功率按馈线
	offset = 0;
	rec_num = m_suc_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_suc_result = m_suc_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "dasucess", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_suc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_suc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_suc_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_suc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA成功率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA执行完成次数按主站
	offset = 0;
	rec_num = m_organ_finish.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_finish = m_organ_finish.begin();
	for(; itr_organ_finish != m_organ_finish.end(); itr_organ_finish++)
	{
		memcpy(m_pdata + offset, "datimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_finish->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_finish->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
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
// 			plog->WriteLog(2, "DA完成次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA执行完成次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA执行完成次数按馈线
	offset = 0;
	rec_num = m_dv_finish.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_finish = m_dv_finish.begin();
	for(; itr_dv_finish != m_dv_finish.end(); itr_dv_finish++)
	{
		memcpy(m_pdata + offset, "datimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		//cout<<itr_dv_finish->first<<" "<<itr_srcdv_organ->second<<" "<<m_srcdv_organ[itr_dv_finish->first]<<endl;
		memcpy(m_pdata + offset, &itr_dv_finish->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_finish->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_finish->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA完成次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA执行完成次数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA启动次数按主站
	offset = 0;
	rec_num = m_organ_start.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_start = m_organ_start.begin();
	for(; itr_organ_start != m_organ_start.end(); itr_organ_start++)
	{
		memcpy(m_pdata + offset, "dastatimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_start->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_start->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
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
// 			plog->WriteLog(2, "DA启动次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA启动次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA启动次数按馈线
	offset = 0;
	rec_num = m_dv_start.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_start = m_dv_start.begin();
	for(; itr_dv_start != m_dv_start.end(); itr_dv_start++)
	{
		memcpy(m_pdata + offset, "dastatimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_start->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_start->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_start->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA启动次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA启动次数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDA::InsertResultDAAbleRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_able_result;
	map<string, int>::iterator itr_srcdv_organ;

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

	//DA可用率按主站
	rec_num = m_able_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_able_result = m_able_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "daval", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_able_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_able_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_able_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA可用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA可用率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA可用率按馈线
	offset = 0;
	rec_num = m_able_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_able_result = m_able_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "daval", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_able_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_able_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_able_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_able_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA可用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA可用率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA线路总数按主站
	offset = 0;
	rec_num = m_organ_lines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_lines = m_organ_lines.begin();
	for(; itr_organ_lines != m_organ_lines.end(); itr_organ_lines++)
	{
		memcpy(m_pdata + offset, "dalines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_lines->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_lines->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "条数", col_attr[3].data_size);
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
// 			plog->WriteLog(2, "DA线路总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA线路总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA线路停用时间之和按主站
	offset = 0;
	rec_num = m_organ_disable.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_disable = m_organ_disable.begin();
	for(; itr_organ_disable != m_organ_disable.end(); itr_organ_disable++)
	{
		memcpy(m_pdata + offset, "datytime", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_disable->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_disable->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "小时", col_attr[3].data_size);
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
// 			plog->WriteLog(2, "DA线路停用时间之和入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA线路停用时间之和按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA线路停用时间之和按馈线
	offset = 0;
	rec_num = m_dv_disable.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_disable = m_dv_disable.begin();
	for(; itr_dv_disable != m_dv_disable.end(); itr_dv_disable++)
	{
		memcpy(m_pdata + offset, "datytime", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_disable->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_disable->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "小时", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_disable->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA线路停用时间之和入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA线路停用时间之和按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDA::InsertResultDAAccuracyRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_acc_result;
	map<string, int>::iterator itr_srcdv_organ;

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

	//DA正确率按主站
	rec_num = m_acc_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_acc_result = m_acc_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "daright", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_acc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_acc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_acc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA正确率按馈线
	offset = 0;
	rec_num = m_acc_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_acc_result = m_acc_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "daright", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_acc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_acc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_acc_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_acc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA正确率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA执行正确次数按主站
	offset = 0;
	rec_num = m_organ_success.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_success = m_organ_success.begin();
	for(; itr_organ_success != m_organ_success.end(); itr_organ_success++)
	{
		memcpy(m_pdata + offset, "darigtimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_success->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_success->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
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
// 			plog->WriteLog(2, "DA执行正确次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA执行正确次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DA执行正确次数按馈线
	offset = 0;
	rec_num = m_dv_success.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_success = m_dv_success.begin();
	for(; itr_dv_success != m_dv_success.end(); itr_dv_success++)
	{
		memcpy(m_pdata + offset, "darigtimes", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_success->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_success->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "次数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_success->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DA执行正确次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DA执行正确次数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

void StatisDA::Clear()
{
	m_srcdv_organ.clear();
	m_dv_finish.clear();
	m_organ_finish.clear();
	m_dv_start.clear();
	m_organ_start.clear();
	m_organ_lines.clear();
	m_dv_disable.clear();
	m_organ_disable.clear();
	m_dv_datt.clear();
	m_dv_success.clear();
	m_organ_success.clear();
	m_suc_result.clear();
	m_able_result.clear();
	m_acc_result.clear();
	m_organ_fatylines.clear();
	m_organ_falines.clear();
	m_ty_result.clear();
	/*m_ty_score.clear();
	m_code_weight.clear();*/
}

int StatisDA::GetDARate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);
#if 0
	int s_time;
	if(statis_type == STATIS_DAY)
		s_time = 1 * 24;
	else
	{
		int tmp_byear, tmp_bmon, tmp_bday;
		int tmp_year, tmp_mon, tmp_day;
		int days;
		GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
		GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
		if(tmp_byear != year || tmp_bmon != month)
		{
			tmp_bday = GetDaysInMonth(year, month);
		}
		days = tmp_bday;
		s_time = days * 24;
	}
	
	//DA正确率
	std::map<std::string, int>::iterator itr_dv_organ;
	std::map<std::string, ES_DATIMES>::iterator itr_dv_finish;
	std::map<int, float>::iterator itr_organ_finish;
	std::map<std::string, ES_DATIMES>::iterator itr_dv_start;
	std::map<int, float>::iterator itr_organ_start;

	//按主站
	itr_organ_start = m_organ_start.begin();
	for(; itr_organ_start != m_organ_start.end(); itr_organ_start++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_start->first;
		itr_organ_finish = m_organ_finish.find(itr_organ_start->first);
		if(itr_organ_finish != m_organ_finish.end())
		{
			es_result.m_fresult = (float)itr_organ_finish->second / (float)itr_organ_start->second;
			m_suc_result.insert(make_pair(DIM_DMS, es_result));
		}
		else
		{
			es_result.m_fresult = 0;
			m_suc_result.insert(make_pair(DIM_DMS, es_result));
		}
	}

	//按馈线
	itr_dv_start = m_dv_start.begin();
	for(; itr_dv_start != m_dv_start.end(); itr_dv_start++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_start->first;
		es_result.m_ntype = itr_dv_start->second.organ_code;
		itr_dv_finish = m_dv_finish.find(itr_dv_start->first);
		if(itr_dv_finish != m_dv_finish.end())
		{
			es_result.m_fresult = (float)itr_dv_finish->second.count / (float)itr_dv_start->second.count;
			m_suc_result.insert(make_pair(DIM_LINE, es_result));
		}
		else
		{
			es_result.m_fresult = 0;
			m_suc_result.insert(make_pair(DIM_LINE, es_result));
		}
	}

	//DA可用率
	map<int, float>::iterator itr_organ_lines;
	map<std::string, int>::iterator itr_srcdv_organ;
	map<std::string, ES_DATIMES>::iterator itr_dv_disable;
	map<int, float>::iterator itr_organ_disable;

	//（全月日历时间×DA线路总数－各DA线路停用时间总和）/（全月日历时间×DA线路总数）×100％
	//按主站
	itr_organ_lines = m_organ_lines.begin();
	for(; itr_organ_lines != m_organ_lines.end(); itr_organ_lines++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_lines->first;
		itr_organ_disable = m_organ_disable.find(itr_organ_lines->first);
		if(itr_organ_disable != m_organ_disable.end())
		{
			float lines = itr_organ_lines->second;
			float total_time = itr_organ_disable->second;
			es_result.m_fresult = (s_time * lines - total_time) / (s_time * lines);
			m_able_result.insert(make_pair(DIM_DMS, es_result));
		}
		else
		{
			es_result.m_fresult = 1.0;
			m_able_result.insert(make_pair(DIM_DMS, es_result));
		}
	}

	//按馈线
	itr_srcdv_organ = m_srcdv_organ.begin();
	for(; itr_srcdv_organ != m_srcdv_organ.end(); itr_srcdv_organ++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_srcdv_organ->first;
		es_result.m_ntype = itr_srcdv_organ->second;
		itr_dv_disable = m_dv_disable.find(itr_srcdv_organ->first);
		if(itr_dv_disable != m_dv_disable.end())
		{
			float lines = 1;
			float total_time = itr_dv_disable->second.count;
			es_result.m_fresult = (s_time * lines - total_time) / (s_time * lines);
		}
		else
		{
			es_result.m_fresult = 1.0;
		}
		m_able_result.insert(make_pair(DIM_LINE, es_result));
	}

	//DA正确率
	map<std::string, ES_DATIMES>::iterator itr_dv_success;
	map<int, float>::iterator itr_organ_success;
	multimap<int, ES_RESULT>::iterator itr_acc_result;

	//按主站
	itr_organ_finish = m_organ_finish.begin();
	for(; itr_organ_finish != m_organ_finish.end(); itr_organ_finish++)
	{
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_finish->first;
		itr_organ_success = m_organ_success.find(itr_organ_finish->first);
		if(itr_organ_success != m_organ_success.end())
		{
			es_result.m_fresult = itr_organ_success->second/itr_organ_finish->second;
			m_acc_result.insert(make_pair(DIM_DMS, es_result));
		}
		else
		{
			es_result.m_fresult = 0;
			m_acc_result.insert(make_pair(DIM_DMS, es_result));
		}
	}

	//按馈线
	itr_dv_finish = m_dv_finish.begin();
	for(; itr_dv_finish != m_dv_finish.end(); itr_dv_finish++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_finish->first;
		es_result.m_ntype = itr_dv_finish->second.organ_code;
		itr_dv_success = m_dv_success.find(itr_dv_finish->first);
		if(itr_dv_success != m_dv_success.end())
		{
			es_result.m_fresult = itr_dv_success->second.count / itr_dv_finish->second.count;
			m_acc_result.insert(make_pair(DIM_LINE, es_result));
		}
		else
		{
			es_result.m_fresult = 0;
			m_acc_result.insert(make_pair(DIM_LINE, es_result));
		}
	}
#endif
	//FA投运率
	map<int, float>::iterator itr_organ_fatylines;
	map<int, float>::iterator itr_organ_falines;
	map<string, float>::iterator itr_code_weight;

	//按主站
	itr_organ_falines = m_organ_falines.begin();
	for(; itr_organ_falines != m_organ_falines.end(); itr_organ_falines++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_falines->first;
		itr_organ_fatylines = m_organ_fatylines.find(itr_organ_falines->first);
		if(itr_organ_fatylines != m_organ_fatylines.end())
		{
			es_result.m_fresult = itr_organ_fatylines->second / itr_organ_falines->second;
		}
		else
			es_result.m_fresult = 0;

		m_ty_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_falines->first;
		itr_code_weight = m_code_weight.find("fatouyun");
		if (itr_code_weight != m_code_weight.end())
		{
		es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
		es_score.m_fresult = 0;

		m_ty_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	return 0;
}

int StatisDA::GetDAProcess(ES_TIME *es_time)
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

	sprintf(sql, "call evalusystem.result.DAPROCESS('%04d%02d%02d 23:59:59','%04d%02d%02d 23:59:59')",
		byear, bmonth, bday, year, month, day);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 		plog->WriteLog(2, "DA数据处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisDA::GetFATouyunLines(int statis_type, ES_TIME *es_time)
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

	/*sprintf(sql, "select a.organ_code,count(a.dv) from evalusystem.detail.datt a,"
		"(select organ_code,dv,max(gtime) gtime from evalusystem.detail.datt group by "
		"organ_code,dv) b where a.organ_code=b.organ_code and a.gtime=b.gtime and a.dv=b.dv "
		"and a.src=0 group by a.organ_code");*/
	sprintf(sql, "select organ_code,count(*) total from evalusystem.detail.datt where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"(type=0 or type=1) group by organ_code", byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING fa;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/FA_Tylines_Day.txt");
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
					fa.organ_code = *(int *)(tempstr);
					break;
				case 1:
					fa.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_fatylines.insert(make_pair(fa.organ_code, fa.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<fa.organ_code<<"\t:"<<fa.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取FA馈线投运数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::GetFALines(int statis_type, ES_TIME *es_time)
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

	/*sprintf(sql, "select a.organ_code,count(a.dv) from evalusystem.detail.datt a,"
		"(select organ_code,dv,max(gtime) gtime from evalusystem.detail.datt group by "
		"organ_code,dv) b where a.organ_code=b.organ_code and a.gtime=b.gtime and a.dv=b.dv "
		"group by a.organ_code");*/
	sprintf(sql, "select organ_code,count(*) total from evalusystem.detail.datt where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by organ_code", byear, bmonth, bday, year, month, day);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING fa;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/FA_FAlines_Day.txt");
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
					fa.organ_code = *(int *)(tempstr);
					break;
				case 1:
					fa.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_falines.insert(make_pair(fa.organ_code, fa.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<fa.organ_code<<"\t:"<<fa.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取FA馈线总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDA::InsertResultFATouyunRate(int statis_type, ES_TIME *es_time)
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
	std::map<int, float>::iterator itr_ty_result;

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

	//FA投运率按主站
	rec_num = m_ty_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ty_result = m_ty_result.begin();
	for(; itr_ty_result != m_ty_result.end(); itr_ty_result++)
	{
		memcpy(m_pdata + offset, "fatouyun", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ty_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ty_result->second, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "FA投运率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "FA投运率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_ty_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ty_result = m_ty_score.begin();
	for(; itr_ty_result != m_ty_score.end(); itr_ty_result++)
	{
		memcpy(m_pdata + offset, "fatouyuns", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ty_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ty_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_ty_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "FA投运率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "FA投运率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//FA馈线投运数按主站
	offset = 0;
	rec_num = m_organ_fatylines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_fatylines = m_organ_fatylines.begin();
	for(; itr_organ_fatylines != m_organ_fatylines.end(); itr_organ_fatylines++)
	{
		memcpy(m_pdata + offset, "fatylines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_fatylines->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_fatylines->second, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "FA馈线投运数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "FA馈线投运数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//FA馈线总数按主站
	offset = 0;
	rec_num = m_organ_falines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_falines = m_organ_falines.begin();
	for(; itr_organ_falines != m_organ_falines.end(); itr_organ_falines++)
	{
		memcpy(m_pdata + offset, "falines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_falines->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_falines->second, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "FA馈线总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "FA馈线总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

/*int StatisDA::GetWeight()
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/STSTISDA_WEIGHT_Day.txt");
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
