/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_terminal.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 终端相关统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <map>
#include <sstream>
#include "statis_terminal.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisTerminal::StatisTerminal(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisTerminal::~StatisTerminal()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

int StatisTerminal::GetTerminalRunningRate(int statis_type, ES_TIME *es_time)
{
	int ret;

	Clear();
	//GetWeight();

	if(statis_type == STATIS_DAY)
	{		
		ret = GetTerminalsByDay(es_time);
		if(ret < 0)
		{
			printf("GetTerminalsByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetOfflineByDay(es_time);
		if(ret < 0)
		{
			printf("GetOfflineByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYKAccTimesByDay(es_time);
		if(ret < 0)
		{
			printf("GetYKAccTimesByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYKTimesByDay(es_time);
		if(ret < 0)
		{
			printf("GetYKTimesByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetCanYKTimesByDay(es_time);
		if(ret < 0)
		{
			printf("GetCanYKTimesByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXAccTimesByDay(es_time);
		if(ret < 0)
		{
			printf("GetYXAccTimesByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXTimesByDay(es_time);
		if(ret < 0)
		{
			printf("GetYXTimesByDay fail.\n");
			goto FAILED_RET;
		}
	}
	else
	{
		ret = GetTerminalsByMonth(es_time);
		if(ret < 0)
		{
			printf("GetTerminalsByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetOfflineByMonth(es_time);
		if(ret < 0)
		{
			printf("GetOfflineByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYKAccTimesByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYKAccTimesByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYKTimesByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYKTimesByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetCanYKTimesByMonth(es_time);
		if(ret < 0)
		{
			printf("GetCanYKTimesByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXAccTimesByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYXAccTimesByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetYXTimesByMonth(es_time);
		if(ret < 0)
		{
			printf("GetYXTimesByMonth fail.\n");
			goto FAILED_RET;
		}
	}	

	//计算
	ret = GetTerminalRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("GetTerminalRate fail.\n");
		goto FAILED_RET;
	}

	//插入数据到统计表中
	ret = InsertResultOnlineRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultOnlineRate fail.\n");
		goto FAILED_RET;
	}
	ret = InsertResultYKAccRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultYKAccRate fail.\n");
		goto FAILED_RET;
	}
	ret = InsertResultYKUsingRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultYKUsingRate fail.\n");
		goto FAILED_RET;
	}
	ret = InsertResultYXAccRate(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultYXAccRate fail.\n");
		goto FAILED_RET;
	}
	return 0;

FAILED_RET:
	ES_ERROR_INFO es_err;
	es_err.statis_type = STATIS_TERMINAL;
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

int StatisTerminal::GetTerminalsByDay(ES_TIME *es_time)
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

	sprintf(sql, "select organ_code,rnum,count(*) from evalusystem.detail.zdzx where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"group by organ_code,rnum",byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			
			//终端编号加地区号防重复处理
			string str_rtu;
			string str_organ;
			ostringstream oss;
			
			oss<<da.organ_code;
			str_organ = oss.str();
			str_rtu = str_organ + "_" + da.dv;

			m_num_terminal.insert(make_pair(str_rtu, da));

			map<int, float>::iterator itr_organ_terminal = m_organ_terminal.find(da.organ_code);
			if(itr_organ_terminal != m_organ_terminal.end())
			{
				float count = itr_organ_terminal->second;
				count += da.count;
				m_organ_terminal[da.organ_code] = count;
			}
			else
			{
				m_organ_terminal.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取配电终端总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetOfflineByDay(ES_TIME *es_time)
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

	sprintf(sql, "select distinct(rnum),organ_code,86400-online from evalusystem.detail.zdzx where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')",byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Day.txt");
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
					da.dv = string(tempstr);
					break;
				case 1:
					da.organ_code = *(int *)(tempstr);
					break;
				case 2:
					da.count = *(int *)(tempstr) / 3600.0;
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			//终端编号加地区号防重复处理
			string str_rtu;
			string str_organ;
			ostringstream oss;

			oss<<da.organ_code;
			str_organ = oss.str();
			str_rtu = str_organ + "_" + da.dv;

			m_num_offline.insert(make_pair(str_rtu, da));

			map<int, float>::iterator itr_organ_offline = m_organ_offline.find(da.organ_code);
			if(itr_organ_offline != m_organ_offline.end())
			{
				float count = itr_organ_offline->second;
				count += da.count;
				m_organ_offline[da.organ_code] = count;
			}
			else
			{
				m_organ_offline.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取终端设备停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYKAccTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "Select organ_code,cb,count(*) from evalusystem.detail.ykcz where result=1 and "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code,cb",
		byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YKAccTimes_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_cb_ykacc.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_ykacc = m_organ_ykacc.find(da.organ_code);
			if(itr_organ_ykacc != m_organ_ykacc.end())
			{
				float count = itr_organ_ykacc->second;
				count += da.count;
				m_organ_ykacc[da.organ_code] = count;
			}
			else
			{
				m_organ_ykacc.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取遥控成功次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYKTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "select organ_code,cb,count(*) from evalusystem.detail.ykcz where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code,cb",
		byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YKTimes_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_cb_yk.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_yk = m_organ_yk.find(da.organ_code);
			if(itr_organ_yk != m_organ_yk.end())
			{
				float count = itr_organ_yk->second;
				count += da.count;
				m_organ_yk[da.organ_code] = count;
			}
			else
			{
				m_organ_yk.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取遥控次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetCanYKTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "select organ_code,sum(c) from (select organ_code,count(*) c from "
		"evalusystem.detail.ykcz where result=1 and "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') "
		"and send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code "
		"union all select organ_code,local from evalusystem.detail.yxinfo where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS')) group by organ_code",
		byear, bmonth, bday, year, month, day, byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_CanYKTimes_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_canyk.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取可遥控操作次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYXAccTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "select organ_code,sum(matchsoe) from evalusystem.detail.yxinfo where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code",
		byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YXAcc_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_yxacc.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取遥控成功次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYXTimesByDay(ES_TIME *es_time)
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

	sprintf(sql, "select organ_code,sum(total) from evalusystem.detail.yxinfo where "
		"send_time>to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and "
		"send_time<to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code",
		byear, bmonth, bday, year, month, day);

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YXTimes_Day.txt");
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
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_yx.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取遥信次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetTerminalsByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,dimvalue,max(value) from (%s) a where a.code='rtunum' and a.dimname='终端' "
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Month.txt");
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

			//终端编号加地区号防重复处理
			string str_rtu;
			string str_organ;
			ostringstream oss;

			oss<<da.organ_code;
			str_organ = oss.str();
			str_rtu = str_organ + "_" + da.dv;

			m_num_terminal.insert(make_pair(str_rtu, da));
			map<int, float>::iterator itr_organ_terminal = m_organ_terminal.find(da.organ_code);
			if(itr_organ_terminal != m_organ_terminal.end())
			{
				float count = itr_organ_terminal->second;
				count += da.count;
				m_organ_terminal[da.organ_code] = count;
			}
			else
			{
				m_organ_terminal.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取配电终端运行月统计数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetOfflineByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='rtuty' and dimname='终端' "
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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

			//终端编号加地区号防重复处理
			string str_rtu;
			string str_organ;
			ostringstream oss;

			oss<<da.organ_code;
			str_organ = oss.str();
			str_rtu = str_organ + "_" + da.dv;

			m_num_offline.insert(make_pair(str_rtu, da));
			map<int, float>::iterator itr_organ_offline = m_organ_offline.find(da.organ_code);
			if(itr_organ_offline != m_organ_offline.end())
			{
				float count = itr_organ_offline->second;
				count += da.count;
				m_organ_offline[da.organ_code] = count;
			}
			else
			{
				m_organ_offline.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYKAccTimesByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='rconrignum' and dimname='开关' "
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YKAccTimes_Month.txt");
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

			m_cb_ykacc.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_ykacc = m_organ_ykacc.find(da.organ_code);
			if(itr_organ_ykacc != m_organ_ykacc.end())
			{
				float count = itr_organ_ykacc->second;
				count += da.count;
				m_organ_ykacc[da.organ_code] = count;
			}
			else
			{
				m_organ_ykacc.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取遥控成功月次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYKTimesByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,dimvalue,sum(value) from (%s) where code='rconnum' and dimname='开关' "
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YKTimes_Month.txt");
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

			m_cb_yk.insert(make_pair(da.dv, da));
			map<int, float>::iterator itr_organ_yk = m_organ_yk.find(da.organ_code);
			if(itr_organ_yk != m_organ_yk.end())
			{
				float count = itr_organ_yk->second;
				count += da.count;
				m_organ_yk[da.organ_code] = count;
			}
			else
			{
				m_organ_yk.insert(make_pair(da.organ_code, da.count));
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
// 		plog->WriteLog(2, "获取遥控月次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetCanYKTimesByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,sum(value) from (%s) a where a.code='krconnum' and a.dimname='无' "
		"group by organ_code",tmp_str.c_str());

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_CanYKTimes_Month.txt");
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
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_canyk.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取月可遥控次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYXAccTimesByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,sum(value) from (%s) a where a.code='pointrighnum' and a.dimname='无' "
		"group by organ_code",tmp_str.c_str());

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YXAccTimes_Month.txt");
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
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_yxacc.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取月遥信成功次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetYXTimesByMonth(ES_TIME *es_time)
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
	sprintf(sql, "select organ_code,sum(value) from (%s) a where a.code='pointnum' and a.dimname='无' "
		"group by organ_code",tmp_str.c_str());

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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_YXTimes_Month.txt");
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
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_yx.insert(make_pair(da.organ_code, da.count));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取月遥信次数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisTerminal::GetTerminalRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

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

	//终端在线率
	//（全月日历时间×配电终端总数－各配电终端设备停用时间总和）/（全月日历时间×配电终端总数）
	map<string, ES_DATIMES>::iterator itr_num_terminal;
	map<int, float>::iterator itr_organ_terminal;
	map<string, ES_DATIMES>::iterator itr_num_offline;
	map<int, float>::iterator itr_organ_offline;
	map<string, float>::iterator itr_code_weight;

	//按主站
	itr_organ_terminal = m_organ_terminal.begin();
	for(; itr_organ_terminal != m_organ_terminal.end(); itr_organ_terminal++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_terminal->first;
		itr_organ_offline = m_organ_offline.find(itr_organ_terminal->first);
		if(itr_organ_offline != m_organ_offline.end())
		{
			es_result.m_fresult = (s_time * itr_organ_terminal->second - itr_organ_offline->second) / 
						(s_time * itr_organ_terminal->second);
		}
		else
		{
			es_result.m_fresult = 0.0;
		}

		m_online_result.insert(make_pair(DIM_DMS, es_result));

		/*es_score.m_ntype = itr_organ_terminal->first;
		itr_code_weight = m_code_weight.find("rtuzx");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;
		m_online_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按终端
	itr_num_terminal = m_num_terminal.begin();
	for(; itr_num_terminal != m_num_terminal.end(); itr_num_terminal++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_num_terminal->first;
		es_result.m_ntype = itr_num_terminal->second.organ_code;
		itr_num_offline = m_num_offline.find(itr_num_terminal->first);
		if(itr_num_offline != m_num_offline.end())
		{
			es_result.m_fresult = (s_time * itr_num_terminal->second.count - itr_num_offline->second.count) / 
						(s_time * itr_num_terminal->second.count);
		}
		else
		{
			es_result.m_fresult = 0.0;
		}

		m_online_result.insert(make_pair(DIM_TERMINAL, es_result));
	}

	//遥控成功率
	//（考核期内遥控成功次数）/（考核期内遥控次数总和）
	map<std::string, ES_DATIMES>::iterator itr_cb_ykacc;
	map<int, float>::iterator itr_organ_ykacc;
	map<std::string, ES_DATIMES>::iterator itr_cb_yk;
	map<int, float>::iterator itr_organ_yk;

	//按主站
	itr_organ_yk = m_organ_yk.begin();
	for(; itr_organ_yk != m_organ_yk.end(); itr_organ_yk++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_yk->first;
		itr_organ_ykacc = m_organ_ykacc.find(itr_organ_yk->first);
		if(itr_organ_ykacc != m_organ_ykacc.end())
			es_result.m_fresult = itr_organ_ykacc->second / itr_organ_yk->second;
		else
			es_result.m_fresult = 0;

		m_ykacc_result.insert(make_pair(DIM_DMS, es_result));

		/*es_score.m_ntype = itr_organ_yk->first;
		itr_code_weight = m_code_weight.find("rcontr");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_ykacc_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按开关
	itr_cb_yk = m_cb_yk.begin();
	for(; itr_cb_yk != m_cb_yk.end(); itr_cb_yk++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_cb_yk->first;
		es_result.m_ntype = itr_cb_yk->second.organ_code;
		itr_cb_ykacc = m_cb_ykacc.find(itr_cb_yk->first);
		if(itr_cb_ykacc != m_cb_ykacc.end())
			es_result.m_fresult = itr_cb_ykacc->second.count / itr_cb_yk->second.count;
		else
			es_result.m_fresult = 0;

		m_ykacc_result.insert(make_pair(DIM_CB, es_result));
	}

	//遥控使用率
	//（考核期内实际遥控次数）/（考核期内可遥控操作次数的总和）
	map<int, float>::iterator itr_organ_canyk;

	//按主站
	itr_organ_canyk = m_organ_canyk.begin();
	for(; itr_organ_canyk != m_organ_canyk.end(); itr_organ_canyk++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_canyk->first;
		itr_organ_ykacc = m_organ_ykacc.find(itr_organ_canyk->first);
		if(itr_organ_ykacc != m_organ_ykacc.end())
			es_result.m_fresult = itr_organ_ykacc->second / itr_organ_canyk->second;
		else
			es_result.m_fresult = 0;

		m_ykusing_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_canyk->first;
		itr_code_weight = m_code_weight.find("rconuse");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_ykusing_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//遥信正确率
	//（遥信正确动作次数）/（遥信动作总次数）
	map<int, float>::iterator itr_organ_yxacc;
	map<int, float>::iterator itr_organ_yx;

	//按主站
	itr_organ_yx = m_organ_yx.begin();
	for(; itr_organ_yx != m_organ_yx.end(); itr_organ_yx++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_yx->first;
		itr_organ_yxacc = m_organ_yxacc.find(itr_organ_yx->first);
		if(itr_organ_yxacc != m_organ_yxacc.end())
		{
			if(itr_organ_yx->second == 0)
				es_result.m_fresult = 1;
			else
				es_result.m_fresult = itr_organ_yxacc->second / itr_organ_yx->second;
		}
		else
			es_result.m_fresult = 0;

		m_yxacc_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_yxacc->first;
		itr_code_weight = m_code_weight.find("pointrigh");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = itr_code_weight->second*es_result.m_fresult;
		else
			es_score.m_fresult = 0;

		m_yxacc_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}
	
	return 0;
}

int StatisTerminal::InsertResultOnlineRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_online_result;

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

	//终端在线率按主站
	rec_num = m_online_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_online_result = m_online_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rtuzx", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_online_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_online_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_online_result = m_online_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rtuzxs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_online_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//终端在线率按终端
	offset = 0;
	rec_num = m_online_result.count(DIM_TERMINAL);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_online_result = m_online_result.find(DIM_TERMINAL);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rtuzx", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_online_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "终端", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		string tmp_str = itr_online_result->second.m_strtype;
		string str_rtu = string(tmp_str.begin()+6, tmp_str.end());
		memcpy(m_pdata + offset, str_rtu.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_online_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "终端在线率按终端当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//配电终端总数按主站
	offset = 0;
	rec_num = m_organ_terminal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_terminal = m_organ_terminal.begin();
	for(; itr_organ_terminal != m_organ_terminal.end(); itr_organ_terminal++)
	{
		memcpy(m_pdata + offset, "rtunum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_terminal->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_terminal->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "配电终端总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电终端总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//配电终端总数按终端
	offset = 0;
	rec_num = m_num_terminal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_num_terminal = m_num_terminal.begin();
	for(; itr_num_terminal != m_num_terminal.end(); itr_num_terminal++)
	{
		memcpy(m_pdata + offset, "rtunum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_num_terminal->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_num_terminal->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "终端", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		string tmp_str = itr_num_terminal->first;
		string str_rtu = string(tmp_str.begin()+6, tmp_str.end());
		memcpy(m_pdata + offset, str_rtu.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电终端总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电终端总数按终端当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//配电终端设备停用时间按主站
	offset = 0;
	rec_num = m_organ_offline.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_offline = m_organ_offline.begin();
	for(; itr_organ_offline != m_organ_offline.end(); itr_organ_offline++)
	{
		memcpy(m_pdata + offset, "rtuty", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_offline->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_offline->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "配电终端设备停用时间入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电终端设备停用时间按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//配电终端设备停用时间按终端
	offset = 0;
	rec_num = m_num_offline.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_num_offline = m_num_offline.begin();
	for(; itr_num_offline != m_num_offline.end(); itr_num_offline++)
	{
		memcpy(m_pdata + offset, "rtuty", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_num_offline->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_num_offline->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "小时", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "终端", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		string tmp_str = itr_num_offline->first;
		string str_rtu = string(tmp_str.begin()+6, tmp_str.end());
		memcpy(m_pdata + offset, str_rtu.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电终端设备停用时间入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电终端设备停用时间按终端当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisTerminal::InsertResultYKAccRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_ykacc_result;

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

	//遥控成功率按主站
	rec_num = m_ykacc_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ykacc_result = m_ykacc_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rcontr", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_ykacc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "遥控成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_ykacc_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ykacc_result = m_ykacc_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rcontrs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_ykacc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "遥控成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "遥控成功率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//遥控成功率按开关
	offset = 0;
	rec_num = m_ykacc_result.count(DIM_CB);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ykacc_result = m_ykacc_result.find(DIM_CB);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "rcontr", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ykacc_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "开关", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_ykacc_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_ykacc_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "遥控成功率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控成功率按开关当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥控成功次数按主站
	offset = 0;
	rec_num = m_organ_ykacc.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_ykacc = m_organ_ykacc.begin();
	for(; itr_organ_ykacc != m_organ_ykacc.end(); itr_organ_ykacc++)
	{
		memcpy(m_pdata + offset, "rconrignum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_ykacc->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_ykacc->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥控成功次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控成功次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥控成功次数按开关
	offset = 0;
	rec_num = m_cb_ykacc.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_cb_ykacc = m_cb_ykacc.begin();
	for(; itr_cb_ykacc != m_cb_ykacc.end(); itr_cb_ykacc++)
	{
		memcpy(m_pdata + offset, "rconrignum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cb_ykacc->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cb_ykacc->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "开关", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_cb_ykacc->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "遥控成功次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控成功次数按开关当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥控次数总和按主站
	offset = 0;
	rec_num = m_organ_yk.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_yk = m_organ_yk.begin();
	for(; itr_organ_yk != m_organ_yk.end(); itr_organ_yk++)
	{
		memcpy(m_pdata + offset, "rconnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_yk->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_yk->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥控次数总和入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控次数总和按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥控次数总和按开关
	offset = 0;
	rec_num = m_cb_yk.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_cb_yk = m_cb_yk.begin();
	for(; itr_cb_yk != m_cb_yk.end(); itr_cb_yk++)
	{
		memcpy(m_pdata + offset, "rconnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cb_yk->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cb_yk->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "开关", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_cb_yk->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "遥控次数总和入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控次数总和按开关当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisTerminal::InsertResultYKUsingRate(int statis_type, ES_TIME *es_time)
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
	map<int, float>::iterator itr_ykusing_result;

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

	//遥控使用率按主站
	rec_num = m_ykusing_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ykusing_result = m_ykusing_result.begin();
	for(; itr_ykusing_result != m_ykusing_result.end(); itr_ykusing_result++)
	{
		memcpy(m_pdata + offset, "rconuse", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ykusing_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ykusing_result->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥控使用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥控使用率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_ykusing_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_ykusing_result = m_ykusing_score.begin();
	for(; itr_ykusing_result != m_ykusing_score.end(); itr_ykusing_result++)
	{
		memcpy(m_pdata + offset, "rconuses", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_ykusing_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_ykusing_result->second, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "遥控使用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "遥控使用率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//实际遥控次数按主站
	offset = 0;
	rec_num = m_organ_ykacc.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_ykacc = m_organ_ykacc.begin();
	for(; itr_organ_ykacc != m_organ_ykacc.end(); itr_organ_ykacc++)
	{
		memcpy(m_pdata + offset, "rerconnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_ykacc->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_ykacc->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "实际遥控次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "实际遥控次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//可遥控操作次数按主站
	offset = 0;
	rec_num = m_organ_canyk.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_canyk = m_organ_canyk.begin();
	for(; itr_organ_canyk != m_organ_canyk.end(); itr_organ_canyk++)
	{
		memcpy(m_pdata + offset, "krconnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_canyk->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_canyk->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "可遥控操作次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "可遥控操作次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisTerminal::InsertResultYXAccRate(int statis_type, ES_TIME *es_time)
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
	map<int, float>::iterator itr_yxacc_result;

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

	//遥信正确率按主站
	rec_num = m_yxacc_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_yxacc_result = m_yxacc_result.begin();
	for(; itr_yxacc_result != m_yxacc_result.end(); itr_yxacc_result++)
	{
		memcpy(m_pdata + offset, "pointrigh", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_yxacc_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_yxacc_result->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥信正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥信正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_yxacc_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_yxacc_result = m_yxacc_score.begin();
	for(; itr_yxacc_result != m_yxacc_score.end(); itr_yxacc_result++)
	{
		memcpy(m_pdata + offset, "pointrighs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_yxacc_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_yxacc_result->second, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "遥信正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "遥信正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//遥信正确动作次数按主站
	offset = 0;
	rec_num = m_organ_yxacc.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_yxacc = m_organ_yxacc.begin();
	for(; itr_organ_yxacc != m_organ_yxacc.end(); itr_organ_yxacc++)
	{
		memcpy(m_pdata + offset, "pointrighnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_yxacc->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_yxacc->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥信正确动作次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥信正确动作次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//遥信动作总次数按主站
	offset = 0;
	rec_num = m_organ_yx.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_yx = m_organ_yx.begin();
	for(; itr_organ_yx != m_organ_yx.end(); itr_organ_yx++)
	{
		memcpy(m_pdata + offset, "pointnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_yx->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_yx->second, col_attr[2].data_size);
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
// 			plog->WriteLog(2, "遥信动作总次数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "遥信动作总次数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

void StatisTerminal::Clear()
{
	m_num_terminal.clear();
	m_organ_terminal.clear();
	m_num_offline.clear();
	m_organ_offline.clear();
	m_cb_ykacc.clear();
	m_organ_ykacc.clear();
	m_cb_yk.clear();
	m_organ_yk.clear();
	m_organ_canyk.clear();
	m_organ_yxacc.clear();
	m_organ_yx.clear();
	m_online_result.clear();
	m_ykacc_result.clear();
	m_ykusing_result.clear();
	m_yxacc_result.clear();
	//m_code_weight.clear();
}

/*int StatisTerminal::GetWeight()
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
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/StatisTerminal_WEIGHT_Day.txt");
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
