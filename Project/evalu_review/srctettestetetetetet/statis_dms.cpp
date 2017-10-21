/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dms.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 主站运行率统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <stdlib.h>
#include "statis_dms.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisDMS::StatisDMS(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	/*m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);*/
}

StatisDMS::~StatisDMS()
{
	m_co_result.clear();
	//m_oci->DisConnect(&err_info);
}

int StatisDMS::GetDMSRunningRate(int statis_type, ES_TIME *es_time)
{
	int year, mon, day;
	int byear, bmon, bday;
	int ret, num;
	int rec_num;
	int col_num;
	int total_time;
	float m_fresult;
	//float m_score;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	multimap<int, ES_ERROR_INFO>::iterator itr_type_err;
	map<string, float>::iterator itr_code_weight;
	
	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;
#ifdef DEBUG_DATE
	year = 2014;
	day = 14;
	mon = 6;
#endif

	//GetWeight();


	/*if(mon == 1 && day == 1)
	{
		byear = year - 1;
		bmon = 12;
		bday = 31;
	}
	else if(mon != 1 && day == 1)
	{
		byear = year;
		bmon = mon - 1;
		bday = GetDaysInMonth(year, bmon);
	}
	else
	{
		byear = year;
		bmon = mon;
		bday = day - 1;
	}*/

	GetBeforeTime(STATIS_DAY, year, mon ,day, &byear, &bmon, &bday);
	
	if(statis_type == STATIS_DAY)
	{
		
		total_time = 86400;
	}
	else
	{
		int tmp_byear, tmp_bmon, tmp_bday;
		int tmp_year, tmp_mon, tmp_day;
		int tmp_days;
		GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
#ifdef DEBUG_DATE
		tmp_year = 2014;
		tmp_day = 14;
		tmp_mon = 6;
#endif
		if(tmp_mon == 1 && tmp_day == 1)
		{
			tmp_byear = tmp_year - 1;
			tmp_bmon = 12;
			tmp_bday = 31;
		}
		else if(tmp_mon != 1 && tmp_day == 1)
		{
			tmp_byear = tmp_year;
			tmp_bmon = tmp_mon - 1;
			tmp_bday = GetDaysInMonth(tmp_year, tmp_bmon);
		}
		else
		{
			tmp_byear = tmp_year;
			tmp_bmon = tmp_mon;
			tmp_bday = tmp_day - 1;
		}

		if(tmp_byear != byear || tmp_bmon != bmon)
		{
			tmp_bday = GetDaysInMonth(byear, bmon);
		}
		
		total_time = tmp_bday * 86400;
	}
	
	if(statis_type == STATIS_DAY)
		sprintf(sql, "select organ_code,sum(online) from evalusystem.detail.dmsop where "
			"send_time > to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') and send_time < "
			"to_date('%04d-%02d-%02d 23:59:59','YYYY-MM-DD HH24:MI:SS') group by organ_code", 
			byear, bmon, bday, year, mon, day);
	else
		sprintf(sql, "select organ_code,sum(online) from evalusystem.detail.dmsop where "
			"count_time like '%04d-%02d%%' group by organ_code", byear, bmon);

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dmsrun;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		if(statis_type == STATIS_DAY)
			sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DMS_Running_Day.txt");
		else
			sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DMS_Running_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			memset(&dmsrun, 0, sizeof(ES_DMSRUNNING));
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dmsrun.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dmsrun.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			
			m_fresult = (float)dmsrun.online / (float)total_time;
			m_co_result.insert(make_pair(dmsrun.organ_code, m_fresult));

			/*itr_code_weight = m_code_weight.find("dmsaveop");
			if (itr_code_weight != m_code_weight.end())
				m_score = m_fresult*itr_code_weight->second;
			else	
				m_score = 0;
			m_co_score.insert(make_pair(dmsrun.organ_code,m_score));*/
#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dmsrun.organ_code<<"\tonline:"<<dmsrun.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		ES_ERROR_INFO es_err;
		es_err.statis_type = STATIS_DMS;
		es_err.es_time.year = year;
		es_err.es_time.month = mon;
		es_err.es_time.day = day;

		num = m_type_err->count(statis_type);
		itr_type_err = m_type_err->find(statis_type);
		for (int i = 0; i < num; i++)
		{
			if(itr_type_err->second == es_err)
			{
				printf("ReadData: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
// 				plog->WriteLog(2, "获取主站在线时长失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 					err_info.error_no, err_info.error_info, __FILE__, __LINE__);
				return -1;
			}
			
			itr_type_err++;
		}
		m_type_err->insert(make_pair(statis_type, es_err));
		printf("ReadData: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "DMS统计ReadData失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	ret = InsertResult(statis_type, byear, bmon, bday);
	if(ret < 0)
	{
		ES_ERROR_INFO es_err;
		es_err.statis_type = STATIS_DMS;
		es_err.es_time.year = year;
		es_err.es_time.month = mon;
		es_err.es_time.day = day;

		num = m_type_err->count(statis_type);
		itr_type_err = m_type_err->find(statis_type);
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

	m_co_result.clear();

	return 0;
}

int StatisDMS::InsertResult(int statis_type, int year, int mon, int day)
{
	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];

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

	//将数据存入到buffer中
	rec_num = m_co_result.size();

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int ,float>::iterator itr_co_result = m_co_result.begin();
	for( ; itr_co_result != m_co_result.end(); itr_co_result++)
	{
		memcpy(m_pdata + offset, "dmsaveop", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_co_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_co_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}
	
	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, mon, day);
	else
		sprintf(table_name, "month_%04d%02d",year, mon);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, mon, day);

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "主站运行情况入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电主站运行情况当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, mon, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_co_score.size();

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_co_result = m_co_score.begin();
	for( ; itr_co_result != m_co_score.end(); itr_co_result++)
	{
		memcpy(m_pdata + offset, "dmsaveops", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_co_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_co_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, mon, day);
	else
		sprintf(table_name, "month_%04d%02d",year, mon);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, mon, day);

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "主站运行情况入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电主站运行情况当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, mon, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	return 0;
}

/*int StatisDMS::GetWeight()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	m_code_weight.clear();

	sprintf(sql,"select code,weight from evalusystem.config.weight;");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_WEIGHT weight;
		char *tempstr = (char *)malloc(30);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_WEIGHT_Day.txt");
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
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}*/

