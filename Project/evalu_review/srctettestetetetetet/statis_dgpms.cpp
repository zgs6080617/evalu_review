/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_dgpms.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 设备类相关统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <map>
#include "statis_dgpms.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisDGPMS::StatisDGPMS(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	/*m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);*/
}

StatisDGPMS::~StatisDGPMS()
{
	Clear();
	//m_oci->DisConnect(&err_info);
}

int StatisDGPMS::GetGMSGPMSInteractiveRate(int statis_type, ES_TIME *es_time)
{
	int ret;

	Clear();
	//GetWeight();
	
	if(statis_type == STATIS_DAY)
	{	
// 		ret = GetDMSLinesByDay(es_time);
// 		if(ret < 0)
// 			goto FAILED_RET;
// 		ret = GetGPMSLinesByDay(es_time);
// 		if(ret < 0)
// 			goto FAILED_RET;
		ret = GetDMSCBsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSCBsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSDISsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSDISsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSLDsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSLDsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGpRightsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSGpDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		/*ret = GetGPMSGpDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSGPMSGpDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;*/
		ret = GetSyncRightDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSStatusDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSStatusDevsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSBUSsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSBUSsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSSTsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPMSSTsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetDMSCorrectiveByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;

		//计算
		ret = GetDMSGPMSRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;

		//插入数据到统计表中
// 		ret = InsertResultLinesRate(statis_type, es_time);
// 		if(ret < 0)
// 			goto FAILED_RET;
		ret = InsertResultCBsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultDISsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultLDsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultInfoRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultStatusRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultBUSsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultSTsRate(statis_type, es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = InsertResultCorrectivesRate(statis_type,es_time);
		if (ret < 0)
			goto FAILED_RET;		
	}
	else
	{
		ret = GetGMSGPMSByMonth(es_time);
		if(ret < 0)
			goto FAILED_RET;
	}	
	
	return 0;

FAILED_RET:
	ES_ERROR_INFO es_err;
	es_err.statis_type = STATIS_DGPMS;
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

int StatisDGPMS::GetDMSLinesByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,count(*) from evalusystem.detail.dmsdv where oper!=2 group by organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSLines_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_dlines.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DMS馈线总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSLinesByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,count(*) from evalusystem.detail.gpmsdv where oper!=2 group by organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSLines_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_gplines.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取GPMS馈线总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetDMSCBsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//DMS开关数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.dmscb where oper!=2 "
		"and type=1 group by organ_code,dv;");*/

	//开关匹配数
	/*sprintf(sql, "select a.organ_code,a.dv,count(*) from evalusystem.detail.dmscb a,"
		"evalusystem.detail.gpmscb b where a.cb=b.cb and a.type=b.type and a.type=1 and a.organ_code = b.organ_code group by a.organ_code,a.dv");*/
	sprintf(sql,"select a.organ_code,c.dv,count(*) from evalusystem.detail.dmscb a,"
		"evalusystem.detail.gpmscb b,evalusystem.config.organdv c where a.cb=b.cb and a.type=b.type and a.type=1 and "
		"a.organ_code = b.organ_code and a.organ_code = c.organ_code group by a.organ_code,c.dv");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSCBs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_dcbs.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_dcbs = m_organ_dcbs.find(dgpms.organ_code);
			if(itr_organ_dcbs != m_organ_dcbs.end())
			{
				float count = itr_organ_dcbs->second;
				count += dgpms.count;
				m_organ_dcbs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_dcbs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSCBsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//GPMS开关数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.gpmscb where oper!=2 "
		"and type=1 group by organ_code,dv");*/

	//开关总数
	/*sprintf(sql, "select organ_code,dv,count(*) from (select organ_code,dv,cb from "
	"evalusystem.detail.dmscb where type=1 union select organ_code,dv,cb from "
	"evalusystem.detail.gpmscb where type=1) group by organ_code,dv");*/
	sprintf(sql,"select a.organ_code,c.dv,count(*) from (select organ_code,cb from evalusystem.detail.dmscb where "
		"type=1 union select organ_code,cb from evalusystem.detail.gpmscb where type=1) a,evalusystem.config.organdv c "
		"where a.organ_code = c.organ_code group by a.organ_code,c.dv");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSCBs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_gpcbs.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_gpcbs = m_organ_gpcbs.find(dgpms.organ_code);
			if(itr_organ_gpcbs != m_organ_gpcbs.end())
			{
				float count = itr_organ_gpcbs->second;
				count += dgpms.count;
				m_organ_gpcbs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gpcbs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取GPMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGpRightsByDay(ES_TIME *es_time)
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

	/*sprintf(sql, "select a.organ_code,a.info,count(*) from (select organ_code,devid,src,info,max(gtime) "
		"from evalusystem.detail.dmsbrand group by organ_code,devid,src,info) a,(select organ_code,devid,src,"
		"info,max(gtime) from evalusystem.detail.gpmsbrand group by organ_code,devid,src,info) b where "
		"a.src=b.src and a.src=0 and a.devid=b.devid and a.info=b.info group by a.organ_code,a.info");*/
	sprintf(sql, "select e.organ_code,e.info,count(*) from (select a.* from evalusystem.detail.dmsbrand a,"
		"(select organ_code,devid,max(gtime) maxtime from evalusystem.detail.dmsbrand where "
		"gtime>=to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') and "
		"gtime<to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') group by organ_code,devid) b "
		"where a.organ_code=b.organ_code and a.devid=b.devid and a.gtime=b.maxtime) e,(select c.* from "
		"evalusystem.detail.gpmsbrand c,(select organ_code,devid,max(gtime) maxtime from "
		"evalusystem.detail.gpmsbrand where gtime>=to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') "
		"and gtime<to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') group by organ_code,devid) d "
		"where c.organ_code=d.organ_code and c.devid=d.devid and c.gtime=d.maxtime) f where e.devid=f.devid "
		"and e.src=f.src and e.info=f.info and e.organ_code=f.organ_code group by e.organ_code,e.info",
		byear, bmonth, bday, year, month, day, byear, bmonth, bday, year, month, day);

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GpRights_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;
			map<ES_ORGAN_GP, float>::iterator itr_gp_rights = m_gp_rights.find(es_dgpms);
			if(itr_gp_rights != m_gp_rights.end())
			{
				float count = itr_gp_rights->second;
				count += dgpms.count;
				itr_gp_rights->second = count;
			}
			else
			{
				m_gp_rights.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_rights = m_organ_rights.find(dgpms.organ_code);
			if(itr_organ_rights != m_organ_rights.end())
			{
				float count = itr_organ_rights->second;
				count += dgpms.count;
				m_organ_rights[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_rights.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取挂牌正确的设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetDMSGpDevsByDay(ES_TIME *es_time)
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

	/*sprintf(sql, "select organ_code,info,count(*) from (select organ_code,devid,src,info,max(gtime) from "
		"evalusystem.detail.dmsbrand group by organ_code,devid,src,info) where src=0 group by organ_code,info");*/
	sprintf(sql, "select organ_code,info,count(*) from (select a.* from evalusystem.detail.dmsbrand a,"
		"(select organ_code,devid,max(gtime) maxtime from evalusystem.detail.dmsbrand where "
		"gtime>=to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') and "
		"gtime<to_date('%04d-%02d-%02d 0:0:0','YYYY-MM-DD HH24:MI:SS') group by organ_code,devid) b where "
		"a.organ_code=b.organ_code and a.devid=b.devid and a.gtime=b.maxtime) group by organ_code,info",
		byear, bmonth, bday, year, month, day);

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSGpDevs_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;
			map<ES_ORGAN_GP, float>::iterator itr_gp_dgpdevs = m_gp_dgpdevs.find(es_dgpms);
			if(itr_gp_dgpdevs != m_gp_dgpdevs.end())
			{
				float count = itr_gp_dgpdevs->second;
				count += dgpms.count;
				itr_gp_dgpdevs->second = count;
			}
			else
			{
				m_gp_dgpdevs.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_dgpdevs = m_organ_dgpdevs.find(dgpms.organ_code);
			if(itr_organ_dgpdevs != m_organ_dgpdevs.end())
			{
				float count = itr_organ_dgpdevs->second;
				count += dgpms.count;
				m_organ_dgpdevs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_dgpdevs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DMS挂牌总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSGpDevsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,info,count(*) from (select organ_code,devid,src,info,max(gtime) from "
		"evalusystem.detail.gpmsbrand group by organ_code,devid,src,info) where src=0 group by organ_code,info");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSGpDevs_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;
			map<ES_ORGAN_GP, float>::iterator itr_gp_gpgpdevs = m_gp_gpgpdevs.find(es_dgpms);
			if(itr_gp_gpgpdevs != m_gp_gpgpdevs.end())
			{
				float count = itr_gp_gpgpdevs->second;
				count += dgpms.count;
				itr_gp_gpgpdevs->second = count;
			}
			else
			{
				m_gp_gpgpdevs.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_gpgpdevs = m_organ_gpgpdevs.find(dgpms.organ_code);
			if(itr_organ_gpgpdevs != m_organ_gpgpdevs.end())
			{
				float count = itr_organ_gpgpdevs->second;
				count += dgpms.count;
				m_organ_gpgpdevs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gpgpdevs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取GPMS挂牌总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetDMSGPMSGpDevsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,info,count(*) from (select organ_code,devid,src,info,max(gtime) from "
		"evalusystem.detail.dmsbrand group by organ_code,devid,src,info) where src=0 group by organ_code,info "
		"union select organ_code,info,count(*) from (select organ_code,devid,src,info,max(gtime) from "
		"evalusystem.detail.gpmsbrand group by organ_code,devid,src,info) where src=0 group by organ_code,info");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DGPMSGpDevs_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;
			map<ES_ORGAN_GP, float>::iterator itr_gp_dgpgpdevs = m_gp_dgpgpdevs.find(es_dgpms);
			if(itr_gp_dgpgpdevs != m_gp_dgpgpdevs.end())
			{
				float count = itr_gp_dgpgpdevs->second;
				count += dgpms.count;
				itr_gp_dgpgpdevs->second = count;
			}
			else
			{
				m_gp_dgpgpdevs.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_dgpgpdevs = m_organ_dgpgpdevs.find(dgpms.organ_code);
			if(itr_organ_dgpgpdevs != m_organ_dgpgpdevs.end())
			{
				float count = itr_organ_dgpgpdevs->second;
				count += dgpms.count;
				m_organ_dgpgpdevs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_dgpgpdevs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DMS和GPMS挂牌设备合集失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetSyncRightDevsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	/*sprintf(sql, "select a.organ_code,a.dv,count(*) from (select organ_code,devid,src,dv,max(gtime) from "
		"evalusystem.detail.dmsstatus group by organ_code,devid,src,dv) a,(select organ_code,devid,src,dv,"
		"max(gtime) from evalusystem.detail.gpmsstatus group by organ_code,devid,src,dv) b where a.src=b.src "
		"and a.devid=b.devid group by a.organ_code,a.dv");*/
	sprintf(sql, "select a.organ_code,c.dv,count(*) from evalusystem.detail.dmscb a,evalusystem.detail.gpmscb "
		"b,evalusystem.config.organdv c where a.cb=b.cb and a.status=b.status and a.flag=1 and c.organ_code = a.organ_code group by a.organ_code,c.dv");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_SyncRightDevs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_sync.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_sync = m_organ_sync.find(dgpms.organ_code);
			if(itr_organ_sync != m_organ_sync.end())
			{
				float count = itr_organ_sync->second;
				count += dgpms.count;
				m_organ_sync[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_sync.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取状态置位同步正确的设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetDMSStatusDevsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	/*sprintf(sql, "select organ_code,dv,count(*) from (select distinct(devid),organ_code,dv "
		"from evalusystem.detail.dmsstatus) group by organ_code,dv");*/
	/*sprintf(sql, "select organ_code,dv,count(*) from (select distinct(cb),organ_code,dv "
		"from evalusystem.detail.dmscb) group by organ_code,dv");*/
	sprintf(sql, "select a.organ_code,c.dv,count(*) from evalusystem.detail.dmscb a,"
		"evalusystem.detail.gpmscb b,evalusystem.config.organdv c where a.cb=b.cb and a.flag=1 and a.organ_code = c.organ_code group by a.organ_code,c.dv");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSStatus_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_dsdevs.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_dsdevs = m_organ_dsdevs.find(dgpms.organ_code);
			if(itr_organ_dsdevs != m_organ_dsdevs.end())
			{
				float count = itr_organ_dsdevs->second;
				count += dgpms.count;
				m_organ_dsdevs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_dsdevs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取DMS状态置位总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSStatusDevsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	/*sprintf(sql, "select organ_code,dv,count(*) from (select distinct(devid),organ_code,dv "
		"from evalusystem.detail.gpmsstatus) group by organ_code,dv");*/
	sprintf(sql, "select organ_code,dv,count(*) from (select distinct(cb),organ_code,dv "
		"from evalusystem.detail.gpmscb) group by organ_code,dv");


	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSStatus_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_gpsdevs.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_gpsdevs = m_organ_gpsdevs.find(dgpms.organ_code);
			if(itr_organ_gpsdevs != m_organ_gpsdevs.end())
			{
				float count = itr_organ_gpsdevs->second;
				count += dgpms.count;
				m_organ_gpsdevs[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gpsdevs.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取GPMS状态置位总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS:: GetDMSLDsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//DMS配变数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.dmsld where "
		"oper!=2 group by organ_code,dv");*/

	//配变匹配数
	/*sprintf(sql, "select a.organ_code,a.dv,count(*) from evalusystem.detail.dmsld a,"
	"evalusystem.detail.gpmsld b where a.ld=b.ld and a.organ_code = b.organ_code group by a.organ_code,a.dv");*/

	sprintf(sql,"select a.organ_code,c.dv,count(*) from evalusystem.detail.dmsld a,evalusystem.detail.gpmsld b,config.organdv c where a.ld=b.ld "
		"and a.organ_code = b.organ_code and a.organ_code = c.organ_code group by a.organ_code,c.dv;");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSLDs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_dlds.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_dlds = m_organ_dlds.find(dgpms.organ_code);
			if(itr_organ_dlds != m_organ_dlds.end())
			{
				float count = itr_organ_dlds->second;
				count += dgpms.count;
				m_organ_dlds[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_dlds.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
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
}

int StatisDGPMS::GetGPMSLDsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//GPMS配变数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.gpmsld where "
		"oper!=2 group by organ_code,dv");*/

	//配变总数
	/*sprintf(sql, "select organ_code,dv,count(*) from (select organ_code,dv,ld from "
		"evalusystem.detail.dmsld union select organ_code,dv,ld from evalusystem.detail.gpmsld) "
		"group by organ_code,dv");*/

	sprintf(sql,"select a.organ_code,b.dv,count(*) from (select organ_code,ld from evalusystem.detail.dmsld union select organ_code,ld "
		"from evalusystem.detail.gpmsld)a, evalusystem.config.organdv b where a.organ_code = b.organ_code group by a.organ_code,b.dv;");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSLDs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_gplds.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_gplds = m_organ_gplds.find(dgpms.organ_code);
			if(itr_organ_gplds != m_organ_gplds.end())
			{
				float count = itr_organ_gplds->second;
				count += dgpms.count;
				m_organ_gplds[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gplds.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取GPMS配变总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGMSGPMSByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 4];
	char tmp_sql[MAX_SQL_LEN * 2];
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

#if 0
	for(int i = days; i >= 1; i--)
	{
		sprintf(tmp_name, "day_%04d%02d%02d", byear, bmonth, i);
// 		sprintf(tmp_sql,"select * from evalusystem.result.%s where code='linefine' or code='dmslines' "
// 			"or code='pmslines' or code='breakfine' or code='dmsbreakers' or code='pmsbreakers' or "
// 			"code='gpinfrigh' or code='gpinfdevs' or code='dmsgpnum' or code='pmsgpnum' or "
// 			"code='allgpnum' or code='devictb' or code='devictbrigh' or code='dmszwnum' or "
// 			"code='pmszenum' or code='allzwnum' or code='ldfine' or code='dmslds' or code='pmslds'",tmp_name);
		sprintf(tmp_sql,"select * from evalusystem.result.%s where "
			"code='breakfine' or code='dmsbreakers' or code='pmsbreakers' or "
			"code='disfine' or code='dmsdiss' or code='pmsdiss' or "
			"code='gpinfrigh' or code='gpinfdevs' or code='dmsgpnum' or code='pmsgpnum' or "
			"code='allgpnum' or code='devictb' or code='devictbrigh' or code='dmszwnum' or "
			"code='pmszenum' or code='allzwnum' or code='ldfine' or code='dmslds' or code='pmslds' or "
			"code='busfine' or code='dmsbuss' or code='pmsbuss' or code='substionfine' or "
			"code='dmssts' or code='pmssts'",tmp_name);
		
		ret = d5000.d5000_ReadData(tmp_sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
		if(ret == 1)
		{
			if(rec_num > 0)
			{
				table_name = tmp_name;
				d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
				break;
			}
			else
			{
				if(i == 1)
					table_name = "no_data";
				d5000.g_CDci.FreeColAttrData(col_attr, col_num);
				continue;
			}
		}
		else
		{
			printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计失败，error(%d) = %s, 文件名：%s, 行号：%d",
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
			"dimvalue from evalusystem.result.%s where code='breakfine' or code='dmsbreakers' or code='pmsbreakers' "
			"or code='disfine' or code='dmsdiss' or code='pmsdiss' or code='gpinfrigh' "
			"or code='gpinfdevs' or code='dmsgpnum' or code='pmsgpnum' or code='allgpnum' or code='devictb' or "
			"code='devictbrigh' or code='dmszwnum' or code='pmszenum' or code='allzwnum' or code='ldfine' or "
			"code='dmslds' or code='pmslds' or code='busfine' or code='dmsbuss' or code='pmsbuss' or "
			"code='substionfine' or code='dmssts' or code='pmssts'", byear, bmonth, table_name.c_str());

		ret = d5000.d5000_ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			return -1;
		}
	}
#else
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

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='dmsbreakers' "
		"or code='pmsbreakers' or code='dmsdiss' or code='pmsdiss' or code='gpinfdevs' or code='dmsgpnum' "
		"or code='pmsgpnum' or code='allgpnum' or code='nrec') and dimname='无' group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='devictbrigh' or "
		"code='dmszwnum' or code='pmszenum' or code='allzwnum' or code='dmslds' or code='pmslds' or "
		"code='dmsbuss' or code='pmsbuss' or code='dmssts' or code='pmssts' or code='wrongnum') and dimname='无' group by "
		"code,organ_code,datatype", byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='breakfine' or "
		"code='disfine' or code='gpinfrigh' or code='devictb' or code='ldfine' or code='busfine' "
		"or code='substionfine' or code='rectification') and dimname='无'  and datatype != '数值' group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	/*sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='breakfines' or "
		"code='disfines' or code='gpinfrighs' or code='devictbs' or code='ldfines' or code='busfines' "
		"or code='substionfines' or code='rectifications') and dimname='无'  group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}*/
#endif

	return 0;
}

int StatisDGPMS::GetDMSGPMSRate(int statis_type, ES_TIME *es_time)
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
		s_time = day * 24;

	//配电线路完整率
	//map<int, float>::iterator itr_organ_dlines;
	//map<int, float>::iterator itr_organ_gplines;

	////按主站
	//itr_organ_dlines = m_organ_dlines.begin();
	//for(; itr_organ_dlines != m_organ_dlines.end(); itr_organ_dlines++)
	//{
	//	ES_RESULT es_result;
	//	es_result.m_ntype = itr_organ_dlines->first;
	//	itr_organ_gplines = m_organ_gplines.find(itr_organ_dlines->first);
	//	if(itr_organ_gplines != m_organ_gplines.end())
	//	{
	//		if(itr_organ_gplines->second <= itr_organ_dlines->second)
	//			es_result.m_fresult = itr_organ_gplines->second / itr_organ_dlines->second;
	//		else
	//			es_result.m_fresult = itr_organ_dlines->second / itr_organ_gplines->second;
	//	}
	//	else
	//		es_result.m_fresult = 0;

	//	m_lines_result.insert(make_pair(es_result.m_ntype, es_result));
	//}

	//配电开关数完整率
	map<int, float>::iterator itr_organ_dcbs;
	map<string, ES_DATIMES>::iterator itr_dv_dcbs;
	map<int, float>::iterator itr_organ_gpcbs;
	map<string, ES_DATIMES>::iterator itr_dv_gpcbs;
	map<string, float>::iterator itr_code_weight;

	//按主站
	itr_organ_gpcbs = m_organ_gpcbs.begin();
	for(; itr_organ_gpcbs != m_organ_gpcbs.end(); itr_organ_gpcbs++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_gpcbs->first;
		itr_organ_dcbs = m_organ_dcbs.find(itr_organ_gpcbs->first);
		if(itr_organ_dcbs != m_organ_dcbs.end())
		{
			if(itr_organ_dcbs->second <= itr_organ_gpcbs->second)
				es_result.m_fresult = itr_organ_dcbs->second / itr_organ_gpcbs->second;
			else
				es_result.m_fresult = itr_organ_gpcbs->second / itr_organ_dcbs->second;
		}
		else
			es_result.m_fresult = 0;

		m_cbs_result.insert(make_pair(DIM_DMS, es_result));

		//es_score.m_ntype = itr_organ_gpcbs->first;
		//itr_code_weight = m_code_weight.find("breakfine");
		//if (itr_code_weight != m_code_weight.end())
		//{
		//	es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		//	//m_cbs_score.insert(make_pair(itr_organ_gpcbs->first,es_result.m_fresult*itr_code_weight->second));
		//}
		//else	
		//	es_score.m_fresult = 0;

		//m_cbs_result.insert(make_pair(DIM_SCORE,es_score));
	}

	//按馈线
	itr_dv_gpcbs = m_dv_gpcbs.begin();
	for(; itr_dv_gpcbs != m_dv_gpcbs.end(); itr_dv_gpcbs++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_gpcbs->first;
		es_result.m_ntype = itr_dv_gpcbs->second.organ_code;
		itr_dv_dcbs = m_dv_dcbs.find(itr_dv_gpcbs->first);
		if(itr_dv_dcbs != m_dv_dcbs.end())
		{
			if(itr_dv_dcbs->second.count <= itr_dv_gpcbs->second.count)
				es_result.m_fresult = itr_dv_dcbs->second.count / itr_dv_gpcbs->second.count;
			else
				es_result.m_fresult = itr_dv_gpcbs->second.count / itr_dv_dcbs->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_cbs_result.insert(make_pair(DIM_LINE, es_result));
	}

	//配电刀闸数完整率
	map<int, float>::iterator itr_organ_ddiss;
	map<string, ES_DATIMES>::iterator itr_dv_ddiss;
	map<int, float>::iterator itr_organ_gpdiss;
	map<string, ES_DATIMES>::iterator itr_dv_gpdiss;

	//按主站
	itr_organ_gpdiss = m_organ_gpdiss.begin();
	for(; itr_organ_gpdiss != m_organ_gpdiss.end(); itr_organ_gpdiss++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_gpdiss->first;
		itr_organ_ddiss = m_organ_ddiss.find(itr_organ_gpdiss->first);
		if(itr_organ_ddiss != m_organ_ddiss.end())
		{
			if(itr_organ_ddiss->second <= itr_organ_gpdiss->second)
				es_result.m_fresult = itr_organ_ddiss->second / itr_organ_gpdiss->second;
			else
				es_result.m_fresult = itr_organ_gpdiss->second / itr_organ_ddiss->second;
		}
		else
			es_result.m_fresult = 0;

		m_diss_result.insert(make_pair(DIM_DMS, es_result));

		/*es_score.m_ntype = itr_organ_gpdiss->first;
		itr_code_weight = m_code_weight.find("disfine");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_diss_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按馈线
	itr_dv_gpdiss = m_dv_gpdiss.begin();
	for(; itr_dv_gpdiss != m_dv_gpdiss.end(); itr_dv_gpdiss++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_gpdiss->first;
		es_result.m_ntype = itr_dv_gpdiss->second.organ_code;
		itr_dv_ddiss = m_dv_ddiss.find(itr_dv_gpdiss->first);
		if(itr_dv_ddiss != m_dv_ddiss.end())
		{
			if(itr_dv_ddiss->second.count <= itr_dv_gpdiss->second.count)
				es_result.m_fresult = itr_dv_ddiss->second.count / itr_dv_gpdiss->second.count;
			else
				es_result.m_fresult = itr_dv_gpdiss->second.count / itr_dv_ddiss->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_diss_result.insert(make_pair(DIM_LINE, es_result));
	}

	//配电变压器完整率
	map<int, float>::iterator itr_organ_dlds;
	map<string, ES_DATIMES>::iterator itr_dv_dlds;
	map<int, float>::iterator itr_organ_gplds;
	map<string, ES_DATIMES>::iterator itr_dv_gplds;

	//按主站
	itr_organ_gplds = m_organ_gplds.begin();
	for(; itr_organ_gplds != m_organ_gplds.end(); itr_organ_gplds++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_gplds->first;
		itr_organ_dlds = m_organ_dlds.find(itr_organ_gplds->first);
		if(itr_organ_dlds != m_organ_dlds.end())
		{
			if(itr_organ_dlds->second <= itr_organ_gplds->second)
				es_result.m_fresult = itr_organ_dlds->second / itr_organ_gplds->second;
			else
				es_result.m_fresult = itr_organ_gplds->second / itr_organ_dlds->second;
		}
		else
			es_result.m_fresult = 0;

		m_lds_result.insert(make_pair(DIM_DMS, es_result));

		//zgs
		/*es_score.m_ntype = itr_organ_gplds->first;
		itr_code_weight = m_code_weight.find("ldfine");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_lds_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按馈线
	itr_dv_gplds = m_dv_gplds.begin();
	for(; itr_dv_gplds != m_dv_gplds.end(); itr_dv_gplds++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_gplds->first;
		es_result.m_ntype = itr_dv_gplds->second.organ_code;
		itr_dv_dlds = m_dv_dlds.find(itr_dv_gplds->first);
		if(itr_dv_dlds != m_dv_dlds.end())
		{
			if(itr_dv_dlds->second.count <= itr_dv_gplds->second.count)
				es_result.m_fresult = itr_dv_dlds->second.count / itr_dv_gplds->second.count;
			else
				es_result.m_fresult = itr_dv_gplds->second.count / itr_dv_dlds->second.count;
		}
		else
			es_result.m_fresult = 0;

		m_lds_result.insert(make_pair(DIM_LINE, es_result));
	}

	//挂牌信息同步率
	map<int, float>::iterator itr_organ_rights;
	map<ES_ORGAN_GP, float>::iterator itr_gp_rights;
	map<int, float>::iterator itr_organ_dgpdevs;
	map<ES_ORGAN_GP, float>::iterator itr_gp_dgpdevs;

	//按主站
	itr_organ_dgpdevs = m_organ_dgpdevs.begin();
	for(; itr_organ_dgpdevs != m_organ_dgpdevs.end(); itr_organ_dgpdevs++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_dgpdevs->first;
		itr_organ_rights = m_organ_rights.find(itr_organ_dgpdevs->first);
		if(itr_organ_rights != m_organ_rights.end())
			es_result.m_fresult = itr_organ_rights->second / itr_organ_dgpdevs->second;
		else
			es_result.m_fresult = 0;

		m_info_result.insert(make_pair(DIM_DMS, es_result));

		/*es_score.m_ntype = itr_organ_dgpdevs->first;
		itr_code_weight = m_code_weight.find("gpinfrigh");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_info_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按挂牌类型
	itr_gp_dgpdevs = m_gp_dgpdevs.begin();
	for(; itr_gp_dgpdevs != m_gp_dgpdevs.end(); itr_gp_dgpdevs++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_gp_dgpdevs->first.gp_type;
		es_result.m_ntype = itr_gp_dgpdevs->first.organ_code;
		ES_ORGAN_GP es_organ;
		es_organ.organ_code = itr_gp_dgpdevs->first.organ_code;
		es_organ.gp_type = itr_gp_dgpdevs->first.gp_type;

		/*cout<<"chazhao:"<<es_organ.organ_code<<" "<<es_organ.gp_type<<endl;
		itr_gp_rights = m_gp_rights.begin();
		for(; itr_gp_rights != m_gp_rights.end(); itr_gp_rights++)
		{
			cout<<itr_gp_rights->first.organ_code<<" "<<itr_gp_rights->first.gp_type<<" "<<itr_gp_rights->second<<endl;
		}
		cout<<endl;*/

		itr_gp_rights = m_gp_rights.find(es_organ);
		if(itr_gp_rights != m_gp_rights.end())
			es_result.m_fresult = itr_gp_rights->second / itr_gp_dgpdevs->second;
		else
			es_result.m_fresult = 0;
			
		m_info_result.insert(make_pair(DIM_GP, es_result));
	}

	//设备状态同步率
	//（状态置位正确的设备数）/（状态置位的设备总数）
	map<int, float>::iterator itr_organ_sync;
	map<string, ES_DATIMES>::iterator itr_dv_sync;
	map<int, float>::iterator itr_organ_dsdevs;
	map<string, ES_DATIMES>::iterator itr_dv_dsdevs;

	//按主站
	itr_organ_dsdevs = m_organ_dsdevs.begin();
	for(; itr_organ_dsdevs != m_organ_dsdevs.end(); itr_organ_dsdevs++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_dsdevs->first;
		itr_organ_sync = m_organ_sync.find(itr_organ_dsdevs->first);
		if(itr_organ_sync != m_organ_sync.end())
			es_result.m_fresult = itr_organ_sync->second / itr_organ_dsdevs->second;
		else
			es_result.m_fresult = 0;

		m_status_result.insert(make_pair(DIM_DMS, es_result));

		/*es_score.m_ntype = itr_organ_dsdevs->first;
		itr_code_weight = m_code_weight.find("devictb");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_status_result.insert(make_pair(DIM_SCORE,es_score));*/
	}

	//按馈线
	itr_dv_dsdevs = m_dv_dsdevs.begin();
	for(; itr_dv_dsdevs != m_dv_dsdevs.end(); itr_dv_dsdevs++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_dv_dsdevs->first;
		es_result.m_ntype = itr_dv_dsdevs->second.organ_code;
		itr_dv_sync = m_dv_sync.find(itr_dv_dsdevs->first);
		if(itr_dv_sync != m_dv_sync.end())
			es_result.m_fresult = itr_dv_sync->second.count / itr_dv_dsdevs->second.count;
		else
			es_result.m_fresult = 0;

		m_status_result.insert(make_pair(DIM_LINE, es_result));
	}

	//配电母线数完整率
	map<int, float>::iterator itr_organ_dmsbus;
	map<int, float>::iterator itr_organ_gpmsbus;

	//按主站
	itr_organ_dmsbus = m_organ_dmsbus.begin();
	for(; itr_organ_dmsbus != m_organ_dmsbus.end(); itr_organ_dmsbus++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_dmsbus->first;
		itr_organ_gpmsbus = m_organ_gpmsbus.find(itr_organ_dmsbus->first);
		if(itr_organ_gpmsbus != m_organ_gpmsbus.end())
		{
			if(itr_organ_gpmsbus->second <= itr_organ_dmsbus->second)
				es_result.m_fresult = itr_organ_gpmsbus->second / itr_organ_dmsbus->second;
			else
				es_result.m_fresult = itr_organ_dmsbus->second / itr_organ_gpmsbus->second;
		}
		else
			es_result.m_fresult = 0;

		m_bus_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_dmsbus->first;
		itr_code_weight = m_code_weight.find("busfine");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_bus_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//配电站房数完整率
	map<int, float>::iterator itr_organ_dmsst;
	map<int, float>::iterator itr_organ_gpmsst;

	//按主站
	itr_organ_dmsst = m_organ_dmsst.begin();
	for(; itr_organ_dmsst != m_organ_dmsst.end(); itr_organ_dmsst++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_dmsst->first;
		itr_organ_gpmsst = m_organ_gpmsst.find(itr_organ_dmsst->first);
		if(itr_organ_gpmsst != m_organ_gpmsst.end())
		{
			if(itr_organ_gpmsst->second <= itr_organ_dmsst->second)
				es_result.m_fresult = itr_organ_gpmsst->second / itr_organ_dmsst->second;
			else
				es_result.m_fresult = itr_organ_dmsst->second / itr_organ_gpmsst->second;
		}
		else
			es_result.m_fresult = 0;

		m_st_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_dmsst->first;
		itr_code_weight = m_code_weight.find("substionfine");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_st_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/

	}

	//两周指标整改率
	map<int, float>::iterator itr_organ_result;
	map<int, ES_Corrective>::iterator itr_organ_corrective;

	//按主站
	itr_organ_corrective = m_organ_corrective.begin();
	for(; itr_organ_corrective != m_organ_corrective.end(); itr_organ_corrective++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_corrective->first;
		if(itr_organ_corrective->second.confailnum != 0 && itr_organ_corrective->second.failnum != 0)
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, (1-(itr_organ_corrective->second.confailnum/itr_organ_corrective->second.failnum))));
			es_result.m_fresult = 1-(itr_organ_corrective->second.confailnum/itr_organ_corrective->second.failnum);
		}
		else if(itr_organ_corrective->second.confailnum == 0)
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, 1.0));
			es_result.m_fresult = 1.0;
		}
		else
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, 0.0));
			es_result.m_fresult = 0.0;
		}

		m_corrective_result.insert(make_pair(DIM_DMS,es_result));

		/*es_score.m_ntype = itr_organ_corrective->first;
		itr_code_weight = m_code_weight.find("rectification");
		if (itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;

		m_corrective_result.insert(make_pair(DIM_SCORE,es_score));*/
	}
	
	return 0;
}

int StatisDGPMS::InsertResultLinesRate(int statis_type, ES_TIME *es_time)
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
	map<int ,ES_RESULT>::iterator itr_lines_result;
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

	//配电线路完整率按主站
	rec_num = m_lines_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_lines_result = m_lines_result.begin();
	for(; itr_lines_result != m_lines_result.end(); itr_lines_result++)
	{
		memcpy(m_pdata + offset, "linefine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_lines_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_lines_result->second.m_fresult, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电线路完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电线路完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统线路数量按主站
	offset = 0;
	rec_num = m_organ_dlines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dlines = m_organ_dlines.begin();
	for(; itr_organ_dlines != m_organ_dlines.end(); itr_organ_dlines++)
	{
		memcpy(m_pdata + offset, "dmslines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dlines->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dlines->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS线路总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS系统线路数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统线路数量按主站
	offset = 0;
	rec_num = m_organ_gplines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gplines = m_organ_gplines.begin();
	for(; itr_organ_gplines != m_organ_gplines.end(); itr_organ_gplines++)
	{
		memcpy(m_pdata + offset, "pmslines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gplines->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gplines->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS线路总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS系统线路数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::InsertResultCBsRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_cbs_result;

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

	//配电开关数完整率按主站
	rec_num = m_cbs_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cbs_result = m_cbs_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "breakfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_cbs_result ++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_cbs_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cbs_result = m_cbs_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "breakfines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_cbs_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//配电开关数完整率按馈线
	offset = 0;
	rec_num = m_cbs_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cbs_result = m_cbs_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "breakfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cbs_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_cbs_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_cbs_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电开关数完整率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统开关数量按主站
	offset = 0;
	rec_num = m_organ_dcbs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dcbs = m_organ_dcbs.begin();
	for(; itr_organ_dcbs != m_organ_dcbs.end(); itr_organ_dcbs++)
	{
		memcpy(m_pdata + offset, "dmsbreakers", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dcbs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dcbs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS系统开关数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS系统开关数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统开关数量按馈线
	offset = 0;
	rec_num = m_dv_dcbs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_dcbs = m_dv_dcbs.begin();
	for(; itr_dv_dcbs != m_dv_dcbs.end(); itr_dv_dcbs++)
	{
		memcpy(m_pdata + offset, "dmsbreakers", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_dcbs->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_dcbs->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_dcbs->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS系统开关数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS系统开关数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统开关数量按主站
	offset = 0;
	rec_num = m_organ_gpcbs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpcbs = m_organ_gpcbs.begin();
	for(; itr_organ_gpcbs != m_organ_gpcbs.end(); itr_organ_gpcbs++)
	{
		memcpy(m_pdata + offset, "pmsbreakers", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpcbs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpcbs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS系统开关数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS系统开关数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统开关数量按馈线
	offset = 0;
	rec_num = m_dv_gpcbs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_gpcbs = m_dv_gpcbs.begin();
	for(; itr_dv_gpcbs != m_dv_gpcbs.end(); itr_dv_gpcbs++)
	{
		memcpy(m_pdata + offset, "pmsbreakers", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpcbs->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpcbs->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_gpcbs->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS系统开关数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS系统开关数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::InsertResultInfoRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_info_result;

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

	//挂牌信息同步率按主站
	rec_num = m_info_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_info_result = m_info_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "gpinfrigh", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_info_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌信息同步率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌信息同步率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_info_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_info_result = m_info_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "gpinfrighs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_info_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//挂牌信息同步率按挂牌类型
	offset = 0;
	rec_num = m_info_result.count(DIM_GP);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_info_result = m_info_result.find(DIM_GP);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "gpinfrigh", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_info_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_info_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_info_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌信息同步率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌信息同步率按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//挂牌信息同步正确设备数按主站
	offset = 0;
	rec_num = m_organ_rights.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_rights = m_organ_rights.begin();
	for(; itr_organ_rights != m_organ_rights.end(); itr_organ_rights++)
	{
		memcpy(m_pdata + offset, "gpinfdevs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_rights->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_rights->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌信息同步正确设备数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌信息同步正确设备数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//挂牌信息同步正确设备数按挂牌类型
	offset = 0;
	rec_num = m_gp_rights.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_rights = m_gp_rights.begin();
	for(; itr_gp_rights != m_gp_rights.end(); itr_gp_rights++)
	{
		memcpy(m_pdata + offset, "gpinfdevs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_rights->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_rights->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_rights->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌信息同步正确设备数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌信息同步正确设备数按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS挂牌设备总数按主站
	offset = 0;
	rec_num = m_organ_dgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dgpdevs = m_organ_dgpdevs.begin();
	for(; itr_organ_dgpdevs != m_organ_dgpdevs.end(); itr_organ_dgpdevs++)
	{
		memcpy(m_pdata + offset, "dmsgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dgpdevs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dgpdevs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS挂牌设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS挂牌设备总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS挂牌设备总数按挂牌类型
	offset = 0;
	rec_num = m_gp_dgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_dgpdevs = m_gp_dgpdevs.begin();
	for(; itr_gp_dgpdevs != m_gp_dgpdevs.end(); itr_gp_dgpdevs++)
	{
		memcpy(m_pdata + offset, "dmsgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_dgpdevs->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_dgpdevs->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_dgpdevs->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS挂牌设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS挂牌设备总数按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
#if 0
	//GPMS挂牌设备总数按主站
	offset = 0;
	rec_num = m_organ_gpgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpgpdevs = m_organ_gpgpdevs.begin();
	for(; itr_organ_gpgpdevs != m_organ_gpgpdevs.end(); itr_organ_gpgpdevs++)
	{
		memcpy(m_pdata + offset, "pmsgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpgpdevs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpgpdevs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS挂牌设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS挂牌设备总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS挂牌设备总数按挂牌类型
	offset = 0;
	rec_num = m_gp_gpgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_gpgpdevs = m_gp_gpgpdevs.begin();
	for(; itr_gp_gpgpdevs != m_gp_gpgpdevs.end(); itr_gp_gpgpdevs++)
	{
		memcpy(m_pdata + offset, "pmsgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_gpgpdevs->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_gpgpdevs->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_gpgpdevs->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS挂牌设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS挂牌设备总数按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS与GPMS挂牌数目合集按主站
	offset = 0;
	rec_num = m_organ_dgpgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dgpgpdevs = m_organ_dgpgpdevs.begin();
	for(; itr_organ_dgpgpdevs != m_organ_dgpgpdevs.end(); itr_organ_dgpgpdevs++)
	{
		memcpy(m_pdata + offset, "allgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dgpgpdevs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dgpgpdevs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS与GPMS挂牌设备合集入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS与GPMS挂牌数目合集按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS与GPMS挂牌数目合集按挂牌类型
	offset = 0;
	rec_num = m_gp_dgpgpdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_dgpgpdevs = m_gp_dgpgpdevs.begin();
	for(; itr_gp_dgpgpdevs != m_gp_dgpgpdevs.end(); itr_gp_dgpgpdevs++)
	{
		memcpy(m_pdata + offset, "allgpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_dgpgpdevs->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_dgpgpdevs->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_dgpgpdevs->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS与GPMS挂牌设备合集入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS与GPMS挂牌数目合集按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
#endif
	return 0;
}

int StatisDGPMS::InsertResultStatusRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_status_result;
	//map<string, float>::iterator itr_organ_score;

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

	//设备状态同步率按主站
	rec_num = m_status_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_status_result = m_status_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "devictb", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_status_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "设备状态同步率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "设备状态同步率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_status_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_status_result = m_status_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "devictbs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_status_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//设备状态同步率按馈线
	offset = 0;
	rec_num = m_status_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_status_result = m_status_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "devictb", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_status_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_status_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_status_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "设备状态同步率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "设备状态同步率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//状态置位同步正确的设备数按主站
	offset = 0;
	rec_num = m_organ_sync.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_sync = m_organ_sync.begin();
	for(; itr_organ_sync != m_organ_sync.end(); itr_organ_sync++)
	{
		memcpy(m_pdata + offset, "devictbrigh", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_sync->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_sync->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "状态置位同步正确的设备数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "状态置位同步正确的设备数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//状态置位同步正确的设备数按馈线
	offset = 0;
	rec_num = m_dv_sync.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_sync = m_dv_sync.begin();
	for(; itr_dv_sync != m_dv_sync.end(); itr_dv_sync++)
	{
		memcpy(m_pdata + offset, "devictbrigh", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_sync->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_sync->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_sync->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			plog->WriteLog(2, "状态置位同步正确的设备数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "状态置位同步正确的设备数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS状态置位设备总数按主站
	offset = 0;
	rec_num = m_organ_dsdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dsdevs = m_organ_dsdevs.begin();
	for(; itr_organ_dsdevs != m_organ_dsdevs.end(); itr_organ_dsdevs++)
	{
		memcpy(m_pdata + offset, "dmszwnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dsdevs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dsdevs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS状态置位设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS状态置位设备总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS状态置位设备总数按馈线
	offset = 0;
	rec_num = m_dv_dsdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_dsdevs = m_dv_dsdevs.begin();
	for(; itr_dv_dsdevs != m_dv_dsdevs.end(); itr_dv_dsdevs++)
	{
		memcpy(m_pdata + offset, "dmszwnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_dsdevs->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_dsdevs->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_dsdevs->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS状态置位设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS状态置位设备总数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS状态置位设备总数按主站
	offset = 0;
	rec_num = m_organ_gpsdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpsdevs = m_organ_gpsdevs.begin();
	for(; itr_organ_gpsdevs != m_organ_gpsdevs.end(); itr_organ_gpsdevs++)
	{
		memcpy(m_pdata + offset, "pmszenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpsdevs->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpsdevs->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS状态置位设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS状态置位设备总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS状态置位设备总数按馈线
	offset = 0;
	rec_num = m_dv_gpsdevs.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_gpsdevs = m_dv_gpsdevs.begin();
	for(; itr_dv_gpsdevs != m_dv_gpsdevs.end(); itr_dv_gpsdevs++)
	{
		memcpy(m_pdata + offset, "pmszenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpsdevs->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpsdevs->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_gpsdevs->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS状态置位设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS状态置位设备总数按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::InsertResultLDsRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_lds_result;

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

	//配电变压器完整率按主站
	rec_num = m_lds_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_lds_result = m_lds_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "ldfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_lds_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电变压器完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电变压器完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_lds_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_lds_result = m_lds_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "ldfines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_lds_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata)*/

	//配电变压器完整率按馈线
	offset = 0;
	rec_num = m_lds_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_lds_result = m_lds_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "ldfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_lds_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_lds_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_lds_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电变压器完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "配电变压器完整率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统变压器数量按主站
	offset = 0;
	rec_num = m_organ_dlds.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dlds = m_organ_dlds.begin();
	for(; itr_organ_dlds != m_organ_dlds.end(); itr_organ_dlds++)
	{
		memcpy(m_pdata + offset, "dmslds", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dlds->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dlds->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS系统变压器数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS系统变压器数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统变压器数量按馈线
	offset = 0;
	rec_num = m_dv_dlds.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_dlds = m_dv_dlds.begin();
	for(; itr_dv_dlds != m_dv_dlds.end(); itr_dv_dlds++)
	{
		memcpy(m_pdata + offset, "dmslds", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_dlds->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_dlds->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_dlds->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS系统变压器数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS系统变压器数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统变压器数量按主站
	offset = 0;
	rec_num = m_organ_gplds.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gplds = m_organ_gplds.begin();
	for(; itr_organ_gplds != m_organ_gplds.end(); itr_organ_gplds++)
	{
		memcpy(m_pdata + offset, "pmslds", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gplds->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gplds->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS系统变压器数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS系统变压器数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统变压器数量按馈线
	offset = 0;
	rec_num = m_dv_gplds.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_gplds = m_dv_gplds.begin();
	for(; itr_dv_gplds != m_dv_gplds.end(); itr_dv_gplds++)
	{
		memcpy(m_pdata + offset, "pmslds", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_gplds->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_gplds->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_gplds->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "GPMS系统变压器数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "GPMS系统变压器数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

void StatisDGPMS::Clear()
{
	m_organ_dlines.clear();
	m_organ_gplines.clear();
	m_organ_dcbs.clear();
	m_dv_dcbs.clear();
	m_organ_gpcbs.clear();
	m_dv_gpcbs.clear();
	m_organ_ddiss.clear();
	m_dv_ddiss.clear();
	m_organ_gpdiss.clear();
	m_dv_gpdiss.clear();
	m_organ_dlds.clear();
	m_dv_dlds.clear();
	m_organ_gplds.clear();
	m_dv_gplds.clear();
	m_organ_rights.clear();
	m_gp_rights.clear();
	m_organ_dgpdevs.clear();
	m_gp_dgpdevs.clear();
	m_organ_gpgpdevs.clear();
	m_gp_gpgpdevs.clear();
	m_organ_dgpgpdevs.clear();
	m_gp_dgpgpdevs.clear();
	m_organ_sync.clear();
	m_dv_sync.clear();
	m_organ_dsdevs.clear();
	m_dv_dsdevs.clear();
	m_organ_gpsdevs.clear();
	m_dv_gpsdevs.clear();
	m_lines_result.clear();
	m_cbs_result.clear();
	m_diss_result.clear();
	m_info_result.clear();
	m_status_result.clear();
	m_lds_result.clear();
	m_organ_dmsbus.clear();
	m_organ_gpmsbus.clear();
	m_organ_dmsst.clear();
	m_organ_gpmsst.clear();
	m_bus_result.clear();
	m_st_result.clear();
	/*m_st_score.clear();
	m_bus_score.clear();
	m_code_weight.clear();*/
}

int StatisDGPMS::GetDGPMSProcess(ES_TIME *es_time)
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

	//删除当日数据防止重复
	sprintf(sql, "call evalusystem.result.DELETEDATA('%04d-%02d-%02d')", byear, bmonth, bday);

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "DMS与GPMS信息交互数据删除处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}
	
	//sprintf(sql, "call evalusystem.result.DGPMSPROCESS()");
	sprintf(sql, "call evalusystem.result.DGPMSPROCESS('%04d%02d%02d')", byear, bmonth, bday);

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 		plog->WriteLog(2, "DMS与GPMS信息交互数据处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisDGPMS::GetDMSDISsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//DMS刀闸数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.dmscb where oper!=2 "
		"and type=0 group by organ_code,dv");*/

	//刀闸匹配数
	/*sprintf(sql, "select a.organ_code,a.dv,count(*) from evalusystem.detail.dmscb a,"
		"evalusystem.detail.gpmscb b where a.cb=b.cb and a.type=b.type and a.type=0  and a.organ_code = b.organ_code "
		"group by a.organ_code,a.dv");*/
	sprintf(sql,"select a.organ_code,c.dv,count(*) from evalusystem.detail.dmscb a,evalusystem.detail.gpmscb "
		"b,evalusystem.config.organdv c where a.cb=b.cb and a.type=b.type and a.type=0  and a.organ_code = b.organ_code and "
		"a.organ_code = c.organ_code group by a.organ_code,c.dv;");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSDISs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_ddiss.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_ddiss = m_organ_ddiss.find(dgpms.organ_code);
			if(itr_organ_ddiss != m_organ_ddiss.end())
			{
				float count = itr_organ_ddiss->second;
				count += dgpms.count;
				m_organ_ddiss[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_ddiss.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSDISsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//GPMS刀闸数
	/*sprintf(sql, "select organ_code,dv,count(*) from evalusystem.detail.gpmscb where oper!=2 "
		"and type=0 group by organ_code,dv");*/

	//刀闸总数
	/*sprintf(sql, "select organ_code,dv,count(*) from (select organ_code,dv,cb from "
	"evalusystem.detail.dmscb where type=0 union select organ_code,dv,cb from "
	"evalusystem.detail.gpmscb where type=0) group by organ_code,dv");*/

	sprintf(sql,"select a.organ_code,b.dv,count(*) from (select organ_code,cb from evalusystem.detail.dmscb where "
		"type=0 union select organ_code,cb from evalusystem.detail.gpmscb where type=0)a,evalusystem.config.organdv "
		"b where a.organ_code = b.organ_code group by a.organ_code,b.dv");


	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSDISs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.dv = string(tempstr);
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_dv_gpdiss.insert(make_pair(dgpms.dv, dgpms));

			map<int, float>::iterator itr_organ_gpdiss = m_organ_gpdiss.find(dgpms.organ_code);
			if(itr_organ_gpdiss != m_organ_gpdiss.end())
			{
				float count = itr_organ_gpdiss->second;
				count += dgpms.count;
				m_organ_gpdiss[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gpdiss.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::InsertResultDISsRate(int statis_type, ES_TIME *es_time)
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
	multimap<int ,ES_RESULT>::iterator itr_diss_result;
	//map<string, float>::iterator itr_organ_score;

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

	//配电刀闸数完整率按主站
	rec_num = m_diss_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_diss_result = m_diss_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "disfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_diss_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电刀闸数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电刀闸数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_diss_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_diss_result = m_diss_result.find(DIM_SCORE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "disfines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_diss_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电开关数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电开关数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//配电刀闸数完整率按馈线
	offset = 0;
	rec_num = m_diss_result.count(DIM_LINE);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_diss_result = m_diss_result.find(DIM_LINE);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "disfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_diss_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_diss_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_diss_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电刀闸数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电刀闸数完整率按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统刀闸数量按主站
	offset = 0;
	rec_num = m_organ_ddiss.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_ddiss = m_organ_ddiss.begin();
	for(; itr_organ_ddiss != m_organ_ddiss.end(); itr_organ_ddiss++)
	{
		memcpy(m_pdata + offset, "dmsdiss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_ddiss->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_ddiss->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS系统刀闸数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS系统刀闸数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS系统刀闸数量按馈线
	offset = 0;
	rec_num = m_dv_ddiss.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_ddiss = m_dv_ddiss.begin();
	for(; itr_dv_ddiss != m_dv_ddiss.end(); itr_dv_ddiss++)
	{
		memcpy(m_pdata + offset, "dmsdiss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_ddiss->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_ddiss->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_ddiss->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS系统刀闸数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DMS系统刀闸数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统刀闸数量按主站
	offset = 0;
	rec_num = m_organ_gpdiss.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpdiss = m_organ_gpdiss.begin();
	for(; itr_organ_gpdiss != m_organ_gpdiss.end(); itr_organ_gpdiss++)
	{
		memcpy(m_pdata + offset, "pmsdiss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpdiss->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpdiss->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS系统刀闸数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "GPMS系统刀闸数量按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS系统刀闸数量按馈线
	offset = 0;
	rec_num = m_dv_gpdiss.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<string, ES_DATIMES>::iterator itr_dv_gpdiss = m_dv_gpdiss.begin();
	for(; itr_dv_gpdiss != m_dv_gpdiss.end(); itr_dv_gpdiss++)
	{
		memcpy(m_pdata + offset, "pmsdiss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpdiss->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_dv_gpdiss->second.count, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "馈线", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_dv_gpdiss->first.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS系统刀闸数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "GPMS系统刀闸数量按馈线当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::GetDMSBUSsByDay(ES_TIME *es_time)
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

	//DMS母线数
	/*sprintf(sql, "select organ_code,count(devid) from evalusystem.detail.dmsbus "
		"where oper!=2 group by organ_code");*/

	//母线匹配数
	sprintf(sql, "select a.organ_code,count(*) from evalusystem.detail.dmsbus a,"
		"evalusystem.detail.gpmsbus b where a.devid=b.devid and a.organ_code=b.organ_code "
		"group by a.organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSBus_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_dmsbus.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS母线数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSBUSsByDay(ES_TIME *es_time)
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

	//GPMS母线数
	/*sprintf(sql, "select organ_code,count(devid) from evalusystem.detail.gpmsbus "
		"where oper!=2 group by organ_code");*/

	//母线总数
	sprintf(sql, "select organ_code,count(*) from (select organ_code,devid from "
		"evalusystem.detail.dmsbus union select organ_code,devid from evalusystem.detail.gpmsbus) "
		"group by organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSBus_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_gpmsbus.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS母线数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetDMSSTsByDay(ES_TIME *es_time)
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

	//DMS站房数
	/*sprintf(sql, "select organ_code,count(devid) from evalusystem.detail.dmsst "
		"where oper!=2 group by organ_code");*/

	//站房匹配数
	sprintf(sql, "select a.organ_code,count(*) from evalusystem.detail.dmsst a,"
		"evalusystem.detail.gpmsst b where a.devid=b.devid and a.organ_code=b.organ_code "
		"group by a.organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSSt_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_dmsst.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS站房数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::GetGPMSSTsByDay(ES_TIME *es_time)
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

	//GPMS站房数
	/*sprintf(sql, "select organ_code,count(devid) from evalusystem.detail.gpmsst "
		"where oper!=2 group by organ_code");*/

	//站房总数
	sprintf(sql, "select organ_code,count(*) from (select organ_code,devid from "
		"evalusystem.detail.dmsst union select organ_code,devid from evalusystem.detail.gpmsst) "
		"group by organ_code");

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_GPMSTs_Day.txt");
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
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					dgpms.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_gpmsst.insert(make_pair(dgpms.organ_code, dgpms.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\t:"<<dgpms.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取GPMS站房数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisDGPMS::InsertResultBUSsRate(int statis_type, ES_TIME *es_time)
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
	std::map<int, float>::iterator itr_bus_result;

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

	//配电母线数完整率按主站
	rec_num = m_bus_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_bus_result = m_bus_result.begin();
	for(; itr_bus_result != m_bus_result.end(); itr_bus_result++)
	{
		memcpy(m_pdata + offset, "busfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_bus_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_bus_result->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电母线数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电母线数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_bus_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_bus_result = m_bus_score.begin();
	for(; itr_bus_result != m_bus_score.end(); itr_bus_result++)
	{
		memcpy(m_pdata + offset, "busfines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_bus_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_bus_result->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电母线数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电母线数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//DMS母线数按主站
	offset = 0;
	rec_num = m_organ_dmsbus.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dmsbus = m_organ_dmsbus.begin();
	for(; itr_organ_dmsbus != m_organ_dmsbus.end(); itr_organ_dmsbus++)
	{
		memcpy(m_pdata + offset, "dmsbuss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsbus->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsbus->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS母线数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电母线数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS母线数按主站
	offset = 0;
	rec_num = m_organ_gpmsbus.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpmsbus = m_organ_gpmsbus.begin();
	for(; itr_organ_gpmsbus != m_organ_gpmsbus.end(); itr_organ_gpmsbus++)
	{
		memcpy(m_pdata + offset, "pmsbuss", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsbus->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsbus->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS母线数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电母线数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::InsertResultSTsRate(int statis_type, ES_TIME *es_time)
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
	std::map<int, float>::iterator itr_st_result;

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

	//配电站房数完整率按主站
	rec_num = m_st_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_st_result = m_st_result.begin();
	for(; itr_st_result != m_st_result.end(); itr_st_result++)
	{
		memcpy(m_pdata + offset, "substionfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_st_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_st_result->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电站房数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_st_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_st_result = m_st_score.begin();
	for(; itr_st_result != m_st_score.end(); itr_st_result++)
	{
		memcpy(m_pdata + offset, "substionfines", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_st_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_st_result->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电站房数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//DMS站房数数按主站
	offset = 0;
	rec_num = m_organ_dmsst.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_dmsst = m_organ_dmsst.begin();
	for(; itr_organ_dmsst != m_organ_dmsst.end(); itr_organ_dmsst++)
	{
		memcpy(m_pdata + offset, "dmssts", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsst->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_dmsst->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//GPMS站房数数按主站
	offset = 0;
	rec_num = m_organ_gpmsst.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpmsst = m_organ_gpmsst.begin();
	for(; itr_organ_gpmsst != m_organ_gpmsst.end(); itr_organ_gpmsst++)
	{
		memcpy(m_pdata + offset, "pmssts", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsst->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpmsst->second, col_attr[2].data_size);
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
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDGPMS::GetDMSCorrectiveByDay(ES_TIME *es_time)
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
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//未整改设备数
	sprintf(sql, "select organ.organ_code,decode(cro.newnum,null,0,cro.newnum),decode(cro.oldnum,null,0,cro.oldnum) from "
		"(select a.organ_code co,a.cb+a.dis+a.st+a.bus+a.ld+a.status+a.tz+a.cj+a.topo newnum,b.cb+b.dis+b.st+b.bus+b.ld+b.status+b.tz+b.cj+b.topo oldnum "
		"from evalusystem.result.CORRECTIVE a,evalusystem.result.CORRECTIVE_DENOMINATOR b where a.organ_code = b.organ_code and a.count_time = b.count_time"
		" and a.count_time = to_date('%04d-%02d-%02d','YYYY-MM-DD')) cro right join evalusystem.config.organ organ on(cro.co = organ.organ_code)where 1=1"
		"order by organ.organ_code;",byear,bmonth,bday);

	ret = d5000.d5000_ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_Corrective dfail;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/DGPMS_DMSCBs_Day.txt");
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
			m_organ_corrective.insert(make_pair(dfail.organ_code,dfail));
#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dfail.organ_code<<"\tconfailnum:"<<dfail.confailnum<<"\tfailnum:"<<dfail.failnum<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS开关总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		d5000.g_CDci.FreeReadData(col_attr, col_num, buffer);
	else
		d5000.g_CDci.FreeColAttrData(col_attr, col_num);

	return 0;
}

//未应用
int StatisDGPMS::GetDMSCorrectiveByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 4];
	char tmp_sql[MAX_SQL_LEN * 2];
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

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='wrongnum' "
		"and dimname='无' group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),0),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='nrec'"
		" and dimname='无' group by "
		"code,organ_code,datatype", byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where code='rectification' and dimname='无'" 
		" group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = d5000.d5000_ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "DMS与GPMS信息交互月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

//未应用
int StatisDGPMS::GetDMSCorrectiveRate(int statis_type, ES_TIME *es_time)
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
		s_time = day * 24;


	//两周指标整改率
	map<int, float>::iterator itr_organ_result;
	map<int, ES_Corrective>::iterator itr_organ_corrective;
	map<string, float>::iterator itr_code_weight;

	//按主站
	itr_organ_corrective = m_organ_corrective.begin();
	for(; itr_organ_corrective != m_organ_corrective.end(); itr_organ_corrective++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_corrective->first;
		if(itr_organ_corrective->second.confailnum != 0 && itr_organ_corrective->second.failnum != 0)
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, (1-itr_organ_corrective->second.confailnum/itr_organ_corrective->second.failnum)));
			es_result.m_fresult = 1-(itr_organ_corrective->second.confailnum/itr_organ_corrective->second.failnum);
		}
		else if(itr_organ_corrective->second.confailnum == 0)
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, 1.0));
			es_result.m_fresult = 1.0;
		}
		else
		{
			//m_corrective_result.insert(make_pair(itr_organ_corrective->first, 0.0));
			es_result.m_fresult = 0.0;
		}

		m_corrective_result.insert(make_pair(DIM_DMS,es_result));

		/*es_score.m_ntype = itr_organ_corrective->first;
		itr_code_weight = m_code_weight.find("rectification");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		else
			es_score.m_fresult = 0.0;

		m_corrective_result.insert(make_pair(DIM_SCORE,es_score));*/
	}
	return 0;
}

int StatisDGPMS::InsertResultCorrectivesRate(int statis_type, ES_TIME *es_time)
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

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//两周未整改率按主站
	rec_num = m_corrective_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cor_result = m_corrective_result.find(DIM_DMS);
	for(int i = 0;i < rec_num;i++)
	{
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

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电站房数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);


	//得分
	/*offset = 0;
	rec_num = m_corrective_result.count(DIM_SCORE);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_cor_result = m_corrective_result.find(DIM_SCORE);
	for(int i = 0;i < rec_num;i++)
	{
		memcpy(m_pdata + offset, "rectifications", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_cor_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_cor_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_cor_result++;
	}

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "配电站房数完整率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//两周未整改设备个数
	offset = 0;
	rec_num = m_organ_corrective.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, ES_Corrective>::iterator itr_organ_judged = m_organ_corrective.begin();
	for(; itr_organ_judged != m_organ_corrective.end(); itr_organ_judged++)
	{
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

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "DMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//两周前错误设备总数
	offset = 0;
	rec_num = m_organ_corrective.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_organ_judged = m_organ_corrective.begin();
	for(; itr_organ_judged != m_organ_corrective.end(); itr_organ_judged++)
	{
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

	if(rec_num > 0)
	{
		ret = d5000.d5000_WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "GPMS站房数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "配电站房数完整率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

/*int StatisDGPMS::GetWeight()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;


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

