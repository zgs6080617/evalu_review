/*******************************************************************************
* Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
* File    : statis_net.h
* Author  : lichengming
* Version : v1.0
* Function : 指令票指标
* History
* ===============================================
* 2015-11-12  lichengming create
* ===============================================
*******************************************************************************/
#include "statis_zlp.h"

StatisZLP::StatisZLP()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisZLP::~StatisZLP()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

//获取DMS指令票数量
int StatisZLP::GetDMSZLPnum(int statis_type,ES_TIME *es_time)
{
	map_dmszlp_num.clear();
	char tempstr[100],bfday[20],bfmonth[10];
	int num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buf = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	
	int year,month,day;
	int byear,bmonth,bday;
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);
	sprintf(bfday,"%04d-%02d-%02d",byear,bmonth,bday);
	sprintf(bfmonth,"%04d-%02d",byear,bmonth);
	if(statis_type == STATIS_DAY)
	{
		sprintf(sql,"select a.organ_code, count(*) c from "
		"(select organ_code, dmsid from evalusystem.detail.dms_zlp where count_time like '%s%%' and (type like '停电%%' or type = '01')) a, "
		"(select organ_code, dmsid from evalusystem.detail.gpms_zlp where count_time like '%s%%' and type like '停电%%') b "
		"where a.organ_code = b.organ_code and a.dmsid = b.dmsid group by a.organ_code;",bfday,bfday);
	}
	else
	{
		sprintf(sql,"select a.organ_code, count(*) c from "
		"(select organ_code, dmsid from evalusystem.detail.dms_zlp where count_time like '%s%%' and (type like '停电%%' or type = '01')) a, "
		"(select organ_code, dmsid from evalusystem.detail.gpms_zlp where count_time like '%s%%' and type like '停电%%') b "
		"where a.organ_code = b.organ_code and a.dmsid = b.dmsid group by a.organ_code;",bfmonth,bfmonth);
	}

	if(m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buf, &err_info) > 0)
	{
		m_presult = buf;
		int m_organ_code;
		float m_num;
		int rec, col;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0x00 , 100);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					m_organ_code = *(int*)(tempstr);
					break;
				case 1:
					m_num = *(int*)(tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_dmszlp_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//获取GPMS指令票数量
int StatisZLP::GetGPMSZLPnum(int statis_type,ES_TIME *es_time)
{
	map_gpmszlp_num.clear();
	char tempstr[100],bfday[20],bfmonth[10];
	int num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buf = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	
	int year,month,day;
	int byear,bmonth,bday;
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);
	sprintf(bfday,"%04d-%02d-%02d",byear,bmonth,bday);
	sprintf(bfmonth,"%04d-%02d",byear,bmonth);
	
	if(statis_type == STATIS_DAY)
	{
		sprintf(sql,"select organ.organ_code,decode(a.c,null,0,a.c) from"
		"(select organ_code,count(*) c from evalusystem.detail.gpms_zlp where count_time like '%s%%' and type like '停电%%' group by organ_code) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);",bfday);
	}
	else
	{
		sprintf(sql,"select organ.organ_code,decode(a.c,null,0,a.c) from"
		"(select organ_code,count(*) c from evalusystem.detail.gpms_zlp where count_time like '%s%%' and type like '停电%%' group by organ_code) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);",bfmonth);
	}

	if(m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buf, &err_info) > 0)
	{
		m_presult = buf;
		int m_organ_code;
		float m_num;
		int rec, col;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0x00 , 100);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					m_organ_code = *(int*)(tempstr);
					break;
				case 1:
					m_num = *(int*)(tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_gpmszlp_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//计算指令票指标值
int StatisZLP::SetZLPRate()
{
	map_zlp_mark.clear();
	//map<int,float>::iterator it_dmszlp_num = map_dmszlp_num.begin();
	map<int,float>::iterator it_gpmszlp_num = map_gpmszlp_num.begin();
	for(;it_gpmszlp_num!=map_gpmszlp_num.end();it_gpmszlp_num++)
	{
		float result;
		//map<int,float>::iterator it_gpmszlp_num = map_gpmszlp_num.find(it_dmszlp_num->first);
		map<int,float>::iterator it_dmszlp_num = map_dmszlp_num.find(it_gpmszlp_num->first);
		if(it_dmszlp_num != map_dmszlp_num.end())
		{
			if( it_gpmszlp_num->second == 0)
				result = 0.0;
			else if(it_dmszlp_num->second == 0)
				result = 0.0;
			else
				result = it_dmszlp_num->second / it_gpmszlp_num->second;
		}
		else
		{
			char m_organ_code[10];
			memset(m_organ_code,0x00,sizeof(m_organ_code));
			sprintf(m_organ_code,"%d",it_gpmszlp_num->first);
			if(strlen(m_organ_code)>5)
				result = 1.0;
			else
				result = 0.0;
		}
		map_zlp_mark.insert(make_pair(it_gpmszlp_num->first,result));
	}
	return 0;
}

//插入指令票数据到数据库
int StatisZLP::InsertZLPRate(int statis_type, ES_TIME *es_time)
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
	map<int,float>::iterator it_dmszlp_num;
	map<int,float>::iterator it_gpmszlp_num;
	map<int,float>::iterator it_zlp_mark;

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
		
	offset = 0;
	rec_num = map_dmszlp_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_dmszlp_num = map_dmszlp_num.begin();
	for(; it_dmszlp_num != map_dmszlp_num.end(); it_dmszlp_num++)
	{
		memcpy(m_pdata + offset, "dmszlpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_dmszlp_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_dmszlp_num->second, col_attr[2].data_size);
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
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
	}
	FREE(m_pdata);
	
	offset = 0;
	rec_num = map_gpmszlp_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_gpmszlp_num = map_gpmszlp_num.begin();
	for(; it_gpmszlp_num != map_gpmszlp_num.end(); it_gpmszlp_num++)
	{
		memcpy(m_pdata + offset, "gpmszlpnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gpmszlp_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gpmszlp_num->second, col_attr[2].data_size);
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
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
	}
	FREE(m_pdata);
	
	offset = 0;
	rec_num = map_zlp_mark.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_zlp_mark = map_zlp_mark.begin();
	for(; it_zlp_mark != map_zlp_mark.end(); it_zlp_mark++)
	{
		memcpy(m_pdata + offset, "zlpfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_zlp_mark->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_zlp_mark->second, col_attr[2].data_size);
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
	
	return 0;
}

//指令票计算
int StatisZLP::GetZLPRate(int statis_type, ES_TIME *es_time)
{
	Clear();
	if(GetDMSZLPnum(statis_type,es_time) < 0)
	{
		printf("GetDMSZLPnum failed.\n");
	}
	if(GetGPMSZLPnum(statis_type,es_time) < 0)
	{
		printf("GetGPMSZLPnum failed.\n");
	}
	SetZLPRate();
	if(InsertZLPRate(statis_type, es_time) < 0)
	{
		printf("InsertZLPRate failed.\n");
	}
}

int StatisZLP::Clear()
{
	map_dmszlp_num.clear();
	map_gpmszlp_num.clear();
	map_zlp_mark.clear();
}