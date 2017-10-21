/*******************************************************************************
* Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
* File    : statis_net.h
* Author  : lichengming
* Version : v1.0
* Function : 异动单指标
* History
* ===============================================
* 2015-11-12  lichengming create
* ===============================================
*******************************************************************************/
#include "statis_yd.h"

StatisYD::StatisYD()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisYD::~StatisYD()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

//获取DMS异动单数量
int StatisYD::GetGDMSYDDnum(int statis_type,ES_TIME *es_time)
{
	map_gdmsydd_num.clear();
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
		sprintf(sql,"select organ.organ_code,decode(cro.co,NULL,0,cro.co) from (select a.organ_code, count(*) co from "
		"(select distinct organ_code, ydd_id from evalusystem.detail.dms_yd where count_time like '%s%%' and status=31) a, "
		"(select distinct organ_code, ydd_id from evalusystem.detail.gpms_yd where send_time like '%s%%') b "
		"where a.organ_code = b.organ_code and substr(a.ydd_id,3,length(a.ydd_id)-2) = substr(b.ydd_id,3,length(b.ydd_id)-2) "
		" group by a.organ_code) cro right join evalusystem.config.organ organ on(cro.organ_code = organ.organ_code);",bfday,bfday);
	}
	else
	{
		sprintf(sql,"select organ.organ_code,decode(cro.co,NULL,0,cro.co) from (select a.organ_code, count(*) co from "
		"(select distinct organ_code, ydd_id,count_time from evalusystem.detail.dms_yd where count_time like '%s%%' and status=31 and flag = 0) a, "
		"(select distinct organ_code, ydd_id,count_time from evalusystem.detail.gpms_yd where send_time like '%s%%' and flag = 0) b "
		"where a.organ_code = b.organ_code and substr(a.ydd_id,3,length(a.ydd_id)-2) = substr(b.ydd_id,3,length(b.ydd_id)-2) and a.count_time = b.count_time "
		"group by a.organ_code) cro right join evalusystem.config.organ organ on(cro.organ_code = organ.organ_code);",bfmonth,bfmonth);
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
			map_gdmsydd_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//获取GPMS异动单数量
int StatisYD::GetGPMSYDDnum(int statis_type,ES_TIME *es_time)
{
	map_gpmsydd_num.clear();
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
		sprintf(sql,"select organ.organ_code,decode(a.c,null,0,a.c) from "
		"(select organ_code,count(*) c from evalusystem.detail.gpms_yd where send_time like '%s%%' group by organ_code) a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);",bfday);
	}
	else
	{
		sprintf(sql,"select organ.organ_code,decode(a.c,null,0,a.c) from "
		"(select organ_code,count(*) c from evalusystem.detail.gpms_yd where send_time like '%s%%' and flag = 0 group by organ_code) a "
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
			map_gpmsydd_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//计算异动单指标值
int StatisYD::SetYDDRate()
{
	map_ydd_mark.clear();
	map_ydd_right.clear();
	map<int,float>::iterator it_gdmsydd_num = map_gdmsydd_num.begin();
	for (;it_gdmsydd_num != map_gdmsydd_num.end();++it_gdmsydd_num) {

		float result;
		map<int,float>::iterator it_gpmsydd_num = map_gpmsydd_num.find(it_gdmsydd_num->first);
		if (it_gpmsydd_num != map_gpmsydd_num.end()) {

			if (it_gpmsydd_num->second == 0) {

				result = 1.0;
			}
			else {

				result = it_gdmsydd_num->second/it_gpmsydd_num->second;
			}
		}
		else {

			result = 1.0;
		}
		map_ydd_mark.insert(make_pair(it_gdmsydd_num->first,result));
	}

	//异动数据正确率
	it_gdmsydd_num = map_dmsydd_norollback_num.begin();
	for (;it_gdmsydd_num != map_dmsydd_norollback_num.end();++it_gdmsydd_num) {

		float result;
		map<int,float>::iterator it_gpmsydd_num = map_gdmsydd_num.find(it_gdmsydd_num->first);
		if (it_gpmsydd_num != map_gdmsydd_num.end()) {

			if (it_gpmsydd_num->second == 0) {

				result = 1.0;
			}
			else {

				result = it_gdmsydd_num->second/it_gpmsydd_num->second;
			}
		}
		else {
			
			result = 1.0;
		}
		map_ydd_right.insert(make_pair(it_gdmsydd_num->first,result));
	}
	return 0;
}

//插入异动单数据到数据库
int StatisYD::InsertYDDRate(int statis_type, ES_TIME *es_time)
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
	map<int,float>::iterator it_gdmsydd_num;
	map<int,float>::iterator it_gpmsydd_num;
	map<int,float>::iterator it_ydd_mark;

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
	rec_num = map_gdmsydd_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_gdmsydd_num = map_gdmsydd_num.begin();
	for(; it_gdmsydd_num != map_gdmsydd_num.end(); it_gdmsydd_num++)
	{
		memcpy(m_pdata + offset, "gdmsyddnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gdmsydd_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gdmsydd_num->second, col_attr[2].data_size);
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

	//异动单未回退数量
	offset = 0;
	rec_num = map_dmsydd_norollback_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_gdmsydd_num = map_dmsydd_norollback_num.begin();
	for(; it_gdmsydd_num != map_dmsydd_norollback_num.end(); ++it_gdmsydd_num)
	{
		memcpy(m_pdata + offset, "dmsnorollnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gdmsydd_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gdmsydd_num->second, col_attr[2].data_size);
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
	rec_num = map_gpmsydd_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_gpmsydd_num = map_gpmsydd_num.begin();
	for(; it_gpmsydd_num != map_gpmsydd_num.end(); it_gpmsydd_num++)
	{
		memcpy(m_pdata + offset, "gpmsyddnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->second, col_attr[2].data_size);
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
	rec_num = map_gpmsydd_num.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	/*it_gpmsydd_num = map_ydd_total_num.begin();
	for(; it_gpmsydd_num != map_ydd_total_num.end(); it_gpmsydd_num++)
	{
		memcpy(m_pdata + offset, "dmsyddnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}*/

	it_gpmsydd_num = map_gpmsydd_num.begin();
	for(; it_gpmsydd_num != map_gpmsydd_num.end(); it_gpmsydd_num++)
	{
		memcpy(m_pdata + offset, "dmsyddnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_gpmsydd_num->second, col_attr[2].data_size);
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
	rec_num = map_ydd_mark.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_ydd_mark = map_ydd_mark.begin();
	for(; it_ydd_mark != map_ydd_mark.end(); it_ydd_mark++)
	{
		memcpy(m_pdata + offset, "yddfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_ydd_mark->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_ydd_mark->second, col_attr[2].data_size);
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

	//异动数据正确率
	offset = 0;
	rec_num = map_ydd_right.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_ydd_mark = map_ydd_right.begin();
	for(; it_ydd_mark != map_ydd_right.end(); it_ydd_mark++)
	{
		memcpy(m_pdata + offset, "yddrightfine", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_ydd_mark->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_ydd_mark->second, col_attr[2].data_size);
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

//异动单计算
int StatisYD::GetYDDRate(int statis_type, ES_TIME *es_time)
{
	Clear();
	if(GetGDMSYDDnum(statis_type,es_time) < 0)
	{
		printf("GetDMSYDDnum failed.\n");
	}
	if(GetGPMSYDDnum(statis_type,es_time) < 0)
	{
		printf("GetGPMSYDDnum failed.\n");
	}
	if(GetYDDnumNoRoolback(statis_type,es_time) < 0)
	{
		printf("GetYDDnumNoRoolback failed.\n");
	}
	if(GetYDDnumTotal(statis_type,es_time) < 0)
	{
		printf("GetYDDnumTotal failed.\n");
	}
	SetYDDRate();
	if(InsertYDDRate(statis_type, es_time) < 0)
	{
		printf("InsertYDDRate failed.\n");
	}
	GetGDMSYDDDetail(es_time);
	return 0;
}

int StatisYD::Clear()
{
	map_gdmsydd_num.clear();
	map_gpmsydd_num.clear();
	map_ydd_mark.clear();
	map_dmsydd_norollback_num.clear();
	map_ydd_total_num.clear();
	map_ydd_right.clear();
}

int StatisYD::GetYDDnumNoRoolback( int statis_type,ES_TIME *es_time )
{
	map_dmsydd_norollback_num.clear();
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
		sprintf(sql,"select organ.organ_code,decode(cro.co,NULL,0,cro.co) from (select a.organ_code, count(*) co from "
			"(select distinct organ_code, ydd_id from evalusystem.detail.dms_yd where count_time like '%s%%' and status=31 and rollback_time = 0) a, "
			"(select distinct organ_code, ydd_id from evalusystem.detail.gpms_yd where send_time like '%s%%') b "
			"where a.organ_code = b.organ_code and substr(a.ydd_id,3,length(a.ydd_id)-2) = substr(b.ydd_id,3,length(b.ydd_id)-2) "
			" group by a.organ_code) cro right join evalusystem.config.organ organ on(cro.organ_code = organ.organ_code);",bfday,bfday);
	}
	else
	{
		sprintf(sql,"select organ.organ_code,decode(cro.co,NULL,0,cro.co) from (select a.organ_code, count(*) co from "
			"(select distinct organ_code, ydd_id,count_time from evalusystem.detail.dms_yd where count_time like '%s%%' and status=31 and  rollback_time = 0 and flag = 0) a, "
			"(select distinct organ_code, ydd_id,count_time from evalusystem.detail.gpms_yd where send_time like '%s%%' and flag = 0) b "
			"where a.organ_code = b.organ_code and substr(a.ydd_id,3,length(a.ydd_id)-2) = substr(b.ydd_id,3,length(b.ydd_id)-2) and a.count_time = b.count_time "
			"group by a.organ_code) cro right join evalusystem.config.organ organ on(cro.organ_code = organ.organ_code);",bfmonth,bfmonth);
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
			map_dmsydd_norollback_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

int StatisYD::GetYDDnumTotal( int statis_type,ES_TIME *es_time )
{
	map_ydd_total_num.clear();
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
		sprintf(sql,"select organ_code,count(*) from evalusystem.detail.dms_yd where count_time like '%s%%' and flag = 0 "
			" group by organ_code;",bfday);
	}
	else
	{
		sprintf(sql,"select organ_code,count(*) from evalusystem.detail.dms_yd where count_time like '%s%%' and flag = 0"
			"  group by organ_code;",bfmonth);
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
			map_ydd_total_num.insert(make_pair(m_organ_code,m_num));
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

int StatisYD::GetGDMSYDDDetail( ES_TIME *es_time )
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

	sprintf(sql, "call evalusystem.result.YD_DETAIL_TO_RESULT('%04d-%02d-%02d')", byear,bmonth,bday);
	//cout<<sql<<endl;
	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	return 0;
}

