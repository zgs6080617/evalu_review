/*******************************************************************************
* Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
* File    : statis_net.h
* Author  : lichengming
* Version : v1.0
* Function : 小水电指标
* History
* ===============================================
* 2015-12-10  lichengming create
* ===============================================
*******************************************************************************/
#include "statis_xsd.h"

StatisXSD::StatisXSD()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisXSD::~StatisXSD()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

//获取小水电数值
int StatisXSD::GetXSDsum(int statis_type,ES_TIME *es_time)
{
	map_xsd_sum.clear();
	char tempstr[100],bfday[20],bfmonth[10];
	int num;
	int rec_num;
	int col_num;
	char sql[200];
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
		"(select organ_code,value c from evalusystem.detail.dms_xsd where count_time like '%s%%') a "
		"right join evalusystem.config.organ organ on(a.organ_code = organ.organ_code);",bfday);
	}
	else
	{
		sprintf(sql,"select organ.organ_code,decode(a.c,null,0,a.c) from"
		"(select organ_code,sum(value) c from evalusystem.detail.dms_xsd where count_time like '%s%%' group by organ_code) a "
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
					m_num = *(double*)(tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_xsd_sum.insert(make_pair(m_organ_code,m_num));
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

//插入小水电数值到数据库
int StatisXSD::InsertXSD(int statis_type, ES_TIME *es_time)
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
	map<int,float>::iterator it_dmsxsd_sum;

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
	memset(sql,0x00,sizeof(sql));
	sprintf(sql,"delete from evalusystem.result.%s where code = 'xsdpower';",table_name);
	ret = m_oci->ExecSingle(sql,&err_info);
	if( !ret )
	{
		printf("DeleteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	
	memset(sql,0x00,sizeof(sql));
	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,2),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);
		
	offset = 0;
	rec_num = map_xsd_sum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_dmsxsd_sum = map_xsd_sum.begin();
	for(; it_dmsxsd_sum != map_xsd_sum.end(); it_dmsxsd_sum++)
	{
		memcpy(m_pdata + offset, "xsdpower", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_dmsxsd_sum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_dmsxsd_sum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "小水电", col_attr[3].data_size);
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

//获取小水电采集率数值
int StatisXSD::GetXSDRate(int statis_type,ES_TIME *es_time)
{
	map_xsd_rate.clear();
	char tempstr[100],bfday[20],bfmonth[10];
	int num;
	int rec_num;
	int col_num;
	char sql[200];
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
		sprintf(sql,"select organ_code,xsd_rate from evalusystem.detail.ycrate where count_time like '%s%%' and xsd_rate >= 0.0",bfday);
	}
	else
	{
		sprintf(sql,"select organ_code,avg(xsd_rate) c from evalusystem.detail.ycrate where count_time like '%s%%' and xsd_rate >= 0.0 group by organ_code ",bfmonth);
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
					m_num = *(double*)(tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_xsd_rate.insert(make_pair(m_organ_code,m_num));
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

//插入小水电采集率数值到数据库
int StatisXSD::InsertXSDRate(int statis_type, ES_TIME *es_time)
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
	map<int,float>::iterator it_dmsxsd_rate;

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
	memset(sql,0x00,sizeof(sql));
	sprintf(sql,"delete from evalusystem.result.%s where code = 'xsdcltrate';",table_name);
	ret = m_oci->ExecSingle(sql,&err_info);
	if( !ret )
	{
		printf("DeleteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		return -1;
	}
	
	memset(sql,0x00,sizeof(sql));
	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);
		
	offset = 0;
	rec_num = map_xsd_rate.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	it_dmsxsd_rate = map_xsd_rate.begin();
	for(; it_dmsxsd_rate != map_xsd_rate.end(); it_dmsxsd_rate++)
	{
		memcpy(m_pdata + offset, "xsdcltrate", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_dmsxsd_rate->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &it_dmsxsd_rate->second, col_attr[2].data_size);
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

//小水电数值计算
int StatisXSD::GetXSD(int statis_type, ES_TIME *es_time)
{
	Clear();
	GetXSDsum(statis_type,es_time);
	InsertXSD(statis_type,es_time);
	GetXSDRate(statis_type,es_time);
	InsertXSDRate(statis_type,es_time);
}

int StatisXSD::Clear()
{
	map_xsd_sum.clear();
	map_xsd_rate.clear();
}