/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_log.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 得分插入
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <stdarg.h>
#include "es_score.h"
#include "es_define.h"

using namespace std;

ESScore::ESScore()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

ESScore::~ESScore()
{
	m_oci->DisConnect(&err_info);
}

int ESScore::GetWeight()
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
}

int ESScore::GetScore()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql,"select code,value,cast(score as float) from evalusystem.config.score;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_SCORE es_score;
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
					es_score.code = string(tempstr);
					break;
				case 1:
					es_score.value = *(double *)(tempstr);
					break;
				case 2:
					es_score.score = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_code_score.insert(make_pair(es_score.code, es_score));
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
}

int ESScore::GetMark(int statis_type, ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	char table_name[MAX_NAME_LEN];

	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql,"select organ_code,code,value from evalusystem.result.%s where datatype = '百分比' and dimname = '无';",table_name);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_MARK es_mark;
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
					es_mark.organ_code = *(int *)(tempstr);
					break;
				case 1:
					es_mark.code = string(tempstr);
					break;
				case 2:
					es_mark.value = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_code_mark.insert(make_pair(es_mark.code, es_mark));
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
}

int ESScore::CaculateMark()
{
	if(m_code_mark.size() == 0)
		return 0;

	ES_MARK es_mark;
	itr_code_mark = m_code_mark.begin();

	for (;itr_code_mark != m_code_mark.end();itr_code_mark++)
	{
		es_mark.organ_code = itr_code_mark->second.organ_code;
		es_mark.code = itr_code_mark->first;
		GetK(itr_code_mark->first,itr_code_mark->second.value,es_mark.value);

		m_code_pointmark.insert(make_pair(es_mark.code,es_mark));
	}
	return 0;
}

int ESScore::GetK(const string &code,float v,float &k)
{
	int rec_num = 0,i = 0;
	float maxfloat = 1;
	float minfloat = 0;
	float maxnum = 100;
	float minnum = 0;

	rec_num = m_code_score.count(code);
	if (rec_num != 0)
	{
		itr_code_score = m_code_score.find(code);

		for(i = 0;i < rec_num; i++)
		{
			if (v > 1)
			{
				k = 100;
				return 0;
			}
			else if (itr_code_score->second.value == v)
			{
				k = itr_code_score->second.score;
				return 0;
			}
			else if(itr_code_score->second.value < v && itr_code_score->second.value >= minfloat)
			{
				minfloat = itr_code_score->second.value;
				minnum = itr_code_score->second.score;
			}
			else if(itr_code_score->second.value > v && itr_code_score->second.value <= minfloat)
			{
				maxfloat = itr_code_score->second.value;
				maxnum = itr_code_score->second.score;
			}

			itr_code_score++;
		}
	}

	k = v*(maxnum-minnum)/(maxfloat-minfloat);

	return 0;
}

void ESScore::Clear()
{
	m_code_weight.clear();
	m_code_score.clear();
	m_code_mark.clear();
	m_code_pointmark.clear();
}

int ESScore::InsertNum(int statis_type, ES_TIME *es_time)
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

	//分值插入
	rec_num = m_code_pointmark.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		return -1;
	}

	itr_code_pointmark = m_code_pointmark.begin();
	for(;itr_code_pointmark != m_code_pointmark.end();itr_code_pointmark++)
	{
		memcpy(m_pdata + offset, itr_code_pointmark->second.code.c_str(), col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_code_pointmark->second.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_code_pointmark->second.value, col_attr[2].data_size);
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

int ESScore::InsertPointScore(int statis_type, ES_TIME *es_time)
{
	Clear();
	/*if(GetWeight() != 0)
	return -1;*/

	if(GetScore() != 0)
		return -1;

	if(GetMark(statis_type,es_time) != 0)
		return -1;

	if(CaculateMark() != 0)
		return -1;

	InsertNum(statis_type,es_time);

	return 0;
}

#if 0
int ESScore::InsertPointScore(int statis_type, ES_TIME *es_time)
{
	int ret;
	int year,month,day;
	int byear, bmonth, bday;
	char qury[2048];
	char table_name[25];
	int rec_num;
	int col_num;
	char *buffer = NULL;
	struct ColAttr *col_attr = NULL;

	byear = es_time->year;
	bmonth = es_time->month;
	bday = es_time->day;
	GetBeforeTime(STATIS_MONTH, byear, bmonth ,bday, &year, &month, &day);

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(qury,"SELECT VALUE,SCORE FROM EVALUSYSTEM.CONFIG.SCORE WHERE CODE = '%s' AND VALUE IN "
		"(SELECT VALUE FROM EVALUSYSTEM.RESULT.MONTH_%04d%02d WHERE CODE = '%s' AND ORGAN_CODE = %d "
		"AND DATATYPE = '百分比' AND DIMNAME = '无');",
		code.c_str(),year,month,code.c_str(),organ_code);

	ret = m_oci->ReadData(qury, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret != 1)
	{
		cout << "exec " << qury << " error!errno:" << err_info.error_no << " err_info:" << err_info.error_info << endl;
		return -1;
	}

	if (rec_num == 0)
	{
		sprintf(qury,"INSERT INTO EVALUSYSTEM.RESULT.%s(CODE,ORGAN_CODE,VALUE,DATATYPE,UPDATETIME,STATISTIME,DIMNAME,DIMVALUE) "
			"(SELECT C.CODE,C.ORGAN_CODE,ROUND(A.SCORE-(A.SCORE-B.SCORE)*(A.VALUE-C.VALUE)/(A.VALUE-B.VALUE),4),'数值',SYSDATE,TO_DATE('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' "
			"FROM (SELECT MIN(VALUE) VALUE,MIN(CAST(SCORE AS FLOAT)) SCORE FROM EVALUSYSTEM.CONFIG.SCORE WHERE VALUE = (SELECT MIN(B.VALUE) VALUE FROM (SELECT VALUE FROM EVALUSYSTEM.RESULT.%s WHERE "
			" DATATYPE = '百分比' AND DIMNAME = '无') A,(SELECT CAST(SCORE AS FLOAT) SCORE,VALUE FROM EVALUSYSTEM.CONFIG.SCORE) B "
			"WHERE A.VALUE < B.VALUE)) A,(SELECT MIN(VALUE) VALUE,MAX(CAST(SCORE AS FLOAT)) SCORE FROM EVALUSYSTEM.CONFIG.SCORE WHERE VALUE = (SELECT MAX(B.VALUE) FROM (SELECT VALUE FROM "
			"EVALUSYSTEM.RESULT.%s WHERE DATATYPE = '百分比' AND DIMNAME = '无') A,(SELECT CAST(SCORE AS FLOAT) SCORE,VALUE FROM "
			"EVALUSYSTEM.CONFIG.SCORE) B WHERE A.VALUE > B.VALUE)) B,(SELECT CODE,ORGAN_CODE,VALUE FROM EVALUSYSTEM.RESULT.%s WHERE "
			"DATATYPE = '百分比' AND DIMNAME = '无') C WHERE 1=1);",
			table_name,year,month,day,table_name,table_name,table_name);
	}
	else
	{
		sprintf(qury,"INSERT INTO EVALUSYSTEM.RESULT.%s(CODE,ORGAN_CODE,VALUE,DATATYPE,UPDATETIME,STATISTIME,DIMNAME,DIMVALUE) "
			"(SELECT A.CODE,A.ORGAN_CODE,MAX(K),'数值',SYSDATE,TO_DATE('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' FROM "
			"(SELECT CAST(B.SCORE AS FLOAT) K FROM (SELECT ORGAN_CODE,CODE,VALUE FROM EVALUSYSTEM.RESULT.%s"
			"AND DATATYPE = '百分比' AND DIMNAME = '无') A,(SELECT SCORE,VALUE FROM EVALUSYSTEM.CONFIG.SCORE) B WHERE A.VALUE = B.VALUE));",
			table_name,year,month,day,table_name);
	}
	
	ret = m_oci->ExecSingle(qury,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,qury);
		return -1;
	}

	return 0;
}

#endif

int ESScore::InsertScore(int statis_type, ES_TIME *es_time)
{

	int ret;
	int year, mon, day, days;
	char sql[MAX_SQL_LEN];

	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;
	days = GetDaysInMonth(year, mon);

	if(mon == 1 && day == 1)
	{
		year = year - 1;
		mon = 12;
		day = 31;
	}
	else if(mon != 1 && day == 1)
	{
		year = year;
		mon = mon - 1;
		day = GetDaysInMonth(year, mon);
	}
	else
	{
		year = year;
		mon = mon;
		day = day - 1;
	}

	//删除总分旧记录
	if(statis_type == STATIS_DAY)
	{
		sprintf(sql, "delete from evalusystem.result.day_%04d%02d%02d where code = 'score';", year,mon,day);
	}
	else
	{
		sprintf(sql, "delete from evalusystem.result.month_%04d%02d where code = 'score';", year,mon);
	}
	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	//插入总分
	if(statis_type == STATIS_DAY)
	{
		sprintf(sql, "insert into evalusystem.result.day_%04d%02d%02d(code,organ_code,value,datatype,"
			"statistime,updatetime,dimname,dimvalue) (select 'score',a.organ_code,round(sum(a.value*b.weight/c.s),4),'数值',"
			"to_date('%04d-%02d%-02d','YYYY-MM-DD'),sysdate,'无','无' from (select * from evalusystem.result.day_%04d%02d%02d "
			"a where updatetime=(select max(updatetime) from evalusystem.result.day_%04d%02d%02d b where a.code=b.code and "
			"a.organ_code=b.organ_code and  a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue and "
			"datatype = '数值' and code != 'score')) a,(select * from evalusystem.config.weight a where "
			"settime=(select max(settime) from evalusystem.config.weight b where a.code=b.code)) b,(select sum(weight) s "
			"from (select * from evalusystem.config.weight a where settime=(select max(settime) from evalusystem.config.weight b "
			"where a.code=b.code))) c where a.code = b.code and datatype = '数值' and dimname = '无' and a.code != 'score' "
			"group by a.organ_code);", year,mon,day,year,mon,day,year,mon,day,year,mon,day);
	}
	else
	{
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
			"statistime,updatetime,dimname,dimvalue) (select 'score',a.organ_code,round(sum(a.value*b.weight/c.s),4),'数值',"
			"to_date('%04d-%02d-%02d','YYYY-MM-DD'),sysdate,'无','无' from (select * from evalusystem.result.month_%04d%02d "
			"a where updatetime=(select max(updatetime) from evalusystem.result.month_%04d%02d b where a.code=b.code and "
			"a.organ_code=b.organ_code and  a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue and "
			"datatype = '数值' and code != 'score')) a,(select * from evalusystem.config.weight a where settime=(select max(settime) "
			"from evalusystem.config.weight b where a.code=b.code)) b,(select sum(weight) s from (select * from evalusystem.config.weight "
			"a where settime=(select max(settime) from evalusystem.config.weight b where a.code=b.code))) c where a.code = b.code and "
			"datatype = '数值' and dimname = '无' and a.code != 'score' group by a.organ_code);",
			year,mon,year,mon,day,year,mon,year,mon);
	}

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
	return 0;
}

