/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_main.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : ͳ��������
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <time.h>
#include <math.h>
#include <sys/stat.h>
#include "datetime.h"
#include "dcisg.h"
#include "statis_dms.h"
#include "statis_da.h"
#include "statis_model.h"
#include "statis_dgpms.h"
#include "statis_terminal.h"
#include "statis_yc.h"
#include "statis_support.h"
#include "es_log.h"
#include "es_score.h"
#include "statis_net.h"
#include "statis_zlp.h"

using namespace std;

#define START_TIME 6	//Ĭ������ʱ��

//ͳ��ʧ��map
multimap<int, ES_ERROR_INFO> mul_type_err;

//��ͳ�Ƶ���ͳ�Ʊ�
//vector<string> v_talbe_name;

//������/��ͳ�Ʊ�
int CreateStatisTable(ES_TIME *es_time);

//����ͳ�Ƶ���ͳ�Ʊ�
//int CheckStatisTable(ES_TIME *es_time);

//ɾ��ͳ�Ʊ�����
int DeleteStatisData(int statis_type, ES_TIME *es_time);

//��ʼ������ͳ��
int InitFirstStatis(const string &begin_time, const string &end_time);

//ͳ��
int StatisAnalyze(ES_TIME *es_time, int cooperation);

//У��ʱ���ʽ
int CheckTimeString(const string &check_time);

//�����־
void* threadlogwrite(void *param);

//����У���ȥ�� add by lcm 20150915
int Data_Check();

//����
void Help();

//���dms��gpms������
int TruncateData();

//��ȡ��������
int GetStartConfig(int *model, int *statis, int *cooperation);

//������ͳ��ʱ���ж�
bool CheckCorrective(ES_TIME *es_time);

int main(int argc, char **argv)
{
	int num, ret;
	int break_flag = 0;
	multimap<int, ES_ERROR_INFO>::iterator itr_type_err;
	
	//����д��־�߳�
	pthread_t pt;
	pthread_attr_t attr;

	pthread_attr_init(&attr);

	if (pthread_create(&pt, &attr, threadlogwrite, NULL) != 0)
	{
		printf("����д��־�߳�ʧ�ܣ�\r\n");
		return -1;
	}
	pthread_detach(pt);

	//��ʼ������ͳ��
	if(argc == 2)
	{
		ret = InitFirstStatis(argv[1], "");
		if(ret < 0)
		{
			cout<<"���ڸ�ʽ����"<<endl;
			return -1;
		}
	}
	else if(argc == 3)
	{
		break_flag = 1;
		ret = InitFirstStatis(argv[1], argv[2]);
		if(ret < 0)
		{
			cout<<"���ڸ�ʽ����"<<endl;
			return -1;
		}
	}
	else if(argc > 3)
	{
		Help();
		return -1;
	}

#if 1
	for( ; ; )
	{
		if(break_flag == 1)
			break;

		int year,month,day,hour,min,sec;
		//��ȡ��ǰʱ��
		GetCurTime(&hour, &min, &sec);
		GetCurDate(&year, &month, &day);
		
		ES_TIME es_time;
		es_time.year = year;
		es_time.month = month;
		es_time.day = day;
		printf("hour:%d,min:%d,sec:%d\n",hour,min,sec);
		//��ȡ��������
		int model, statis, cooperation;
		GetStartConfig(&model, &statis, &cooperation);
		//cout<<model<<"\t"<<statis<<"\t"<<cooperation<<endl;
		
		if(0 == hour && 0 == min)
		{
			TruncateData();
		}

		if(0 == min || 30 == min)
		{
			StatisNet net;
			net.GetConfig();
			net.UpdateNetCount();
			net.InsertNetScore();
			//net_check_count++;
		}

		if(cooperation == 0 && model == hour && min == 0)//mode = 6
		{
			StatisModel s_model(&mul_type_err);
			ret = s_model.ModelCheck(&es_time);
			GetCurTime(&hour, &min, &sec);
			if(statis < hour)//4 statis = 7
			{
				if(CheckCorrective(&es_time))	//������ͳ��
				{
					StatisSupport s_support;
					s_support.GetCorrective(&es_time);
				}

				ret = StatisAnalyze(&es_time, cooperation);

			}
		}

		if(statis == hour && min == 0)//4 statis=7
		{
			if(CheckCorrective(&es_time))	//������ͳ��
			{
				StatisSupport s_support;
				s_support.GetCorrective(&es_time);
			}

			ret = StatisAnalyze(&es_time, cooperation);

		}

		if (14 == hour && min == 0)
		{
			char date[10];
			int byear,bmonth,bday;
			GetBeforeTime(0,year,month,day,&byear,&bmonth,&bday);
			sprintf(date,"%04d%02d%02d",byear,bmonth,bday);
			ret = InitFirstStatis(date, date);
			if(ret < 0)
			{
				cout<<"���ڸ�ʽ����"<<endl;
				return -1;
			}
		}

		//ͳ��ʧ�ܴ���
#ifdef DEBUG_ERROR
		ES_ERROR_INFO es_err;
		es_err.statis_type = STATIS_DMS;
		es_err.es_time.year = 2014;
		es_err.es_time.month = 6;
		es_err.es_time.day = 14;
		mul_type_err.insert(make_pair(STATIS_DAY, es_err));

		es_err.statis_type = STATIS_DMS;
		es_err.es_time.year = 2014;
		es_err.es_time.month = 6;
		es_err.es_time.day = 14;
		num = mul_type_err.count(STATIS_DAY);
		itr_type_err = mul_type_err.find(STATIS_DAY);
		for (int i = 0; i < num; i++)
		{
			if(itr_type_err->second == es_err)
			{
				printf("err_map: error, repeart\n");
			}
			itr_type_err++;
		}

		es_err.statis_type = STATIS_DMS;
		es_err.es_time.year = 2014;
		es_err.es_time.month = 7;
		es_err.es_time.day = 1;
		mul_type_err.insert(make_pair(STATIS_MONTH, es_err));
#endif

		//��ͳ�ƴ���
		num = mul_type_err.count(STATIS_DAY);
		if(num > 0)
		{
			itr_type_err = mul_type_err.find(STATIS_DAY);
			for (int i = 0; i < num; i++)
			{
				int flag = 0;
				int statis_type = itr_type_err->second.statis_type;
				switch(statis_type)
				{
				case STATIS_DMS:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//��վ�������
						StatisDMS s_dms(&mul_type_err);
						ret = s_dms.GetDMSRunningRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_MODEL:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//��վģ������
						StatisModel s_model(&mul_type_err);
						ret = s_model.getHHStatus(&es_time);
						if (ret < 0)
						{
							flag = 1;
						}
						ret = s_model.DeleteHH(&es_time);
						if (ret < 0)
						{
							flag = 1;
						}
						ret = s_model.GetModelInfo(&itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						ret = s_model.GetDMSModelRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						ret = s_model.GetModelProcess(&itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_DA:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//DAִ�����
						StatisDA s_da(&mul_type_err);
						ret = s_da.GetDARunningRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						ret = s_da.GetDAProcess(&itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_DGPMS:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//DMS��GPMS��Ϣ�������
						StatisDGPMS s_dgp(&mul_type_err);
						ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						ret = s_dgp.GetDGPMSProcess(&itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_YC:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//DMS���ò���Ϣ�������
						StatisYC s_yc(&mul_type_err);
						ret = s_yc.GetYCRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						ret = s_yc.GetYCProcess(&itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_TERMINAL:
					{
						//������ͳ�Ʊ�
						CreateStatisTable(&itr_type_err->second.es_time);

						//�ն��������
						StatisTerminal s_tu(&mul_type_err);
						ret = s_tu.GetTerminalRunningRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				}

				//ɾ���ձ�����
				DeleteStatisData(STATIS_DAY, &itr_type_err->second.es_time);

				/*ESScore s_score;
				ret = s_score.InsertPointScore(STATIS_DAY, &itr_type_err->second.es_time);
				ret = s_score.InsertScore(STATIS_DAY, &itr_type_err->second.es_time);

				DeleteStatisData(STATIS_DAY, &itr_type_err->second.es_time);*///ɾ��'��ֵ'�ظ�����

				if(flag == 0)
					mul_type_err.erase(itr_type_err);

				itr_type_err++;
			}
		}

		//��ͳ�ƴ���
		num = mul_type_err.count(STATIS_MONTH);
		if(num > 0)
		{
			itr_type_err = mul_type_err.find(STATIS_MONTH);
			for (int i = 0; i < num; i++)
			{
				int flag = 0;
				//��վ�������
				StatisDMS s_dms(&mul_type_err);				
				ret = s_dms.GetDMSRunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//��վģ������, ģ��Ҫ�ŵ�DA��GPMSͳ��֮ǰ
				StatisModel s_model(&mul_type_err);
				ret = s_model.GetDMSModelRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DAִ�����
				StatisDA s_da(&mul_type_err);
				ret = s_da.GetDARunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DMS��GPMS��Ϣ�������
				StatisDGPMS s_dgp(&mul_type_err);
				ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DMS���ò���Ϣ�������
				StatisYC s_yc(&mul_type_err);
				ret = s_yc.GetYCRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//�ն��������
				StatisTerminal s_tu(&mul_type_err);
				ret = s_tu.GetTerminalRunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//ɾ���±�����
				DeleteStatisData(STATIS_MONTH, &itr_type_err->second.es_time);

				ESScore s_score;
				ret = s_score.InsertPointScore(STATIS_MONTH, &itr_type_err->second.es_time);
				ret = s_score.InsertScore(STATIS_MONTH, &itr_type_err->second.es_time);

				DeleteStatisData(STATIS_MONTH, &itr_type_err->second.es_time);//ɾ��'��ֵ'�ظ�����

				if(flag == 0)
					mul_type_err.erase(itr_type_err);

				itr_type_err++;
			}
		}
		
		//���ߵ�����Сʱ
		GetCurTime(&hour, &min, &sec);
		printf("hour:%d,min:%d,sec:%d\n",hour,min,sec);
		if(min < 30)
			sleep(30 * 60 - 60 * min - sec); 
		else
			sleep(30 * 60 - 60 * (min - 30) - sec); 
	}
#endif
	return 0;
}

//������/��ͳ�Ʊ�
int CreateStatisTable(ES_TIME *es_time)
{
	int ret;
	int year, mon, day, days;
	char table_name[MAX_NAME_LEN];
	char table_exist[MAX_SQL_LEN];
	char sql[MAX_SQL_LEN];
	ErrorInfo err_info;

	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;
	days = GetDaysInMonth(year, mon);

#ifdef DEBUG_DATE
	year = 2014;
	day = 13;
	mon = 6;
	days = 30;
#endif
	
	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}
		
	//��ͳ�Ʊ�
	for(int i = 1; i <= days; i++)
	{
		//�жϱ��Ƿ����
		sprintf(table_name, "day_%04d%02d%02d", year, mon ,i);
		sprintf(table_exist, "select * from evalusystem.result.%s where 1!=1" ,table_name);
		if(m_oci->ExecNoBind(table_exist,&err_info))
			continue;

		sprintf(sql, "create table evalusystem.result.%s (code varchar(20),organ_code INT,value FLOAT,datatype varchar(10),"
			"updatetime timestamp,statistime timestamp,dimname varchar(10),dimvalue varchar(50),pos INT IDENTITY(1, 1),FLAG INT DEFAULT 0,"
			"PRIMARY KEY(POS))",table_name);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateTable: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
			return -1;
		}

		//��������
		sprintf(sql, "CREATE INDEX INDEX_%04d%02d%02d ON EVALUSYSTEM.RESULT.DAY_%04d%02d%02d"
			"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, i, year, mon, i);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateIndex: sql= %s \t error(%d) = %s;\n", sql,err_info.error_no,err_info.error_info);
			return -1;
		}

		//������ָ���ʼ������
		sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,"
			"statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'�ٷֱ�',"
			"to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'��','��' "
			"from (select code from evalusystem.config.info where sequence!=-1 and code!='dmsaveop') a,"
			"(select organ_code from evalusystem.config.organ) b union all select a.code,b.organ_code,0,"
			"'�ٷֱ�',to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'��',"
			"'��' from (select code from evalusystem.config.info where sequence!=-1 and code='dmsaveop') a,"
			"(select organ_code from evalusystem.config.organ) b", 
			table_name, year, mon, i, year, mon, i, year, mon, i, year, mon, i);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("DayInit: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
			return -1;
		}
	}

	//��ͳ�Ʊ�
	//�жϱ��Ƿ����
	sprintf(table_name, "month_%04d%02d", year, mon);
	sprintf(table_exist, "select * from evalusystem.result.%s where 1!=1" ,table_name);
	if(!m_oci->ExecNoBind(table_exist,&err_info))
	{
		sprintf(sql, "create table evalusystem.result.%s (code varchar(20),organ_code INT,value FLOAT,datatype varchar(10),"
			"updatetime timestamp,statistime timestamp,dimname varchar(10),dimvalue varchar(50),pos INT IDENTITY(1, 1),FLAG INT DEFAULT 0,"
			"PRIMARY KEY(POS))",table_name);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateTable: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
			return -1;
		}

		//��������
		sprintf(sql, "CREATE INDEX INDEX_%04d%02d ON EVALUSYSTEM.RESULT.MONTH_%04d%02d"
			"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, year, mon);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			return -1;
		}

		//������ָ���ʼ������
		sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,"
			"updatetime,statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'�ٷֱ�',"
			"to_date('%04d-%02d-01','YYYY-MM-DD'),to_date('%04d-%02d-01','YYYY-MM-DD'),'��','��' "
			"from (select code from evalusystem.config.info where sequence!=-1) a,"
			"(select organ_code from evalusystem.config.organ) b", table_name, year, mon, year, mon);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("MonthInit: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
			return -1;
		}
	}

	if(day == 1)
	{
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

		//�����һ������Ϊ��һ�죬������һ����ͳ�Ʊ�
		sprintf(table_name, "month_%04d%02d", year, mon);
		sprintf(table_exist, "select * from evalusystem.result.%s where 1!=1" ,table_name);
		if(!m_oci->ExecNoBind(table_exist,&err_info))
		{
			sprintf(sql, "create table evalusystem.result.%s (code varchar(20),organ_code INT,value FLOAT,"
				"datatype varchar(10),updatetime timestamp,statistime timestamp,dimname varchar(10),"
				"dimvalue varchar(50),pos INT IDENTITY(1, 1),FLAG INT DEFAULT 0,PRIMARY KEY(POS))",table_name);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateTable: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
				return -1;
			}

			//��������
			sprintf(sql, "CREATE INDEX INDEX_%04d%02d ON EVALUSYSTEM.RESULT.MONTH_%04d%02d"
				"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, year, mon);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
				return -1;
			}

			//������ָ���ʼ������
			sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,"
				"updatetime,statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'�ٷֱ�',"
				"to_date('%04d-%02d-01','YYYY-MM-DD'),to_date('%04d-%02d-01','YYYY-MM-DD'),'��','��' "
				"from (select code from evalusystem.config.info where sequence!=-1) a,"
				"(select organ_code from evalusystem.config.organ) b", table_name, year, mon, year, mon);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("MonthInit: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
				return -1;
			}
		}

		sprintf(table_name, "day_%04d%02d%02d", year, mon ,day);
		sprintf(table_exist, "select * from evalusystem.result.%s where 1!=1" ,table_name);
		if(!m_oci->ExecNoBind(table_exist,&err_info))
		{
			sprintf(sql, "create table evalusystem.result.%s (code varchar(20),organ_code INT,value FLOAT,"
				"datatype varchar(10),updatetime timestamp,statistime timestamp,dimname varchar(10),"
				"dimvalue varchar(50),pos INT IDENTITY(1, 1),FLAG INT DEFAULT 0,PRIMARY KEY(POS))",table_name);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateTable: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
				return -1;
			}

			//��������
			sprintf(sql, "CREATE INDEX INDEX_%04d%02d%02d ON EVALUSYSTEM.RESULT.DAY_%04d%02d%02d"
				"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, day, year, mon, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
				return -1;
			}

			//������ָ���ʼ������
			sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,"
				"statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'�ٷֱ�',"
				"to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'��','��' "
				"from (select code from evalusystem.config.info where sequence!=-1 and code!='dmsaveop') a,"
				"(select organ_code from evalusystem.config.organ) b union all select a.code,b.organ_code,0,"
				"'�ٷֱ�',to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'��',"
				"'��' from (select code from evalusystem.config.info where sequence!=-1 and code='dmsaveop') a,"
				"(select organ_code from evalusystem.config.organ) b", 
				table_name, year, mon, day, year, mon, day, year, mon, day, year, mon, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("DayInit: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
				return -1;
			}
		}
	}	

	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}

//����ͳ�Ƶ���ͳ�Ʊ�
//int CheckStatisTable(ES_TIME *es_time)
//{
//	int ret, num;
//	int year, mon, day;
//	char table_name[MAX_NAME_LEN];
//	char sql[MAX_SQL_LEN];
//	ErrorInfo err_info;
//	int rec_num;
//	int col_num;
//	char *buffer = NULL;
//	char *m_presult = NULL;
//	struct ColAttr *col_attr = NULL;
//
//	if(es_time->month == 1 && es_time->day == 1)
//	{
//		year = es_time->year - 1;
//		mon = 12;
//		day = 31;
//	}
//	else if(es_time->month != 1 && es_time->day == 1)
//	{
//		year = es_time->year;
//		mon = es_time->month - 1;
//		day = GetDaysInMonth(year, mon);
//	}
//	else
//	{
//		year = es_time->year;
//		mon = es_time->month;
//		day = es_time->day - 1;
//	}
//
//#ifdef DEBUG_DATE
//	year = 2014;
//	day = 13;
//	mon = 6;
//#endif
//
//	CDci  *m_oci = new CDci;
//	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
//	if(!ret)
//	{
//		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
//		return -1;
//	}
//
//	for(int i = 1; i <= day; i++)
//	{
//		sprintf(table_name, "day_%04d%02d%02d"��year, mon, day);
//		sprintf(sql, "select 1 from evalusystem.result.%s where rownum < 2", table_name);
//		ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
//		if(ret == 1)
//		{			
//			if(rec_num > 0)
//			{
//				v_talbe_name.insert(table_name);
//			}
//			else
//			{
//				ES_ERROR_INFO es_err;
//				es_err.statis_type = STATIS_DAY;
//				es_err.es_time.year = es_time->year;
//				es_err.es_time.month = es_time->month;
//				es_err.es_time.day = es_time->day;
//#ifdef DEBUG_DATE
//				es_err.es_time.year = 2014;
//				es_err.es_time.month = 6;
//				es_err.es_time.day = 14;
//#endif
//				int flag = 0;
//				num = mul_type_err->count(STATIS_DAY);
//				multimap<int, ES_ERROR_INFO> itr_type_err = mul_type_err->find(STATIS_DAY);
//				for (int i = 0; i < num; i++)
//				{
//					if(itr_type_err->second == es_err)
//					{
//						flag = 1;
//						break;
//					}
//
//					itr_type_err++;
//				}
//				if(flag == 0)
//					mul_type_err->insert(make_pair(STATIS_DAY, es_err));
//			}
//		}
//		else
//		{
//			ES_ERROR_INFO es_err;
//			es_err.statis_type = STATIS_DAY;
//			es_err.es_time.year = es_time->year;
//			es_err.es_time.month = es_time->month;
//			es_err.es_time.day = es_time->day;
//#ifdef DEBUG_DATE
//			es_err.es_time.year = 2014;
//			es_err.es_time.month = 6;
//			es_err.es_time.day = 14;
//#endif
//			int flag = 0;
//			num = mul_type_err->count(STATIS_DAY);
//			multimap<int, ES_ERROR_INFO> itr_type_err = mul_type_err->find(STATIS_DAY);
//			for (int i = 0; i < num; i++)
//			{
//				if(itr_type_err->second == es_err)
//				{
//					flag = 1;
//					break;
//				}
//
//				itr_type_err++;
//			}
//			if(flag == 0)
//				mul_type_err->insert(make_pair(STATIS_DAY, es_err));
//			
//			continue;
//		}
//
//		if(rec_num > 0)
//			m_oci->FreeReadData(col_attr, col_num, buffer);
//		else
//			m_oci->FreeColAttrData(col_attr, col_num);
//	}
//
//	ret = m_oci->DisConnect(&err_info);
//	if(!ret)
//	{
//		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
//		return -1;
//	}
//
//	return 0;
//}

//ɾ��ͳ�Ʊ�����
int DeleteStatisData(int statis_type, ES_TIME *es_time)
{
	int ret;
	int year, mon, day, days;
	char sql_delete[MAX_SQL_LEN];
	ErrorInfo err_info;

	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;
	days = GetDaysInMonth(year, mon);

#ifdef DEBUG_DATE
	year = 2014;
	day = 13;
	mon = 6;
	days = 30;
#endif

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

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}	
	
	//ɾ���ظ�����
	if(statis_type == STATIS_MONTH)
	{
		sprintf(sql_delete, "delete from evalusystem.result.month_%04d%02d a where updatetime!=(select min(updatetime) "
			"from evalusystem.result.month_%04d%02d b where a.code=b.code and a.organ_code=b.organ_code and "
			" a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue and a.value = b.value)",
			year, mon, year, mon);
	}
	else
	{
		sprintf(sql_delete, "delete from evalusystem.result.day_%04d%02d%02d a where updatetime!=(select min(updatetime) "
			"from evalusystem.result.day_%04d%02d%02d b where a.code=b.code and a.organ_code=b.organ_code and "
			" a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue and a.value = b.value)",
			year, mon, day, year, mon, day);
	}
	/*if(statis_type == STATIS_MONTH)
	{
		sprintf(sql_delete, "delete from evalusystem.result.month_%04d%02d a where rowid!=(select min(rowid) "
			"from evalusystem.result.month_%04d%02d b where a.code=b.code and a.organ_code=b.organ_code and "
			"a.value=b.value and a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue)",
			year, mon, year, mon);
	}
	else
	{
		sprintf(sql_delete, "delete from evalusystem.result.day_%04d%02d%02d a where rowid!=(select min(rowid) "
			"from evalusystem.result.day_%04d%02d%02d b where a.code=b.code and a.organ_code=b.organ_code and "
			"a.value=b.value and a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue)",
			year, mon, day, year, mon, day);
	}*/
// 	sprintf(sql_delete, "delete from evalusystem.result.day_20140630 a where rowid!=(select min(rowid) from "
// 		"evalusystem.result.day_20140630 b where a.code=b.code and a.organ_code=b.organ_code and "
// 		"a.value=b.value and a.datatype=b.datatype "
// 		"and a.dimname=b.dimname and a.dimvalue=b.dimvalue)");
	ret = m_oci->ExecSingle(sql_delete,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql_delete);
		return -1;
	}
	
	//ɾ������������
	if(statis_type == STATIS_MONTH)
	{
// 		sprintf(sql_delete, "delete from evalusystem.result.month_%04d%02d a where not exists "
// 			"(select * from (select code,max(statistime) statistime from evalusystem.result.month_%04d%02d "
// 			"group by code) b where a.code=b.code and a.statistime=b.statistime)", year, mon, year, mon);
		sprintf(sql_delete, "delete from evalusystem.result.month_%04d%02d a where updatetime!="
			"(select max(updatetime) from evalusystem.result.month_%04d%02d b where a.code=b.code and "
			"a.organ_code=b.organ_code and a.dimvalue=b.dimvalue and a.datatype = b.datatype);", year, mon, year, mon);
	}
	else
	{
// 		sprintf(sql_delete, "select * from evalusystem.result.day_%04d%02d%02d a where not exists "
// 			"(select * from (select code,max(updatetime) updatetime from evalusystem.result.day_%04d%02d%02d "
// 			"group by code) b where a.code=b.code and a.updatetime=b.updatetime);", year, mon, day, year, mon, day);
		sprintf(sql_delete, "delete from evalusystem.result.day_%04d%02d%02d a where updatetime!="
			"(select max(updatetime) from evalusystem.result.day_%04d%02d%02d b where a.code=b.code and "
			"a.organ_code=b.organ_code and a.dimvalue=b.dimvalue and a.datatype = b.datatype);", year, mon, day, year, mon, day);
	}

// 	sprintf(sql_delete, "delete from evalusystem.result.day_20140630 a where not exists "
// 		"(select * from (select code,max(statistime) statistime from evalusystem.result.day_20140630 "
// 		"group by code) b where a.code=b.code and a.statistime=b.statistime)");
	ret = m_oci->ExecSingle(sql_delete,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql_delete);
		return -1;
	}
	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}

//��ʼ������ͳ������
int InitFirstStatis(const string &begin_time, const string &end_time)
{
	struct tm timevalue;
	struct tm tmp_tm;
	int year, month, day;
	int year2, month2, day2;
	int ret;
	time_t n_time, tmptime;
	ES_TIME es_time;
	
	ret = CheckTimeString(begin_time);
	if(ret < 0)
		return -1;
	if(end_time != "")
	{
		ret = CheckTimeString(end_time);
		if(ret < 0)
			return -1;
	}

	year = atoi(string(begin_time.begin(), begin_time.begin()+4).c_str());
	month = atoi(string(begin_time.begin()+4, begin_time.begin()+6).c_str());
	day = atoi(string(begin_time.begin()+6, begin_time.begin()+8).c_str());

	timevalue.tm_year = year - 1900;
	timevalue.tm_mon = month - 1;
	timevalue.tm_mday = day;
	timevalue.tm_hour = 0;
	timevalue.tm_min = 0;
	timevalue.tm_sec = 0;
	tmptime = mktime(&timevalue) + 86400;
	//cout<<"tmptime = "<<tmptime<<endl;

	GetCurDate(&year, &month, &day);
	timevalue.tm_year = year - 1900;
	timevalue.tm_mon = month - 1;
	timevalue.tm_mday = day;
	timevalue.tm_hour = 0;
	timevalue.tm_min = 0;
	timevalue.tm_sec = 0;
	n_time = mktime(&timevalue);

	if(end_time != "")
	{
		year2 = atoi(string(end_time.begin(), end_time.begin()+4).c_str());
		month2 = atoi(string(end_time.begin()+4, end_time.begin()+6).c_str());
		day2 = atoi(string(end_time.begin()+6, end_time.begin()+8).c_str());
		
		timevalue.tm_year = year2 - 1900;
		timevalue.tm_mon = month2 - 1;
		timevalue.tm_mday = day2;
		timevalue.tm_hour = 0;
		timevalue.tm_min = 0;
		timevalue.tm_sec = 0;
		n_time = mktime(&timevalue);
		n_time += 86400;
	}

#ifdef DEBUG_DATE
	n_time = 1402675200;
#endif
	while(tmptime <= n_time)
	{
		tmp_tm = *localtime(&tmptime);
		es_time.year = tmp_tm.tm_year + 1900;
		es_time.month = tmp_tm.tm_mon + 1;
		es_time.day = tmp_tm.tm_mday;
		
		//cout<<es_time.year<<" "<<es_time.month<<" "<<es_time.day<<endl;
		//cout<<"tmptime = "<<tmptime<<endl;
		//cout<<"n_time = "<<n_time<<endl;
		if(CheckCorrective(&es_time))	//������ͳ��
		{
			StatisSupport s_support;
			s_support.GetCorrective(&es_time);
		}

		StatisAnalyze(&es_time, 1);


		tmptime += 86400;
	}

	return 0;
}

//ͳ��
int StatisAnalyze(ES_TIME *es_time, int cooperation)
{
	int ret;
#define TEST_DMS 1
#define TEST_MODEL 1
#define TEST_FA 1
#define TEST_DGPMS 1
#define TEST_YC 1
#define TEST_TER 1
#define TEST_ZLP 1

	//������/��ͳ�Ʊ�
	ret = CreateStatisTable(es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "������/��ͳ�Ʊ�ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#if 1
#if TEST_DMS
	//��ͳ��
	//��վ�������
	printf("��ͳ��\n");
	printf("��վ�������ͳ��\n");
	StatisDMS s_dms(&mul_type_err);
	ret = s_dms.GetDMSRunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վ���������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_MODEL
	//��վģ������, ģ��Ҫ�ŵ�DA��GPMSͳ��֮ǰ
	printf("��վģ������ͳ��\n");
	StatisModel s_model(&mul_type_err);
	if(cooperation == 1)
	{
		//ģ��У��
		ret = s_model.ModelCheck(es_time);
		if(ret < 0)
		{
			// 		plog->WriteLog(2, "��ȡģ����Ϣʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
			// 			es_time->year, es_time->month, es_time->day);
		}
	}

	ret = s_model.getHHStatus(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "��ȡģ����Ϣʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.DeleteHH(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "��ȡģ����Ϣʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetModelInfo(es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "��ȡģ����Ϣʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetDMSModelRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վģ����ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetModelProcess(es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վģ�����ݴ���ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_FA
	//DAִ�����
	printf("DAִ�����ͳ��\n");
	StatisDA s_da(&mul_type_err);
	ret = s_da.GetDARunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����Զ���ִ�������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_da.GetDAProcess(es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����Զ������ݴ���ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_DGPMS
	//DMS��GPMS��Ϣ�������
	printf("DMS��GPMS��Ϣ�������ͳ��\n");
	StatisDGPMS s_dgp(&mul_type_err);
	ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վ��GPMS��Ϣ������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_dgp.GetDGPMSProcess(es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վ��GPMS��Ϣ�������ݴ���ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_YC
	//DMS���ò���Ϣ�������
	printf("DMS���ò���Ϣ�������ͳ��\n");
	StatisYC s_yc(&mul_type_err);
	ret = s_yc.GetYCRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "�����վ���ò���Ϣ������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_yc.GetYCProcess(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "�����վ���ò���Ϣ�������ݴ���ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_TER
	//�ն��������
	printf("�ն��������ͳ��\n");
	StatisTerminal s_tu(&mul_type_err);
	ret = s_tu.GetTerminalRunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "����ն����������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_ZLP
	//ָ��Ʊ���
	printf("ָ��Ʊ���ͳ��\n");
	StatisZLP s_zlp;
	ret = s_zlp.GetZLPRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
	}
#endif

	//ɾ���ձ�����
	printf("�ձ�ȥ��\n");
	DeleteStatisData(STATIS_DAY, es_time);

	ESScore s_score;
	ret = s_score.InsertPointScore(STATIS_DAY, es_time);
	ret = s_score.InsertScore(STATIS_DAY, es_time);
	DeleteStatisData(STATIS_DAY, es_time);//ɾ��'��ֵ'�ظ�����
	//��ͳ��
#if TEST_DMS
	//��վ�������
	printf("��ͳ��\n");
	printf("��վ�������ͳ��\n");
	ret = s_dms.GetDMSRunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վ���������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_MODEL
	//��վģ������
	printf("��վģ������ͳ��\n");
	ret = s_model.GetDMSModelRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վģ����ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_FA
	//DAִ�����
	printf("DAִ�����ͳ��\n");
	ret = s_da.GetDARunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����Զ���ִ�������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_DGPMS
	//DMS��GPMS��Ϣ�������
	printf("DMS��GPMS��Ϣ�������ͳ��\n");
	ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "�����վ��GPMS��Ϣ������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_YC
	//DMS���ò���Ϣ�������
	printf("DMS���ò���Ϣ�������ͳ��\n");
	ret = s_yc.GetYCRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "�����վ���ò���Ϣ������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_TER
	//�ն��������
	printf("�ն��������ͳ��\n");
	ret = s_tu.GetTerminalRunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
// 		plog->WriteLog(2, "����ն����������ͳ��ʧ�ܣ���ͳ������Ϊ��%04d-%02d-%02d",
// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_ZLP
	//ָ��Ʊ���
	printf("ָ��Ʊ���ͳ��\n");
	ret = s_zlp.GetZLPRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
	}
#endif
	//ɾ���±�����
	printf("�±�ȥ��\n");
	DeleteStatisData(STATIS_MONTH, es_time);
	
	ret = s_score.InsertPointScore(STATIS_MONTH, es_time);
	ret = s_score.InsertScore(STATIS_MONTH, es_time);

	DeleteStatisData(STATIS_MONTH, es_time);//ɾ��'��ֵ'�ظ�����
#endif

	return 0;
}

//У��ʱ���ʽ
int CheckTimeString(const string &check_time)
{
	//20140612
	int year, month, day;

	//����У��
	if (check_time.size() != 8)
	{
		cout << "charactor size error! size =  " << check_time.size() << endl;
		return -1;
	}

	//�ַ�У��
	for(int i = 0; i < check_time.size(); i++)
	{
		//cout<<begin_time.at(i)<<endl;
		char ch = check_time.at(i);
		if(ch < '0' || ch >'9')
		{
			cout<<"charactor type error! ch = "<<ch<<endl;
			return -1;
		}
	}

	year = atoi(string(check_time.begin(), check_time.begin()+4).c_str());
	month = atoi(string(check_time.begin()+4, check_time.begin()+6).c_str());
	day = atoi(string(check_time.begin()+6, check_time.begin()+8).c_str());

	//����У��
	if(year < 1970 || year > 9999 || month < 1 || month > 12)
	{
		cout<<"date error! year = "<<year<<", month = "<<month<<", day = "<<day<<endl;
		return -1;
	}

	if(month == 1 || month == 3 || month == 5 || month == 7 || month == 8 || month == 10 || month == 12)
	{
		if(day < 1 || day > 31)
		{
			cout<<"date error! year = "<<year<<", month = "<<month<<", day = "<<day<<endl;
			return -1;
		}
	}
	else if(month == 4 || month == 6 || month == 9 || month == 11)
	{
		if(day < 1 || day > 30)
		{
			cout<<"date error! year = "<<year<<", month = "<<month<<", day = "<<day<<endl;
			return -1;
		}
	}
	else
	{
		if(day != GetDaysInMonth(year, month))
		{
			cout<<"date error! year = "<<year<<", month = "<<month<<", day = "<<day<<endl;
			return -1;
		}
	}
	
	return 0;
}

void Help()
{
	cout<<"Usage: evalu_review [begin_time] [end_time]"<<endl
		<<"\t1) begin_time: ��ʼʱ�䣬����ӿ�ʼʱ��ͳ�Ƶ����죬Ȼ��ʼ�����������̣�"<<endl
		<<"\t2) end_time: ����ʱ�䣬����ӿ�ʼʱ��ͳ�Ƶ�����ʱ�䣬Ȼ���˳���"<<endl;
}

int GetCleanPlan(vector<int> & vec_int)
{
	int ret;
	ErrorInfo err_info;

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code from evalusystem.config.datatrunc where weekofday = %d;",WhichDayInWeek(time(NULL)));
	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					vec_int.push_back(*(int*)tempstr);
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

	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}

//���dms��gpms������
int TruncateData()
{
	vector<int> vec_plan;
	vec_plan.clear();

	int ret;
	char sql_delete[MAX_SQL_LEN];
	ErrorInfo err_info;

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}	

	//��ȡGPMSȫ����ͳ�ƻ�
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code from evalusystem.config.datatrunc where weekofday = %d;",WhichDayInWeek(time(NULL)));
	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					vec_plan.push_back(*(int*)tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}
		}
	}
	
	vector<int>::iterator it_plan = vec_plan.begin();
	for (;it_plan != vec_plan.end();it_plan++)
	{
		sprintf(sql_delete, "call evalusystem.detail.truncatedata(%d)",*it_plan);
		ret = m_oci->ExecSingle(sql_delete,&err_info);
		if(!ret)
		{
			printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql_delete);
			break;
		}

	}
	
	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}

//��ȡ��������
int GetStartConfig(int *model, int *statis, int *cooperation)
{
	int ret;
	ErrorInfo err_info;

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select * from evalusystem.config.startup");
	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					*model = *(int *)(tempstr);
					break;
				case 1:
					*statis = *(int *)(tempstr);
					break;
				case 2:
					*cooperation = *(int *)(tempstr);
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
		*model = START_TIME;
		*statis = START_TIME;
		*cooperation = 1;	//Ĭ��ģ��У�������ͳ�Ƴ�������
		m_oci->FreeColAttrData(col_attr, col_num);
	}

	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}

//������ͳ��ʱ���ж�
bool CheckCorrective(ES_TIME *es_time)
{
	struct tm time_tm;
	time_tm.tm_year = es_time->year - 1900;
	time_tm.tm_mon = es_time->month - 1;
	time_tm.tm_mday = es_time->day;
	time_tm.tm_hour = 0;
	time_tm.tm_min = 0;
	time_tm.tm_sec = 0;

	time_t tmp_time = mktime(&time_tm);
	struct tm tmp_tm;
	localtime_r(&tmp_time, &tmp_tm);

	int month_num = (tmp_tm.tm_yday - tmp_tm.tm_wday) / 7 + 1;
	if(month_num % 2 != 0 && tmp_tm.tm_wday == 0)	//�ж��Ƿ�������
		return true;

	return false;
}

void* threadlogwrite(void *param)
{
	const int buf_size = 1024 * 1024; //1M
	int pipefd[2];
	vector<char> 	vBuf;
	tm last = { 0 };
	FILE *fp = NULL;
	char szLogPath[260];

	sprintf(szLogPath, "/home/d5000/fujian/bin/log");

	struct stat st;
	if (stat(szLogPath, &st) == -1)
	{
		if (mkdir(szLogPath, S_IRWXU | S_IRWXG | S_IRWXO) != 0)
		{
			exit(0);
		}
	}

	cout << "ע�⣺��Ļ�����д����־�ļ�������cat����鿴" << szLogPath << "Ŀ¼����Ӧ�ļ���" << endl;

	pipe(pipefd);

	//�������������Ϊ�л���ģʽ
	setvbuf(stdout, NULL, _IOLBF, BUFSIZ);
	setvbuf(stderr, NULL, _IOLBF, BUFSIZ);

	dup2(pipefd[1], STDOUT_FILENO);
	dup2(pipefd[1], STDERR_FILENO);

	while (true)
	{
		char ch = 0;
		int ret = read(pipefd[0], &ch, 1);
		vBuf.push_back(ch);

		if (ch == '\n')
		{
			time_t now = time(NULL);
			tm *ptm = localtime(&now);

			//������µ�һ�죬�����½����µ���־�ļ�
			if (ptm->tm_mday != last.tm_mday)
			{
				memcpy(&last, ptm, sizeof(tm));

				if (fp != NULL)
				{
					fclose(fp);
				}

				char szFile[260] = { 0 };

				sprintf(szFile, "%s/evalu_review_%04d%02d%02d.log", szLogPath, ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday);

				fp = fopen(szFile, "a+");

				if (fp == NULL)
				{
					exit(0);
				}
			}

			char szTime[50] = { 0 };

			sprintf(szTime, "[%04d/%02d/%02d %02d:%02d:%02d]:", ptm->tm_year + 1900, ptm->tm_mon + 1, ptm->tm_mday, ptm->tm_hour, ptm->tm_min, ptm->tm_sec);
			fwrite(szTime, 1, strlen(szTime), fp);

			fwrite(&vBuf.front(), 1, vBuf.size(), fp);
			fflush(fp);

			vBuf.clear();
		}
	}

	return NULL;
}

//����У���ȥ�� add by lcm 20150915
int Data_Check()
{
	int ret;
	ErrorInfo err_info;
	char sql[200];
	memset(sql,0,sizeof(sql));
	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}
	sprintf(sql, "call evalusystem.manage.CHECK_ERR_FILE");

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
	memset(sql,0,sizeof(sql));
	sprintf(sql, "call evalusystem.result.DATAREC_CHECK");

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}
	
	ret = m_oci->DisConnect(&err_info);
	if(!ret)
	{
		printf("DisConnect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}

	return 0;
}



