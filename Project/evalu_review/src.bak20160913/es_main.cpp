/*******************************************************************************
* Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
* File    : es_main.cpp
* Author  : guoqiuye
* Version : v1.0
* Function : 统计主程序
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
#include "statis_xsd.h"
#include "statis_ztgj.h"
#include "statis_yd.h"
#include "statis_line.h"
#include "statis_district.h"
using namespace std;

#define START_TIME 6	//默认启动时间
#define TEST 0

//统计失败map
multimap<int, ES_ERROR_INFO> mul_type_err;

//可统计的日统计表
//vector<string> v_talbe_name;

//建立日/月统计表
int CreateStatisTable(ES_TIME *es_time);

//检查可统计的日统计表
//int CheckStatisTable(ES_TIME *es_time);

//删除统计表数据
int DeleteStatisData(int statis_type, ES_TIME *es_time);

//计算指标值全市平均值及分子分母全市总加值
int CaculateAvgAll(int statis_type, ES_TIME *es_time);

int CaculateScoreAverCity(int statis_type, ES_TIME *es_time);

//统计首页数据
int CaculateIndexDate( ES_TIME *es_time);

//初始化初次统计
int InitFirstStatis(const string &begin_time, const string &end_time);

//统计
int StatisAnalyze(ES_TIME *es_time, int cooperation);

//校验时间格式
int CheckTimeString(const string &check_time);

//输出日志
void* threadlogwrite(void *param);

void* network_check();

//数据校验表去重 add by lcm 20150915
int Data_Check();

//帮助
void Help();

//清空dms和gpms表数据
int TruncateData();

//获取启动配置
int GetStartConfig(int *model, int *statis, int *cooperation);

//整改率统计时间判断
bool CheckCorrective(ES_TIME *es_time);

int main(int argc, char **argv)
{
	int num, ret;
	int break_flag = 0;
	multimap<int, ES_ERROR_INFO>::iterator itr_type_err;

	//创建写日志线程
	pthread_t pt;
	pthread_attr_t attr;

	pthread_attr_init(&attr);

	if (pthread_create(&pt, &attr, threadlogwrite, NULL) != 0)
	{
		printf("创建写日志线程失败！\r\n");
		return -1;
	}
	pthread_detach(pt);

	//初始化初次统计
	if(argc == 2)
	{
		ret = InitFirstStatis(argv[1], "");
		if(ret < 0)
		{
			cout<<"日期格式错误！"<<endl;
			return -1;
		}
	}
	else if(argc == 3)
	{
		break_flag = 1;

		ret = InitFirstStatis(argv[1], argv[2]);
		if(ret < 0)
		{
			cout<<"日期格式错误！"<<endl;
			return -1;
		}
	}
	else if(argc > 3)
	{
		Help();
		return -1;
	}
#if TEST
	ES_TIME es_time;
	es_time.year = 2016;
	es_time.month = 03;
	es_time.day = 01;
	printf("异动单情况统计\n");
	StatisYD s_yd;
	ret = s_yd.GetYDDRate(STATIS_DAY, &es_time);
#endif

#if 1
	for( ; ; )
	{
		if(break_flag == 1)
			break;

		int year,month,day,hour,min,sec; 
		int weekday;

		//获取当前时间
		GetCurTime(&hour, &min, &sec);
		GetCurDate(&year, &month, &day);

		weekday = WhichDayInWeek(time(NULL));
		ES_TIME es_time;
		es_time.year = year;
		es_time.month = month;
		es_time.day = day;
		printf("hour:%d,min:%d,sec:%d\n",hour,min,sec);
		//获取启动配置
		int model, statis, cooperation;
		GetStartConfig(&model, &statis, &cooperation);
		//cout<<model<<"\t"<<statis<<"\t"<<cooperation<<endl;

		if(0 == hour && 0 == min)
		{
			TruncateData();
		}

		if(0 == min || 30 == min)
		{
			//创建写网络状态监测线程
			pthread_t p_check;

			if (pthread_create(&p_check, NULL, (void*(*)(void*))network_check, NULL) != 0)
			{
				printf("创建写检测网络状态线程失败！\r\n");
				return -1;
			}
			pthread_detach(p_check);
			//net_check_count++;
		}

		if(cooperation == 0 && model == hour && min == 0)//mode = 6
		{
			StatisModel s_model(&mul_type_err);
			ret = s_model.ModelCheck(&es_time);
			GetCurTime(&hour, &min, &sec);
			if(statis < hour)//4 statis = 7
			{
				if(CheckCorrective(&es_time))	//整改率统计
				{
					StatisSupport s_support;
					s_support.GetCorrective(&es_time, 0);
				}
				else {

					StatisSupport s_support;
					s_support.GetCorrective(&es_time,1);
				}

				ret = StatisAnalyze(&es_time, cooperation);

			}
		}

		if(statis == hour && min == 0)//4 statis=7
		{
			if(CheckCorrective(&es_time))	//整改率统计
			{
				StatisSupport s_support;
				s_support.GetCorrective(&es_time, 0);
			}
			else {

				StatisSupport s_support;
				s_support.GetCorrective(&es_time,1);
			}

			ret = StatisAnalyze(&es_time, cooperation);

		}
		/*if(hour == 8 && min == 0)
		{
			printf("小水电8点统计实际值\n");
			StatisXSD s_xsd;
			s_xsd.GetXSD(STATIS_DAY,&es_time);
			s_xsd.GetXSD(STATIS_MONTH,&es_time);
		}*/
		if(hour == 9 && min == 0 && weekday == 1)
		{
			printf("周三统计停电分析表格\n");
			int byear,bmonth,bday;
			int bwyear,bwmonth,bwday;
			char sql[MAX_SQL_LEN];
			ErrorInfo err_info;

			GetBeforeTime(STATIS_DAY,year,month,day,&byear,&bmonth,&bday);
			GetBeforeweekDate(year,month,day,&bwyear,&bwmonth,&bwday);
			printf("停电研判统计：统计时间从%04d-%02d-%02d到%04d-%02d-%02d\n",bwyear,bwmonth,bwday,byear, bmonth, bday);
			CDci *m_oci = new CDci;
			ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
			if(!ret)
			{
				printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
				return -1;
			}
			memset(sql,0x00,sizeof(sql));
			sprintf(sql, "call evalusystem.result.DMS_TD_MATCH('%04d-%02d-%02d','%04d-%02d-%02d')",bwyear,bwmonth,bwday, byear, bmonth, bday);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
				return -1;
			}
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
				cout<<"日期格式错误！"<<endl;
				return -1;
			}
		}

		//统计失败处理
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

		//日统计处理
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
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//主站运行情况
						StatisDMS s_dms(&mul_type_err);
						ret = s_dms.GetDMSRunningRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				case STATIS_MODEL:
					{
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//主站模型评估
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
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//DA执行情况
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
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//DMS和GPMS信息交互情况
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
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//DMS和用采信息交互情况
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
						//建立日统计表
						CreateStatisTable(&itr_type_err->second.es_time);

						//终端运行情况
						StatisTerminal s_tu(&mul_type_err);
						ret = s_tu.GetTerminalRunningRate(STATIS_DAY, &itr_type_err->second.es_time);
						if(ret < 0)
							flag = 1;
						break;
					}
				}

				//删除日表数据
				DeleteStatisData(STATIS_DAY, &itr_type_err->second.es_time);

				CaculateAvgAll(STATIS_DAY,&itr_type_err->second.es_time);

				StatisDistrict district(&mul_type_err);
				district.CaculateDistrict(STATIS_DAY,&itr_type_err->second.es_time);
				CaculateScoreAverCity(STATIS_DAY,&itr_type_err->second.es_time);

				if(flag == 0)
					mul_type_err.erase(itr_type_err);

				itr_type_err++;
			}
		}

		//月统计处理
		num = mul_type_err.count(STATIS_MONTH);
		if(num > 0)
		{
			itr_type_err = mul_type_err.find(STATIS_MONTH);
			for (int i = 0; i < num; i++)
			{
				int flag = 0;
				//主站运行情况
				StatisDMS s_dms(&mul_type_err);				
				ret = s_dms.GetDMSRunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//主站模型评估, 模型要放到DA和GPMS统计之前
				StatisModel s_model(&mul_type_err);
				ret = s_model.GetDMSModelRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DA执行情况
				StatisDA s_da(&mul_type_err);
				ret = s_da.GetDARunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DMS和GPMS信息交互情况
				StatisDGPMS s_dgp(&mul_type_err);
				ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//DMS和用采信息交互情况
				StatisYC s_yc(&mul_type_err);
				ret = s_yc.GetYCRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//终端运行情况
				StatisTerminal s_tu(&mul_type_err);
				ret = s_tu.GetTerminalRunningRate(STATIS_MONTH, &itr_type_err->second.es_time);
				if(ret < 0)
					flag = 1;
				//删除月表数据
				DeleteStatisData(STATIS_MONTH, &itr_type_err->second.es_time);

				ESScore s_score;
				ret = s_score.InsertPointScore(STATIS_MONTH, &itr_type_err->second.es_time);
				ret = s_score.InsertScore(STATIS_MONTH, &itr_type_err->second.es_time);

				DeleteStatisData(STATIS_MONTH, &itr_type_err->second.es_time);//删除'数值'重复数据
				CaculateAvgAll(STATIS_MONTH,&itr_type_err->second.es_time);
				StatisDistrict district(&mul_type_err);
				district.CaculateDistrict(STATIS_MONTH,&itr_type_err->second.es_time);
				CaculateScoreAverCity(STATIS_MONTH,&itr_type_err->second.es_time);
				CaculateIndexDate(&itr_type_err->second.es_time);

				if(flag == 0)
					mul_type_err.erase(itr_type_err);

				itr_type_err++;
			}
		}

		//休眠到整半小时
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

//建立日/月统计表
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

	//日统计表
	for(int i = 1; i <= days; i++)
	{
		//判断表是否存在
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

		//建立索引
		sprintf(sql, "CREATE INDEX INDEX_%04d%02d%02d ON EVALUSYSTEM.RESULT.DAY_%04d%02d%02d"
			"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, i, year, mon, i);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateIndex: sql= %s \t error(%d) = %s;\n", sql,err_info.error_no,err_info.error_info);
			return -1;
		}

		//插入日指标初始化数据
		sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,"
			"statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'百分比',"
			"to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' "
			"from (select code from evalusystem.config.info where sequence!=-1 and code!='dmsaveop') a,"
			"(select organ_code from evalusystem.config.organ) b union all select a.code,b.organ_code,0,"
			"'百分比',to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无',"
			"'无' from (select code from evalusystem.config.info where sequence!=-1 and code='dmsaveop') a,"
			"(select organ_code from evalusystem.config.organ) b", 
			table_name, year, mon, i, year, mon, i, year, mon, i, year, mon, i);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("DayInit: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
			return -1;
		}
	}

	//月统计表
	//判断表是否存在
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

		//建立索引
		sprintf(sql, "CREATE INDEX INDEX_%04d%02d ON EVALUSYSTEM.RESULT.MONTH_%04d%02d"
			"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, year, mon);
		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			return -1;
		}

		//插入月指标初始化数据
		sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,"
			"updatetime,statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'百分比',"
			"to_date('%04d-%02d-01','YYYY-MM-DD'),to_date('%04d-%02d-01','YYYY-MM-DD'),'无','无' "
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

		//程序第一次启动为第一天，建立上一个月统计表
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

			//建立索引
			sprintf(sql, "CREATE INDEX INDEX_%04d%02d ON EVALUSYSTEM.RESULT.MONTH_%04d%02d"
				"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, year, mon);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
				return -1;
			}

			//插入月指标初始化数据
			sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,"
				"updatetime,statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'百分比',"
				"to_date('%04d-%02d-01','YYYY-MM-DD'),to_date('%04d-%02d-01','YYYY-MM-DD'),'无','无' "
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

			//建立索引
			sprintf(sql, "CREATE INDEX INDEX_%04d%02d%02d ON EVALUSYSTEM.RESULT.DAY_%04d%02d%02d"
				"(CODE, ORGAN_CODE, DIMVALUE)", year, mon, day, year, mon, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("CreateIndex: sql= %s \t error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
				return -1;
			}

			//插入日指标初始化数据
			sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,"
				"statistime,dimname,dimvalue) select a.code,b.organ_code,0.0,'百分比',"
				"to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' "
				"from (select code from evalusystem.config.info where sequence!=-1 and code!='dmsaveop') a,"
				"(select organ_code from evalusystem.config.organ) b union all select a.code,b.organ_code,0,"
				"'百分比',to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无',"
				"'无' from (select code from evalusystem.config.info where sequence!=-1 and code='dmsaveop') a,"
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

//检查可统计的日统计表
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
//		sprintf(table_name, "day_%04d%02d%02d"，year, mon, day);
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

//删除统计表数据
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
	
	//删除重复数据
	if(statis_type == STATIS_MONTH)
	{
		sprintf(sql_delete, "delete from evalusystem.result.month_%04d%02d a where rowid!=(select max(rowid) "
			"from evalusystem.result.month_%04d%02d b where a.code=b.code and a.organ_code=b.organ_code and "
			" a.datatype=b.datatype and a.dimname=b.dimname and a.dimvalue=b.dimvalue and a.value = b.value)",
			year, mon, year, mon);
	}
	else
	{
		sprintf(sql_delete, "delete from evalusystem.result.day_%04d%02d%02d a where rowid!=(select max(rowid) "
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
	
	//删除非最新数据
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

//计算指标值全市平均值及分子分母全市总加值
int CaculateAvgAll(int statis_type, ES_TIME *es_time)
{
	int ret;
	int year, mon, day, byear,bmon,bday;
	char sql[4096];
	ErrorInfo err_info;
	char table_name[20];

	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;

	GetBeforeTime(statis_type,year,mon,day,&byear,&bmon,&bday);

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}	

	/*sprintf(sql, "call evalusystem.result.CACULATEAVGALL('%04d-%02d-%02d','%04d-%02d')", byear, bmon, bday,byear,bmon);

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
	}*/

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",byear, bmon, bday);
	else
		sprintf(table_name, "month_%04d%02d",byear, bmon);

	sprintf(sql,"delete from result.%s where code like '%%Aver' or code like '%%All';",table_name);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	/*sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.code+'Aver',substr(a.organ_code,0,5),round(avg(a.value),4),a.datatype,sysdate,a.statistime,'无','无' from "
		"(select * from result.%s where datatype = '百分比' and dimname = '无' ) a,"
		"(select * from evalusystem.config.averall where flag = 0) b "
		"where a.code = b.code "
		"group by substr(a.organ_code,0,5),a.code,a.datatype,a.statistime;"
		,table_name,table_name);*/
	sprintf(sql,"insert into result.%s(organ_code,code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.organ_code,a.code+'Aver',round(a.v*0.95+b.v*0.05,4),a.d,sysdate,sysdate-1,'无','无' from "
		"(select a.code,organ.organ_code organ_code,a.v,a.d from "
		"(select a.code code,a.organ_code organ_code,a.value v,a.datatype d from " 
		"(select * from result.%s where datatype in( '百分比','数值' ) and dimname = '无' ) a, "
		"(select * from evalusystem.config.averall where flag = 0) b, "
		"(select * from config.organ where flag = 1 and subtype = 0) c  "
		"where a.code = b.code and a.organ_code = c.organ_code) a right join (select organ_code from "
		"evalusystem.config.organ where flag = 1) organ "
		"on(a.organ_code = organ.organ_code)) a, "
		"(select b.code,organ.organ_code,b.v,b.d from "
		"(select a.code code,substr(a.organ_code,0,5) organ_code,round(avg(a.value),4) v,a.datatype d from "
		"(select * from result.%s where datatype in( '百分比','数值' ) and dimname = '无' ) a, "
		"(select * from evalusystem.config.averall where flag = 0) b, "
		"(select * from config.organ where flag = 0) c "
		"where a.code = b.code and a.organ_code = c.organ_code "
		"group by substr(a.organ_code,0,5),a.code,a.datatype) b right join (select organ_code from "
		"evalusystem.config.organ where flag = 1) organ "
		"on(b.organ_code = organ.organ_code)) b "
		"where a.code = b.code and a.organ_code = b.organ_code and a.d = b.d",table_name,table_name,table_name);  //地市得分95%,县得分5%

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	//厦门没有县公司
	sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.code+'Aver',a.organ_code,round(avg(a.value),4),a.datatype,sysdate,sysdate-1,'无','无' from "
		"(select * from result.%s where datatype in( '百分比','数值' ) and dimname = '无' ) a,"
		"(select * from evalusystem.config.averall where flag = 0 ) b, "
		"(select * from config.organ where flag = 1 and subtype = 1) c "
		"where a.code = b.code and a.organ_code = c.organ_code "
		"group by a.organ_code,a.code,a.datatype;"
		,table_name,table_name);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.code+'Aver',a.organ_code,round(avg(a.value),4),a.datatype,sysdate,sysdate-1,'无','无' from "
		"(select * from result.%s where datatype in( '百分比','数值' ) and dimname = '无' ) a,"
		"(select * from evalusystem.config.averall where flag = 1) b "
		"where a.code = b.code "
		"group by a.organ_code,a.code,a.datatype;"
		,table_name,table_name);
	/*sprintf(sql,"select a.organ_code,a.code,a.v*0.95+b.v*0.05,a.d,sysdate,a.s,'无','无' from "
		"(select a.code code,a.organ_code organ_code,a.value v,a.datatype d,a.statistime s from "
		"(select * from result.%s where datatype = '百分比' and dimname = '无' ) a, "
		"(select * from evalusystem.config.averall where flag = 1) b, "
		"(select * from config.organ where flag = 1) c "
		"where a.code = b.code and a.organ_code = c.organ_code) a, "
		"(select a.code code,substr(a.organ_code,0,5) organ_code,round(avg(a.value),4) v from " 
		"(select * from result.%s where datatype = '百分比' and dimname = '无' ) a, "
		"(select * from evalusystem.config.averall where flag = 1) b, "
		"(select * from config.organ where flag = 1) c "
		"where a.code = b.code and a.organ_code = c.organ_code "
		"group by substr(a.organ_code,0,5),a.code,a.datatype,a.statistime) b "
		"where a.code = b.code and a.organ_code = b.organ_code "table_name,table_name);*/

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.code+'All',substr(a.organ_code,0,5),sum(a.value),a.datatype,sysdate,sysdate-1,'无','无' from"
		"(select * from result.%s where datatype in('个数','小时') and dimname = '无' ) a,"
		"(select * from evalusystem.config.averall where flag = 0) b "
		"where a.code = b.code "
		"group by substr(a.organ_code,0,5),a.code,a.datatype;"
		,table_name,table_name);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}	

	sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select a.code+'All',a.organ_code,sum(a.value),a.datatype,sysdate,sysdate-1,'无','无' from "
		"(select * from result.%s where datatype in('个数','小时') and dimname = '无' ) a,"
		"(select * from evalusystem.config.averall where flag = 1) b "
		"where a.code = b.code "
		"group by a.organ_code,a.code,a.datatype;"
		,table_name,table_name);

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

int CaculateScoreAverCity(int statis_type, ES_TIME *es_time)
{
	int ret;
	int year, mon, day, byear,bmon,bday;
	char sql[4096];
	ErrorInfo err_info;
	char table_name[20];

	year = es_time->year;
	mon = es_time->month;
	day = es_time->day;

	GetBeforeTime(statis_type,year,mon,day,&byear,&bmon,&bday);

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}	

	/*sprintf(sql, "call evalusystem.result.CACULATEAVGALL('%04d-%02d-%02d','%04d-%02d')", byear, bmon, bday,byear,bmon);

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
	}*/

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",byear, bmon, bday);
	else
		sprintf(table_name, "month_%04d%02d",byear, bmon);

	//求scoreavg地市值
	sprintf(sql,"insert into result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,dimvalue) "
		"select 'scoreAver',score.organ_code,round(sum(score.value*weight.weight),5),'数值',sysdate,sysdate-1,'无','无' from "
		"(select * from result.%s where code like '%%Aver' and datatype = '百分比') as score,"
		"(select code,weight from evalusystem.config.weight) as weight,"
		"(select organ_code from evalusystem.config.organ where flag = 1 ) organ "
		"where score.code = weight.code+'Aver' and organ.organ_code = score.organ_code "
		"group by score.organ_code order by score.organ_code;",table_name,table_name);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		return -1;
	}

	return 0;
}

//计算首页数据
int CaculateIndexDate( ES_TIME *es_time)
{
	char query[5000];
	char tempstr[300];
	int ret = 0;
	int rec_num,rec;
	int col_num,col;
	int organ_code = 0;
	ErrorInfo err_info;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	int pyear = 0,pmonth = 0,pday = 0;

	GetBeforeTime(STATIS_DAY,es_time->year,es_time->month,es_time->day,&pyear,&pmonth,&pday);

	vector<int> organ_temp;
	vector<int>::iterator it_organ;

	memset(query,0x00,sizeof(query));

	CDci *m_oci = new CDci;
	ret = m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
	if(!ret)
	{
		printf("Connect: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		return -1;
	}	

	sprintf(query,"SELECT ORGAN_CODE FROM CONFIG.ORGAN WHERE FLAG = 1;");

	ret = m_oci->ReadData(query, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1) {
		m_presult = buffer;
		for (rec = 0; rec < rec_num; rec++) {

			for(col = 0; col < col_num; col++) {

				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				organ_code = *(int*)(tempstr);
				m_presult += col_attr[col].data_size;
			}
			organ_temp.push_back(organ_code);
		}
	}
	else {

		printf("ReadData: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,query);
		return -1;
	}

	sprintf(query,"TRUNCATE TABLE EVALUSYSTEM.RESULT.DEVICE_STATIS;");

	ret = m_oci->ExecSingle(query,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,query);
		return -1;
	}

	it_organ = organ_temp.begin();
	for (; it_organ != organ_temp.end();++it_organ) {

		memset(query,0x00,sizeof(query));
		sprintf(query,"INSERT INTO EVALUSYSTEM.RESULT.DEVICE_STATIS(ID,ORGAN_CODE,KG_COUNT,KG_AVG,PB_COUNT,PB_AVG,DZ_COUNT,DZ_AVG,MX_COUNT,MX_AVG,ZF_COUNT,ZF_AVG,GZ_COUNT,GZ_AVG,EY_COUNT,EY_AVG,SY_COUNT,SY_AVG) "
			"VALUES(%d,%d,"
			"(select count(*) as value from detail.gpmscb where type='1' and organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d where code = 'breakfineAver' and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select count(*) from detail.gpmsld where organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d where code = 'ldfineAver' and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select count(*) from detail.gpmscb where  type='0' and organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d  where code = 'disfineAver' and dimname='无' and datatype='数值'and organ_code = %d),"
			"(select count(*) from detail.gpmsbus where organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d where code = 'busfineAver' and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select count(*) from detail.gpmsst where organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d  where code = 'substionfineAver'  and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select  sum(value) as value  from RESULT.DAY_%04d%02d%02d where code='indicatornum' and dimname='无' and organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d where code = 'incovlineAver' and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select  sum(value) as value  from RESULT.DAY_%04d%02d%02d where code='tworemotenum' and dimname='无' and organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d where code = 'twocovlineAver' and dimname='无' and datatype='数值' and organ_code = %d),"
			"(select sum(value) from RESULT.DAY_%04d%02d%02d where code='threeremotenum' and dimname='无' and organ_code like '%d%%'),"
			"(select value from RESULT.DAY_%04d%02d%02d  where code = 'threecovlineAver' and dimname='无' and datatype='数值' and organ_code = %d));",
			*it_organ,*it_organ,
			*it_organ,
			pyear,pmonth,pday,*it_organ,
			*it_organ,
			pyear,pmonth,pday,*it_organ,
			*it_organ,
			pyear,pmonth,pday,*it_organ,
			*it_organ,
			pyear,pmonth,pday,*it_organ,
			*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ,
			pyear,pmonth,pday,*it_organ);

		ret = m_oci->ExecSingle(query,&err_info);
		if(!ret)
		{
			printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,query);
			return -1;
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

//初始化初次统计日期
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
		if(CheckCorrective(&es_time))	//整改率统计
		{
			StatisSupport s_support;
			s_support.GetCorrective(&es_time, 0);
		}
		else {

			StatisSupport s_support;
			s_support.GetCorrective(&es_time,1);
		}

		StatisAnalyze(&es_time, 1);


		tmptime += 86400;
	}

	return 0;
}

//统计
int StatisAnalyze(ES_TIME *es_time, int cooperation)
{
//#define TEST_DMS 1
//#define TEST_MODEL 1
//#define TEST_FA 1
//#define TEST_DGPMS 1
#define TEST_YC 1
//#define TEST_TER 1
#define TEST_ZLP 1
//#define TEST_XSD 1
//#define TEST_ZTGJ 1
//#define TEST_YD 1
//#define TEST_LINE 1

	int ret;

	//建立日/月统计表
	ret = CreateStatisTable(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "创建日/月统计表失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#if 1
#if TEST_DMS
	//日统计
	//主站运行情况
	printf("日统计\n");
	printf("主站运行情况统计\n");
	StatisDMS s_dms(&mul_type_err);
	ret = s_dms.GetDMSRunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站运行情况日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_MODEL
	//主站模型评估, 模型要放到DA和GPMS统计之前
	printf("主站模型评估统计\n");
	StatisModel s_model(&mul_type_err);
	if(cooperation == 1)
	{
		//模型校验
		ret = s_model.ModelCheck(es_time);
		if(ret < 0)
		{
			// 		plog->WriteLog(2, "读取模型信息失败，所统计日期为：%04d-%02d-%02d",
			// 			es_time->year, es_time->month, es_time->day);
		}
	}
	ret = s_model.getHHStatus(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "读取模型信息失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.DeleteHH(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "读取模型信息失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetModelInfo(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "读取模型信息失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetDMSModelRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站模型日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_model.GetModelProcess(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站模型数据处理失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_FA
	//DA执行情况
	printf("DA执行情况统计\n");
	StatisDA s_da(&mul_type_err);
	ret = s_da.GetDARunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "馈线自动化执行情况日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_da.GetDAProcess(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "馈线自动化数据处理失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif

#if TEST_DGPMS
	//DMS和GPMS信息交互情况
	printf("DMS和GPMS信息交互情况统计\n");
	StatisDGPMS s_dgp(&mul_type_err);
	ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与GPMS信息交互日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_dgp.GetDGPMSProcess(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与GPMS信息交互数据处理失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_YC
	//DMS和用采信息交互情况
	printf("DMS和用采信息交互情况统计\n");
	StatisYC s_yc(&mul_type_err);
	ret = s_yc.GetYCRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与用采信息交互日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
	ret = s_yc.GetYCProcess(es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与用采信息交互数据处理失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_TER
	//终端运行情况
	printf("终端运行情况统计\n");
	StatisTerminal s_tu(&mul_type_err);
	ret = s_tu.GetTerminalRunningRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电终端运行情况日统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_ZLP
	//指令票情况
	printf("指令票情况统计\n");
	StatisZLP s_zlp;
	ret = s_zlp.GetZLPRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		printf("指令票统计失败！");
	}
#endif
#if TEST_XSD
	//小水电情况
	printf("小水电情况统计\n");
	StatisXSD s_xsd;
	ret = s_xsd.GetXSD(STATIS_DAY, es_time);
	if(ret < 0)
	{
		printf("小水电统计失败！");
	}
#endif
#if TEST_ZTGJ
	//开关状态估计情况
	printf("开关状态估计情况统计\n");
	StatisZTGJ s_ztgj;
	ret = s_ztgj.GetZTGJ(STATIS_DAY, es_time);
	if(ret < 0)
	{
		printf("开关状态估计统计失败！");
	}
#endif
#if TEST_YD
	//异动单情况
	printf("异动单情况统计\n");
	StatisYD s_yd;
	ret = s_yd.GetYDDRate(STATIS_DAY, es_time);
	if(ret < 0)
	{
		printf("异动单统计失败！");
	}
	s_yd.GetGDMSYDDDetail(es_time);
#endif
#ifdef TEST_LINE
	// 线路终端覆盖率情况
	printf("线路终端覆盖率统计\n");
	STATISLINES s_line;
	ret = s_line.Operator(STATIS_DAY,es_time);
	if (ret < 0)
	{
		printf("线路终端覆盖率统计失败!");
	}
#endif
	//删除日表数据
	printf("日表去重\n");
	DeleteStatisData(STATIS_DAY, es_time);

	ESScore s_score;
	ret = s_score.InsertPointScore(STATIS_DAY, es_time);
	ret = s_score.InsertScore(STATIS_DAY, es_time);
	DeleteStatisData(STATIS_DAY, es_time);//删除'数值'重复数据
	CaculateAvgAll(STATIS_DAY,es_time);

	StatisDistrict district(&mul_type_err);
	district.CaculateDistrict(STATIS_DAY,es_time);
	CaculateScoreAverCity(STATIS_DAY,es_time);
	//月统计
#if TEST_DMS
	//主站运行情况
	printf("月统计\n");
	printf("主站运行情况统计\n");
	ret = s_dms.GetDMSRunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站运行情况月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif

#if TEST_MODEL
	//主站模型评估
	printf("主站模型评估统计\n");
	ret = s_model.GetDMSModelRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站模型月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_FA
	//DA执行情况
	printf("DA执行情况统计\n");
	ret = s_da.GetDARunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "馈线自动化执行情况月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_DGPMS
	//DMS和GPMS信息交互情况
	printf("DMS和GPMS信息交互情况统计\n");
	ret = s_dgp.GetGMSGPMSInteractiveRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与GPMS信息交互月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_YC
	//DMS和用采信息交互情况
	printf("DMS和用采信息交互情况统计\n");
	ret = s_yc.GetYCRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电主站与用采信息交互月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_TER
	//终端运行情况
	printf("终端运行情况统计\n");
	ret = s_tu.GetTerminalRunningRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		// 		plog->WriteLog(2, "配电终端运行情况月统计失败，所统计日期为：%04d-%02d-%02d",
		// 			es_time->year, es_time->month, es_time->day);
	}
#endif
#if TEST_ZLP
	//指令票情况
	printf("指令票情况统计\n");
	ret = s_zlp.GetZLPRate(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		printf("指令票统计失败！");
	}
#endif
#if TEST_XSD
	//小水电情况
	printf("小水电情况统计\n");
	ret = s_xsd.GetXSD(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		printf("小水电统计失败！");
	}
#endif
#if TEST_ZTGJ
	//开关状态估计情况
	printf("开关状态估计情况统计\n");
	ret = s_ztgj.GetZTGJ(STATIS_MONTH, es_time);
	if(ret < 0)
	{
		printf("开关状态估计统计失败！");
	}
#endif
#if TEST_YD
	//异动单情况
	printf("异动单情况统计\n");

	ret = s_yd.GetYDDRate(STATIS_MONTH, es_time);

	if(ret < 0)
	{
		printf("异动单情况统计失败！");
	}

#endif
#ifdef TEST_LINE
	// 线路终端覆盖率情况
	printf("线路终端覆盖率统计\n");
	ret = s_line.Operator(STATIS_MONTH,es_time);
	if (ret < 0)
	{
		printf("线路终端覆盖率统计失败!");
	}
#endif

	//删除月表数据
	printf("月表去重\n");
	DeleteStatisData(STATIS_MONTH, es_time);

	ret = s_score.InsertPointScore(STATIS_MONTH, es_time);
	ret = s_score.InsertScore(STATIS_MONTH, es_time);

	DeleteStatisData(STATIS_MONTH, es_time);//删除'数值'重复数据
	CaculateAvgAll(STATIS_MONTH,es_time);

	district.CaculateDistrict(STATIS_MONTH,es_time);
	CaculateScoreAverCity(STATIS_MONTH,es_time);
	CaculateIndexDate(es_time);
#endif

	return 0;
}

//校验时间格式
int CheckTimeString(const string &check_time)
{
	//20140612
	int year, month, day;

	//长度校验
	if (check_time.size() != 8)
	{
		cout << "charactor size error! size =  " << check_time.size() << endl;
		return -1;
	}

	//字符校验
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

	//日期校验
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
		if(day < 1 || day > GetDaysInMonth(year, month))
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
		<<"\t1) begin_time: 开始时间，程序从开始时间统计到当天，然后开始正常程序流程；"<<endl
		<<"\t2) end_time: 结束时间，程序从开始时间统计到结束时间，然后退出。"<<endl;
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

//清空dms和gpms表数据
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

	//获取GPMS全量体统计划
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

//获取启动配置
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
		*cooperation = 1;	//默认模型校验程序随统计程序启动
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

//整改率统计时间判断
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
	if(month_num % 2 != 0 && tmp_tm.tm_wday == 0)	//判断是否单周周日
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

	cout << "注意：屏幕输出已写入日志文件，请用cat命令查看" << szLogPath << "目录下相应文件。" << endl;

	pipe(pipefd);

	//将输出缓存设置为行缓存模式
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

			//如果是新的一天，则重新建立新的日志文件
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

//数据校验表去重 add by lcm 20150915
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

void* network_check()
{
	StatisNet net;
	net.GetConfig();
	net.UpdateNetCount();
	net.InsertNetScore();
}


