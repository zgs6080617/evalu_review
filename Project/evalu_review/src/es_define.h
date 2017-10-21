/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_define.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 定义相关头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _ES_DEFINE_H_
#define _ES_DEFINE_H_
#include <iostream>
#include <time.h>
#include <vector>
#include "dcisg.h"

using namespace std;

#define MAX_SQL_LEN 1024
#define STATIS_DAY 0
#define STATIS_MONTH 1

#define STATIS_DMS 2
#define STATIS_DA 3
#define STATIS_MODEL 4
#define STATIS_DGPMS 5
#define STATIS_TERMINAL 6
#define STATIS_YC 7

#define DIM_DMS 0
#define DIM_LINE 1
#define DIM_DEV 2
#define DIM_GP 3
#define DIM_TERMINAL 4
#define DIM_CB 5

//#define DIM_SCORE 6

#ifdef _HOME_ENV
#define DATABASE_NAME "192.168.100.6"
#else
#define DATABASE_NAME getenv("DMS_OCI_SRV")
#endif // _HOME_ENV
#define USERNAME "EVALU"
#define PASSWORD "EVALU"

//#define DEBUG
//#define DEBUG_DATE
//#define DEBUG_ERROR
#define  DEBUG_TEST

// 内存释放
#define FREE(ptr) \
	if(ptr != NULL) \
	{\
		free(ptr);\
		ptr = NULL;\
	}

struct ES_DMSRUNNING
{
	int organ_code;
	float online;

	ES_DMSRUNNING()
	{
		organ_code = 0;
		online = 0;
	}
};

struct ES_DATIMES
{
	int organ_code;
	string dv;
	float count;

	ES_DATIMES()
	{
		organ_code = 0;
		count = 0;
	}
};

struct ES_TQ 
{
	int organ_code;
	float total;
	float sendnum;
	float threenum;

	ES_TQ()
	{
		organ_code = 0;
		total = 0;
		sendnum = 0;
		threenum = 0;
	}
};

struct ES_GZZSQ
{
	int organ_code;
	float cover_rate;
	float shake_rate;
	float misoperation_rate;
	float missreport_rate;
	float tworatiocover_rate;
	float threeratiocover_rate;

	ES_GZZSQ()
	{
		organ_code = 0;
		cover_rate = 0;
		shake_rate = 0;
		misoperation_rate = 0;
		missreport_rate = 0;
		tworatiocover_rate = 0;
		threeratiocover_rate = 0;
	}
};

struct ES_YCRT
{
	int organ_code;
	float ycrt;

	ES_YCRT()
	{
		organ_code = 0;
		ycrt = 0;
	}
};

struct ES_TIME
{
	int year;
	int month;
	int day;

	ES_TIME()
	{
		year = 1970;
		month = 1;
		day = 1;
	}
};

struct ES_RESULT
{
	int m_ntype;
	string m_strtype;
	float m_fresult;
	int m_nresult;

	ES_RESULT()
	{
		m_ntype = 0;
		m_fresult = 0.0;
		m_nresult = 0;
	}
};

struct ES_ERROR_INFO
{
	int statis_type;
	ES_TIME es_time;

	bool operator==(const ES_ERROR_INFO& rightInfo)
	{
		return (statis_type == rightInfo.statis_type &&
			es_time.year == rightInfo.es_time.year &&
			es_time.month == rightInfo.es_time.month &&
			es_time.day == rightInfo.es_time.day);
	}
};

struct ES_ORGAN_GP
{
	int organ_code;
	string gp_type;

	ES_ORGAN_GP()
	{
		organ_code = 0;
	}

	bool operator==(const ES_ORGAN_GP& right)
	{
		return (organ_code == right.organ_code && gp_type == right.gp_type);
	}

	 bool operator<(const ES_ORGAN_GP &a) const
	 {
		 return organ_code < a.organ_code || 
			 (organ_code == a.organ_code && strcmp(gp_type.c_str(), a.gp_type.c_str()) < 0);
	 }
};
struct ES_DATT
{
	int organ_code;
	string dv;
	time_t gtime;
	int src;

	ES_DATT()
	{
		organ_code = 0;
		src = 0;
	}
};

struct ES_ACC
{
	int organ_code;
	string dv;
	string da;
	float count;
};

struct ES_HH
{
	string dv;
	string reason;
};

//两周指标整改率zgs
struct ES_Corrective
{
	int organ_code;
	float confailnum;//仍未整改
	float failnum;//不匹配设备总数

	ES_Corrective()
	{
		organ_code = 0;
		confailnum = 0;
		failnum = 0;
	}
};

//DMS停电研判发布率
struct ES_GDMSJUDGED
{
	int type;   //0:dms 1:gpms
	int num;    //停电范围是否包含相同配变，是0 否1
	int organ_code;
	int judged_type;
	int sendtype;
	string organ_name;
	string dmsid;
	string fault_no;
	string DMSDdaffTime; //调度员在DMS点击发布时间
	string DMSJudgedTime;
	string GPMSJudgedTime;
	string DMSJudged;
	string GPMSJudged;
	string count_time;

	ES_GDMSJUDGED()
	{
		type = 0;
	}
};

//权重
struct ES_WEIGHT
{
	float weight;
	string code;
};

//分值划分
struct ES_SCORE
{
	string code;
	float value;
	float score;
};

//百分比
struct ES_MARK
{
	int organ_code;
	string code;
	float value;
};

// 地区分子比分母
struct ES_DISTRICT
{
	int organ_code;
	char code[20];
	float div;
	float divend;
	float result; //百分比
	float value; //数值
};

#endif
