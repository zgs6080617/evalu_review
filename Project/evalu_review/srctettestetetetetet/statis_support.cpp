/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_support.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 辅助统计实现
 * History
 * ===============================================
 * 2015-01-28  guoqiuye create
 * ===============================================
*******************************************************************************/
#include "statis_support.h"
#include <iostream>
#include "datetime.h"

using namespace std;

StatisSupport::StatisSupport()
{
	/*m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);*/
}

StatisSupport::~StatisSupport()
{
	//m_oci->DisConnect(&err_info);
}

int StatisSupport::GetCorrective(ES_TIME *es_time)
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

	sprintf(sql, "call evalusystem.result.CORRECTIVEPROCESS('%04d-%02d-%02d')", byear, bmonth, bday);
	//cout<<sql<<endl;
 	ret = d5000.d5000_ExecSingle(sql,&err_info);
 	if(!ret)
 	{
 		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
 		return -1;
 	}

	return 0;
}

