/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : datetime.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 时间相关通用函数头文件
 * History
 * ===============================================
 * 2014-06-24  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _DATETIME_H
#define _DATETIME_H

#include <time.h>
#include <iostream>

using namespace std;

// 根据该时间，获得星期信息。
int WhichDayInWeek(const time_t ntime);

// 根据该时间，获取月信息。
int WhichDayInMonth(const time_t ntime);

// 获得当前日期
int GetCurDate(int *year,int *mon,int *day);

// 获得当前时间
int GetCurTime(int *hour,int *min,int *sec);

// 将long型时间转换成字符格式YYYYMMDDhhmmss时间
int TransTimeToString(long sec,char *timep,int length);

// 将字符串格式YYYYMMDDhhmmss时间转换成long型时间
long TransTimeToLong(const char *timep,const char *format);

//获取当月日期数
int GetDaysInMonth(int year,int month);

//获取前日日期
void GetBeforeTime(int statis_type, int year, int month ,int day, int *byear, int *bmonth, int *bday);

//将string型时间转换成秒数
time_t GetTimeTByYYYYMMDDHHMMSS(const string & timestr);

#endif // end datetime.h
