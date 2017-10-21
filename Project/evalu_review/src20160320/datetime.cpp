/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : datetime.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 时间相关通用函数实现
 * History
 * ===============================================
 * 2014-06-24  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include "datetime.h"
#include "es_define.h"

/*************************************************
* Function  : WhichDayInWeek()
* Parameter :
* 		in  : ntime - 时间
*		out : none
* Return    :
* 		failed  : none
*		success : none
* Description : 根据该时间，获得星期信息。
**************************************************/
int WhichDayInWeek(const time_t ntime)
{
	struct tm *pbuf,tmbuf;

	pbuf = localtime_r(&ntime,&tmbuf);
	if(pbuf == NULL)
	{
		printf("localtime_r: error = %s\n",strerror(errno));
	}

	//return pbuf == NULL ? 0 : pbuf->tm_min;
	return pbuf == NULL ? 0 : pbuf->tm_wday;
}

/*************************************************
* Function  : WhichDayInMonth()
* Parameter :
* 		in  : ntime - 时间
*		out : none
* Return    :
* 		failed  : none
*		success : none
* Description : 根据该时间，获得日信息。
**************************************************/
int WhichDayInMonth(const time_t ntime)
{
	struct tm *pbuf,tmbuf;

	pbuf = localtime_r(&ntime,&tmbuf);
	if(pbuf == NULL)
	{
		printf("localtime_r: error = %s\n",strerror(errno));
	}

	return pbuf == NULL ? 0 : pbuf->tm_mday;
}

/*************************************************
* Function  : GetCurDate()
* Parameter :
* 		in  : year,mon,day;
*		out : none
* Return    :
* 		failed  : none
*		success : none
* Description : 获取当前日期
**************************************************/
int GetCurDate(int *year,int *mon,int *day)
{
	time_t now_time;
	struct tm *ptime,datetime;

	time(&now_time);    // 当前时间
	ptime = localtime_r(&now_time,&datetime);
	if(ptime == NULL)
	{
		return -1;
	}

	if(year != NULL)
	{
		*year = ptime->tm_year + 1900;   // 年
	}
	if(mon != NULL)
	{
		*mon = ptime->tm_mon + 1;    // 月
	}
	if(day != NULL)
	{
		*day = ptime->tm_mday;   // 日
	}

	return 0;
}

/*************************************************
* Function  : GetCurTime()
* Parameter :
* 		in  : ntime - 时间
*		out : none
* Return    :
* 		failed  : none
*		success : none
* Description : 根据该时间，获得日信息。
**************************************************/
int GetCurTime(int *hour,int *min,int *sec)
{
	time_t now_time;
	struct tm *ptime,datetime;

	time(&now_time);    // 当前时间
	ptime = localtime_r(&now_time,&datetime);
	if(ptime == NULL)
	{
		return -1;
	}

	if(hour != NULL)
	{
		*hour = ptime->tm_hour; // 小时
	}
	if(min != NULL)
	{
		*min = ptime->tm_min;    // 分钟
	}
	if(sec != NULL)
	{
		*sec = ptime->tm_sec;    // 秒
	}

	return 0;
}

/****************************************************
* Function  : TransTimeToString()
* Parameter :
* 		in  : sec,timep,length;
*		out : none
* Return    :
* 		failed  : -1
*		success : 0
* Description : 转换日期格式-YYYYMMDDhhmmss;
****************************************************/
int TransTimeToString(long sec,char *timep,int length)
{
	int ret = 0;
	struct tm *ptime,datetime;

	ptime = localtime_r(&sec,&datetime);  // localtime非线程安全
	if(ptime == NULL)
	{
		return -1;
	}

	ret = snprintf(timep,length,"%04d%02d%02d%02d%02d%02d",
		ptime->tm_year + 1900,      // year
		ptime->tm_mon + 1,  // month
		ptime->tm_mday,     // day
		ptime->tm_hour,     // hour
		ptime->tm_min,      // minute
		ptime->tm_sec);     // second

	return 0;
}

/****************************************************
* Function  : TransTimeToLong()
* Parameter :
* 		in  : timep,format;
*		out : sec
* Return    :
* 		failed  : -1
*		success : time
* Description : format格式为strptime中的格式;
****************************************************/
long TransTimeToLong(const char *timep,const char *format)
{
	time_t value = -1;
	struct tm tm = {0};

	if(strptime(timep,format,&tm) != NULL)
	{
		value = mktime(&tm);
	}

	return (long)value;
}

/****************************************************
* Function  : GetDaysInMonth()
* Parameter :
* 		in  : year,month;
*		out : none
* Return    :
* 		failed  : -1
*		success : days
* Description : 获取当月日期数;
****************************************************/
int GetDaysInMonth(int year,int month)
{
	int days;
	int days_array[] = {31,28,31,30,31,30,31,31,30,31,30,31};

	if (2 == month)
	{
		days = (((0 == year % 4) && (0 != year % 100) || ( 0 == year % 400)) ? 29 : 28);
	}
	else
	{
		days = days_array[month - 1];

	}

	return days;
}

/****************************************************
* Function  : GetBeforeTime()
* Parameter :
* 		in  : statis_type, year, month ,day
*		out : byear, bmonth, bday
* Return    :
* 		failed  : none
*		success : none
* Description : 获取前日日期;
****************************************************/
void GetBeforeTime(int static_type, int year, int month ,int day, int *byear, int *bmonth, int *bday)
{
#ifdef DEBUG_DATE
	year = 2014;
	day = 14;
	month = 6;
#endif

	//if(statis_type == STATIS_DAY)
	//{
	if(month == 1 && day == 1)
	{
		*byear = year - 1;
		*bmonth = 12;
		*bday = 31;
	}
	else if(month != 1 && day == 1)
	{
		*byear = year;
		*bmonth = month - 1;
		*bday = GetDaysInMonth(year, *(int *)bmonth);
	}
	else
	{
		*byear = year;
		*bmonth = month;
		*bday = day - 1;
	}
}

/****************************************************
* Function  : GetAfterTime()
* Parameter :
* 		in  : year, month ,day
*		out : ayear, amonth, aday
* Return    :
* 		failed  : none
*		success : none
* Description : 获取前日日期;
****************************************************/
int GetAfterTime(int year, int month ,int day, int *ayear, int *amonth, int *aday)
{
	int yearnow,monthnow,daynow;
	struct tm timeinput,tm_after;
	time_t tmptime,tmptimenow;
	timeinput.tm_year = year - 1900;
	timeinput.tm_mon = month - 1;
	timeinput.tm_mday = day;
	timeinput.tm_hour = 0;
	timeinput.tm_min = 0;
	timeinput.tm_sec = 0;
	tmptime = mktime(&timeinput) + 86400;
	
	GetCurDate(&yearnow, &monthnow, &daynow);
	timeinput.tm_year = yearnow - 1900;
	timeinput.tm_mon = monthnow - 1;
	timeinput.tm_mday = daynow;
	timeinput.tm_hour = 0;
	timeinput.tm_min = 0;
	timeinput.tm_sec = 0;
	tmptimenow = mktime(&timeinput);
	
	if(tmptime > tmptimenow)
	{
		printf("输入时间超出计算时间！\n");
		return -1;
	}
	
	tm_after = *localtime(&tmptime);
	*ayear = tm_after.tm_year + 1900;
	*amonth = tm_after.tm_mon + 1;
	*aday = tm_after.tm_mday;
	return 0;
};

/****************************************************
* Function  : GetTimeTByYYYYMMDDHHMMSS()
* Parameter :
* 		in  : timestr
*		out : none
* Return    :
* 		failed  : -1
*		success : rettime
* Description : 将string型时间转换成秒数;
****************************************************/
time_t GetTimeTByYYYYMMDDHHMMSS(const string & timestr)
{
	if (timestr.size() < 19)
	{
		cout << "timestr.size: " << timestr.size() << endl;
		return -1;
	}

	struct tm timevalue;
	int year, month, day, hour, min, sec;

	year = atoi(string(timestr.begin(), timestr.begin()+4).c_str());
	month = atoi(string(timestr.begin()+5, timestr.begin()+7).c_str());
	day = atoi(string(timestr.begin()+8, timestr.begin()+10).c_str());
	hour = atoi(string(timestr.begin()+11, timestr.begin()+13).c_str());
	min = atoi(string(timestr.begin()+14, timestr.begin()+16).c_str());
	sec = atoi(string(timestr.begin()+17, timestr.begin()+19).c_str());
	timevalue.tm_year = year-1900;
	timevalue.tm_mon = month-1;
	timevalue.tm_mday = day;
	timevalue.tm_hour = hour;
	timevalue.tm_min = min;
	timevalue.tm_sec = sec;
	if ( 0 )
	{
		cout << "timevalue.tm_year: " << timevalue.tm_year << endl;
		cout << "timevalue.tm_mon : " << timevalue.tm_mon << endl;
		cout << "timevalue.tm_mday: " << timevalue.tm_mday << endl;
		cout << "timevalue.tm_hour: " << timevalue.tm_hour << endl;
		cout << "timevalue.tm_min : " << timevalue.tm_min << endl;
		cout << "timevalue.tm_sec : " << timevalue.tm_sec << endl;
	}
	//char temp[20];
	//bzero(temp, sizeof(temp));
	//strftime(temp, 15, "%Y%m%d%H%M", &timevalue);
	time_t rettime = mktime(&timevalue);

	return rettime;
}
/****************************************************
* Function  : GetBeforeweekdate()
* Parameter :
* 		in  : statis_type, year, month ,day
*		out : byear, bmonth, bday
* Return    :
* 		failed  : none
*		success : none
* Description : 获取前一周日期;
****************************************************/
void GetBeforeweekDate(int year, int month ,int day, int *byear, int *bmonth, int *bday)
{
#ifdef DEBUG_DATE
	year = 2014;
	day = 14;
	month = 6;
#endif

	//if(statis_type == STATIS_DAY)
	//{
	if(month == 1 && day <= 7)
	{
		*byear = year - 1;
		*bmonth = 12;
		*bday = 24+day;
	}
	else if(month != 1 && day <= 7)
	{
		*byear = year;
		*bmonth = month - 1;
		*bday = GetDaysInMonth(year, *(int *)bmonth) - 7 + day;
	}
	else
	{
		*byear = year;
		*bmonth = month;
		*bday = day - 7;
	}
}