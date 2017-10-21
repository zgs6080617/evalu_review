/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : datetime.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : ʱ�����ͨ�ú���ͷ�ļ�
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

// ���ݸ�ʱ�䣬���������Ϣ��
int WhichDayInWeek(const time_t ntime);

// ���ݸ�ʱ�䣬��ȡ����Ϣ��
int WhichDayInMonth(const time_t ntime);

// ��õ�ǰ����
int GetCurDate(int *year,int *mon,int *day);

// ��õ�ǰʱ��
int GetCurTime(int *hour,int *min,int *sec);

// ��long��ʱ��ת�����ַ���ʽYYYYMMDDhhmmssʱ��
int TransTimeToString(long sec,char *timep,int length);

// ���ַ�����ʽYYYYMMDDhhmmssʱ��ת����long��ʱ��
long TransTimeToLong(const char *timep,const char *format);

//��ȡ����������
int GetDaysInMonth(int year,int month);

//��ȡǰ������
void GetBeforeTime(int statis_type, int year, int month ,int day, int *byear, int *bmonth, int *bday);

//��string��ʱ��ת��������
time_t GetTimeTByYYYYMMDDHHMMSS(const string & timestr);

#endif // end datetime.h
