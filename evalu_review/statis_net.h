/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : ftp�����������ָ��
 * History
 * ===============================================
 * 2015-10-26  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_NET_H_
#define _STATIS_NET_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisNet
{
public:
	StatisNet();
	~StatisNet();
	//��ȡ�ŵ���ftp������IP��ַ
	int GetConfig();
	//ÿ����Сʱ����һ����������ָ��
	int UpdateNetCount();
	//������������ָ��ֵ
	int GetNetValue();
	//����ָ��ֵ
	int InsertNetScore();
private:
	//�ŵ���IP��ַ
	map<int,string> map_ipaddr;
	//�ŵ������������÷�
	map<int,float> map_netscore;
	//�ŵ�����������ʵʱ�÷�
	map<int,int> map_netquality;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

