/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : ����״̬����ͳ��
 * History
 * ===============================================
 * 2015-12-30  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_ZTGJ_H_
#define _STATIS_ZTGJ_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisZTGJ
{
public:
	StatisZTGJ();
	~StatisZTGJ();
	//��ȡ����״̬������ֵ
	int GetZTGJsum(int statis_type,ES_TIME *es_time);
	//���뿪��״̬������ֵ�����ݿ�
	int InsertZTGJ(int statis_type, ES_TIME *es_time);
	//����״̬������ֵ����
	int GetZTGJ(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_ztgj_sum;//����״̬������ֵ
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

