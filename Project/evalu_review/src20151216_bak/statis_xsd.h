/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : Сˮ��ָ��
 * History
 * ===============================================
 * 2015-12-10  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_XSD_H_
#define _STATIS_XSD_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisXSD
{
public:
	StatisXSD();
	~StatisXSD();
	//��ȡСˮ����ֵ
	int GetXSDsum(int statis_type,ES_TIME *es_time);
	//����Сˮ����ֵ�����ݿ�
	int InsertXSD(int statis_type, ES_TIME *es_time);
	//Сˮ����ֵ����
	int GetXSD(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_xsd_sum;//Сˮ����ֵ
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif