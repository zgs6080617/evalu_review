/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : ָ��Ʊָ��
 * History
 * ===============================================
 * 2015-11-12  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_ZLP_H_
#define _STATIS_ZLP_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisZLP
{
public:
	StatisZLP();
	~StatisZLP();
	//��ȡDMS������GPMS����ָ��Ʊ����
	int GetDMSZLPnum(int statis_type,ES_TIME *es_time);
	//��ȡGPMS����ָ��Ʊ����
	int GetGPMSZLPnum(int statis_type,ES_TIME *es_time);
	//����ָ��Ʊָ��ֵ
	int SetZLPRate();
	//����ָ��Ʊ���ݵ����ݿ�
	int InsertZLPRate(int statis_type, ES_TIME *es_time);
	//ָ��Ʊ����
	int GetZLPRate(int statis_type, ES_TIME *es_time);
	int Clear();
private:
	map<int,float> map_dmszlp_num;//DMSָ��Ʊ
	map<int,float> map_gpmszlp_num;//GPMSָ��Ʊ
	
	//�ŵ���ָ��Ʊ�÷�
	map<int,float> map_zlp_mark;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif