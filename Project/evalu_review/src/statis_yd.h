/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_net.h
 * Author  : lichengming
 * Version : v1.0
 * Function : �춯ָ��
 * History
 * ===============================================
 * 2015-11-12  lichengming create
 * ===============================================
*******************************************************************************/
#ifndef _STATIS_YD_H_
#define _STATIS_YD_H_
#include <iostream>
#include <map>
#include <string>
#include "dcisg.h"
#include "datetime.h"
#include "es_define.h"

class StatisYD
{
public:
	StatisYD();
	~StatisYD();
	//��ȡDMS������GPMS�����춯������
	int GetGDMSYDDnum(int statis_type,ES_TIME *es_time);
	//��ȡGPMS�����춯������
	int GetGPMSYDDnum(int statis_type,ES_TIME *es_time);
	//�����춯��ָ��ֵ
	int SetYDDRate();
	//�����춯�����ݵ����ݿ�
	int InsertYDDRate(int statis_type, ES_TIME *es_time);
	//�춯������
	int GetYDDRate(int statis_type, ES_TIME *es_time);
	int Clear();

	//��ȡ�춯��δ���˵���
	int GetYDDnumNoRoolback(int statis_type,ES_TIME *es_time);

	//��ȡ�춯����������
	int GetYDDnumTotal(int statis_type,ES_TIME *es_time);

	//�洢���̴洢�춯��λ�ʷ���
	int GetGDMSYDDDetail(ES_TIME *es_time);
private:
	map<int,float> map_gdmsydd_num;//GDMS�춯��
	map<int,float> map_gpmsydd_num;//GPMS�춯��

	map<int,float> map_dmsydd_norollback_num; //dmsδ�����춯��
	map<int,float> map_ydd_total_num; //�춯����������
	
	//�ŵ����춯���÷�
	map<int,float> map_ydd_mark;
	//�ŵ����춯������ȷ��
	map<int,float> map_ydd_right;
	CDci *m_oci;
	ErrorInfo err_info;
};
#endif

