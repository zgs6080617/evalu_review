#ifndef _STATIS_DISTRICT
#define _STATIS_DISTRICT

#include <iostream>
#include <map>
#include <string>
#include <vector>
#include "datetime.h"
#include "es_define.h"

class StatisDistrict
{
public:
	StatisDistrict(std::multimap<int, ES_ERROR_INFO> *mul_type_err);
	~StatisDistrict();

	/************************************************************************/
	/* ��ȡ��������Ϊ���ӱȷ�ĸcode��ֵ                                        */ 
	/************************************************************************/
	int ReadDataByCode(int statis_type, ES_TIME *es_time);

	/************************************************************************/
	/* ��ȡָ��ֵ                                                                */ 
	/************************************************************************/
	int GetDistrictRate();

	/************************************************************************/
	/* ������                                                                */ 
	/************************************************************************/
	int InsertIntoResult(int statis_type, ES_TIME *es_time);

	/************************************************************************/
	/* �ۺ�                                                                */ 
	/************************************************************************/
	int CaculateDistrict(int statis_type, ES_TIME *es_time);

protected:

	void Clear();
private:
	CDci *m_oci;
	ErrorInfo err_info;

	std::vector<ES_DISTRICT> m_vDistrict;

	std::multimap<int, ES_ERROR_INFO> *m_type_err;
};

#endif

