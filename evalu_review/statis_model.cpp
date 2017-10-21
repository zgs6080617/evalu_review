/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : statis_model.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 模型相关统计实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <iostream>
#include <fstream>
#include <map>
#include <stdlib.h>
#include "statis_model.h"
#include "datetime.h"
#include "es_log.h"

using namespace std;

StatisModel::StatisModel(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisModel::~StatisModel()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

int StatisModel::GetDMSModelRate(int statis_type, ES_TIME *es_time)
{
	int ret;

	Clear();
	//GetWeight();

	if(statis_type == STATIS_DAY) {	
		/*ret = GetGPZWsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;
		ret = GetGPTotalsByDay(es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = GetDevtpgoodByDay(es_time);
		if(ret < 0) {
			printf("GetDevtpgoodByDay fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevicesByDay(es_time);
		if(ret < 0) {
			printf("GetDevicesByDay fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevtpgoodByDayForUp(es_time);
		if (ret < 0) {
			printf("GetDevtpgoodByDayForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevicesByDayForUp(es_time);
		if (ret < 0) {
			printf("GetDevicesByDayForUp fail.\n");
			goto FAILED_RET;
		}

		//自动成图数据更新
		/*ret = UpdateAutograph(es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = GetAutoRnumByDay(es_time);
		if(ret < 0)
		{
			printf("GetAutoRnumByDay fail.\n");
			goto FAILED_RET;
		}

		ret = GetAutoSnumByDay(es_time);
		if(ret < 0)
		{
			printf("GetAutoSnumByDay fail.\n");
			goto FAILED_RET;
		}

		//计算
		ret = GetModelRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetModelRate fail.\n");
			goto FAILED_RET;
		}
		//模型程序不再入库地区指标，无需删除
		/*ret = DeleteModel(es_time);
		if(ret < 0)
			goto FAILED_RET;*/

		ret = GetTpdvgoodByDay(es_time);
		if(ret < 0)
		{
			printf("GetTpdvgoodByDay fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvtotalByDay(es_time);
		if(ret < 0)
		{
			printf("GetTpdvtotalByDay fail.\n");
			goto FAILED_RET;
		}
		ret = GetLoopnumByDay(es_time);
		if(ret < 0)
		{
			printf("GetLoopnumByDay fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvgoodByDayForUp(es_time);
		if (ret < 0) {
			printf("GetTpdvgoodByDayForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvtotalByDayForUp(es_time);
		if (ret < 0) {
			printf("GetTpdvtotalByDayForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetLoopnumByDayForUp(es_time);
		if (ret < 0) {
			printf("GetLoopnumByDayForUp fail.\n");
			goto FAILED_RET;
		}

		//计算
		ret = GetTPRate(statis_type, es_time);
		if(ret < 0) {
			printf("GetTPRate fail.\n");
			goto FAILED_RET;
		}

		ret = CaculateTopogoodRate();
		if (ret < 0) {
			printf("GetTPRate fail.\n");
			goto FAILED_RET;
		}

		//插入数据到统计表中
		ret = InsertResultGPZWRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultGPZWRate fail.\n");
			goto FAILED_RET;
		}

		ret = InsertResultTPgoodRate(statis_type, es_time);
		if(ret < 0) {
			printf("InsertResultTPgoodRate fail.\n");
			goto FAILED_RET;
		}

		ret = InsertTopogoodData(statis_type, es_time);
		if (ret < 0) {
			printf("InsertTopogoodData fail.\n");
			goto FAILED_RET;
		}

		//将日表中的馈线插入到月表中
		ret = InsertResultToMonth(es_time);
		if(ret < 0) {
			printf("InsertResultToMonth fail.\n");
			goto FAILED_RET;
		}

		ret = InsertResultAutoGraph(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultAutoGraph fail.\n");
			goto FAILED_RET;
		}

		/*ret = InsertAutoData(es_time);
		if(ret < 0)
			goto FAILED_RET;*/
	}
	else {

		ret = GetModelByMonth(es_time);
		if(ret < 0) {
			printf("GetModelByMonth fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevtpgoodByMonth(es_time);//
		if(ret < 0)
		{
			printf("GetDevtpgoodByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetDevicesByMonth(es_time);//
		if(ret < 0)
		{
			printf("GetDevicesByMonth fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvgoodByMonth(es_time);//
		if(ret < 0)
		{
			printf("GetTpdvgoodByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetTpdvtotalByMonth(es_time);//
		if(ret < 0)
		{
			printf("GetTpdvtotalByMonth fail.\n");
			goto FAILED_RET;
		}
		ret = GetLoopnumByMonth(es_time);
		if(ret < 0)
		{
			printf("GetLoopnumByMonth fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevtpgoodByMonthForUp(es_time);//
		if (ret < 0) {
			printf("GetDevtpgoodByMonthForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetDevicesByMonthForUp(es_time);//
		if (ret < 0) {
			printf("GetDevicesByMonthForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvgoodByMonthForUp(es_time);//
		if (ret < 0) {
			printf("GetTpdvgoodByMonthForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetTpdvtotalByMonthForUp(es_time);//
		if (ret < 0) {
			printf("GetTpdvtotalByMonthForUp fail.\n");
			goto FAILED_RET;
		}

		ret = GetLoopnumByMonthForUp(es_time);
		if (ret < 0) {
			printf("GetLoopnumByMonthForUp fail.\n");
			goto FAILED_RET;
		}

		//计算
		ret = GetModelRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetModelRate fail.\n");
			goto FAILED_RET;
		}
		ret = GetTPRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("GetTPRate fail.\n");
			goto FAILED_RET;
		}

		ret = CaculateTopogoodRate();
		if (ret < 0) {
			printf("CaculateTopogoodRate fail.\n");
			goto FAILED_RET;
		}

		//插入数据到统计表中
		ret = InsertResultGPZWRate(statis_type, es_time);
		if(ret < 0)
		{
			printf("InsertResultGPZWRate fail.\n");
			goto FAILED_RET;
		}
		ret = InsertResultTPgoodRate(statis_type, es_time);
		if(ret < 0) {
			printf("InsertResultTPgoodRate fail.\n");
			goto FAILED_RET;
		}

		ret = InsertTopogoodData(statis_type, es_time);
		if (ret < 0) {
			printf("InsertTopogoodData fail.\n");
			goto FAILED_RET;
		}
	}

	return 0;

FAILED_RET:
	ES_ERROR_INFO es_err;
	es_err.statis_type = STATIS_MODEL;
	es_err.es_time.year = es_time->year;
	es_err.es_time.month = es_time->month;
	es_err.es_time.day = es_time->day;
#ifdef DEBUG_DATE
	es_err.es_time.year = 2014;
	es_err.es_time.day = 14;
	es_err.es_time.month = 6;
#endif

	int num = m_type_err->count(statis_type);
	multimap<int, ES_ERROR_INFO>::iterator itr_type_err = m_type_err->find(statis_type);
	for (int i = 0; i < num; i++) {

		if(itr_type_err->second == es_err) {
			return -1;
		}

		itr_type_err++;
	}

	m_type_err->insert(make_pair(statis_type, es_err));

	return -1;
}

int StatisModel::GetModelProcess(ES_TIME *es_time)
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

	//sprintf(sql, "call evalusystem.result.MODELPROCESS()");
	sprintf(sql, "call evalusystem.result.MODELPROCESS('%04d%02d%02d')", byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 		plog->WriteLog(2, "模型数据处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisModel::GetGPZWsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select a.organ_code,a.info,count(*) from (select organ_code,devid,devtype,src,info,max(gtime) "
		"from evalusystem.detail.dmsbrand where src=0 group by organ_code,devid,devtype,src,info) a,"
		"(select organ_code,devid,devtype,src,max(gtime) from evalusystem.detail.dmsstatus group by "
		"organ_code,devid,devtype,src) b where a.devid=b.devid and ((a.info!=4 and b.src=3) or "
		"(a.info=4 and b.src=2)) group by a.organ_code,a.info");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/M_GPZW_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;
			map<ES_ORGAN_GP, float>::iterator itr_gp_gpzw = m_gp_gpzw.find(es_dgpms);
			if(itr_gp_gpzw != m_gp_gpzw.end())
			{
				float count = itr_gp_gpzw->second;
				count += dgpms.count;
				itr_gp_gpzw->second = count;
			}
			else
			{
				m_gp_gpzw.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_gpzw = m_organ_gpzw.find(dgpms.organ_code);
			if(itr_organ_gpzw != m_organ_gpzw.end())
			{
				float count = itr_organ_gpzw->second;
				count += dgpms.count;
				m_organ_gpzw[dgpms.organ_code] = count;
			}
			else
			{
				m_organ_gpzw.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取状态置位正确的挂牌数目失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetGPTotalsByDay(ES_TIME *es_time)
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql, "select organ_code,info,count(*) from (select organ_code,devid,devtype,src,info,max(gtime) "
		"from evalusystem.detail.dmsbrand group by organ_code,devid,devtype,src,info) a where devtype='cb' "
		"and src=0 group by organ_code,info");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1) {
		m_presult = buffer;
		ES_DATIMES dgpms;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/M_GP_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		char tmp[20];
		for (rec = 0; rec < rec_num; rec++) {

			for(col = 0; col < col_num; col++) {
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch(col) {
				case 0:
					dgpms.organ_code = *(int *)(tempstr);
					break;
				case 1:
					sprintf(tmp, "%d", *(int *)(tempstr));
					dgpms.dv = tmp;
					break;
				case 2:
					dgpms.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			ES_ORGAN_GP es_dgpms;
			es_dgpms.organ_code = dgpms.organ_code;
			es_dgpms.gp_type = dgpms.dv;

			map<ES_ORGAN_GP, float>::iterator itr_gp_gp = m_gp_gp.find(es_dgpms);
			if(itr_gp_gp != m_gp_gp.end()) {
				float count = itr_gp_gp->second;
				count += dgpms.count;
				itr_gp_gp->second = count;
			}
			else {
				m_gp_gp.insert(make_pair(es_dgpms, dgpms.count));
			}

			map<int, float>::iterator itr_organ_gp = m_organ_gp.find(dgpms.organ_code);
			if(itr_organ_gp != m_organ_gp.end()) {
				float count = itr_organ_gp->second;
				count += dgpms.count;
				m_organ_gp[dgpms.organ_code] = count;
			}
			else {
				m_organ_gp.insert(make_pair(dgpms.organ_code, dgpms.count));
			}

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<dgpms.organ_code<<"\tdv:"<<dgpms.dv<<"\tcount:"<<dgpms.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "获取挂牌总数目，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetModelByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char tmp_name[MAX_NAME_LEN];
	string table_name;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_MONTH, year, month ,day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

#if 0
	for(int i = days; i >= 1; i--)
	{
		sprintf(tmp_name, "day_%04d%02d%02d", byear, bmonth, i);
		/*sprintf(tmp_sql,"select * from evalusystem.result.%s where code='gpzwrig' or code='rigbrands' "
			"or code='brands' or code='topogood' or code='devtpgood' or code='devices' or code='topogood2' "
			"or code='tpdvgood' or code='tpdvtotal' or code='autograph' or code='dmsloopnum' or code='topogood3'",
			tmp_name);*/
		sprintf(tmp_sql,"select * from evalusystem.result.%s where code='autograph'",tmp_name);

		ret = m_oci->ReadData(tmp_sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
		if(ret == 1)
		{
			if(rec_num > 0)
			{
				table_name = tmp_name;
				m_oci->FreeReadData(col_attr, col_num, buffer);
				break;
			}
			else
			{
				if(i == 1)
					table_name = "no_data";
				m_oci->FreeColAttrData(col_attr, col_num);
				continue;
			}
		}
		else
		{
			printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "配电主站模型月统计失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			return -1;
		}		
	}

	if(table_name != "no_data")
	{
		/*sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,updatetime,"
			"statistime,dimname,dimvalue) select code,organ_code,value,datatype,updatetime,statistime,dimname,"
			"dimvalue from evalusystem.result.%s where code='gpzwrig' or code='rigbrands' or code='brands' or "
			"code='topogood' or code='devtpgood' or code='devices' or code='topogood2' or code='tpdvgood' or "
			"code='tpdvtotal' or code='dmsloopnum' or code='topogood3'", byear, bmonth, table_name.c_str());*/
		sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,updatetime,"
			"statistime,dimname,dimvalue) select code,organ_code,value,datatype,updatetime,statistime,dimname,"
			"dimvalue from evalusystem.result.%s where code='autograph'", byear, bmonth, table_name.c_str());

		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret)
		{
			printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 			plog->WriteLog(2, "配电主站月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			return -1;
		}
	}
#else
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,"
		"updatetime,statistime,dimname,dimvalue) select code,organ_code,round(avg(value),4),datatype,"
		"sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),'无','无' from (%s) where (code='autograph') "
		"and dimname='无' and datatype != '数值' group by code,organ_code,datatype", 
		byear, bmonth, byear, bmonth, bday, tmp_str.c_str());

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 			plog->WriteLog(2, "配电主站月统计入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}
#endif

	return 0;
}

int StatisModel::GetModelRate(int statis_type, ES_TIME *es_time)
{
	//挂牌与置位校验正确率
	//状态置位正确的挂牌数目 / 挂牌总数目
	map<int, float>::iterator itr_organ_gpzw;
	map<ES_ORGAN_GP, float>::iterator itr_gp_gpzw;
	map<int, float>::iterator itr_organ_gp;
	map<ES_ORGAN_GP, float>::iterator itr_gp_gp;

	//按主站
	itr_organ_gp = m_organ_gp.begin();
	for(; itr_organ_gp != m_organ_gp.end(); itr_organ_gp++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_gp->first;
		itr_organ_gpzw = m_organ_gpzw.find(itr_organ_gp->first);
		if(itr_organ_gpzw != m_organ_gpzw.end())
			es_result.m_fresult = itr_organ_gpzw->second / itr_organ_gp->second;
		else
			es_result.m_fresult = 0;

		m_gpzw_result.insert(make_pair(DIM_DMS, es_result));
	}

	//按挂牌类型
	itr_gp_gp = m_gp_gp.begin();
	for(; itr_gp_gp != m_gp_gp.end(); itr_gp_gp++)
	{
		ES_RESULT es_result;
		es_result.m_strtype = itr_gp_gp->first.gp_type;
		es_result.m_ntype = itr_gp_gp->first.organ_code;
		ES_ORGAN_GP es_organ;
		es_organ.organ_code = itr_gp_gp->first.organ_code;
		es_organ.gp_type = itr_gp_gp->first.gp_type;

		/*cout<<"chazhao:"<<es_organ.organ_code<<" "<<es_organ.gp_type<<endl;
		itr_gp_rights = m_gp_rights.begin();
		for(; itr_gp_rights != m_gp_rights.end(); itr_gp_rights++)
		{
			cout<<itr_gp_rights->first.organ_code<<" "<<itr_gp_rights->first.gp_type<<" "<<itr_gp_rights->second<<endl;
		}
		cout<<endl;*/

		itr_gp_gpzw = m_gp_gpzw.find(es_organ);
		if(itr_gp_gpzw != m_gp_gpzw.end())
			es_result.m_fresult = itr_gp_gpzw->second / itr_gp_gp->second;
		else
			es_result.m_fresult = 0;
			
		m_gpzw_result.insert(make_pair(DIM_GP, es_result));
	}

	//自动成图率
	map<int, float>::iterator itr_organ_rnum;
	map<int, float>::iterator itr_organ_snum;
	map<string, float>::iterator itr_code_weight;

	//按主站
	itr_organ_snum = m_organ_snum.begin();
	for(; itr_organ_snum != m_organ_snum.end(); itr_organ_snum++)
	{
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_snum->first;
		itr_organ_rnum = m_organ_rnum.find(itr_organ_snum->first);
		if(itr_organ_rnum != m_organ_rnum.end())
		{
			if(itr_organ_snum->second != 0)
				es_result.m_fresult = itr_organ_rnum->second / itr_organ_snum->second;
			else
				es_result.m_fresult = 0;
		}
		else
			es_result.m_fresult = 0;

		m_autograph_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_snum->first;
		itr_code_weight = m_code_weight.find("autograph");
		if(itr_code_weight != m_code_weight.end())
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		else	
			es_score.m_fresult = 0;
		m_autograph_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}
	
	return 0;
}

int StatisModel::InsertResultGPZWRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];

	col_num = 6;
	//英文标识
	strcpy(col_attr[0].col_name, "CODE");
	col_attr[0].data_type = DCI_STR;
	col_attr[0].data_size = 20;

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_CODE");
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);

	//数据值
	strcpy(col_attr[2].col_name, "VALUE");
	col_attr[2].data_type = DCI_FLT;
	col_attr[2].data_size = sizeof(float);

	//数据单位
	strcpy(col_attr[3].col_name, "DATATYPE");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 10;

	//分解维度名称
	strcpy(col_attr[4].col_name, "DIMNAME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 10;

	//分解维度取值
	strcpy(col_attr[5].col_name, "DIMVALUE");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);
	
	//挂牌与置位校验正确率按主站
	offset = 0;
	rec_num = m_gpzw_result.count(DIM_DMS);
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	std::multimap<int, ES_RESULT>::iterator itr_gpzw_result = m_gpzw_result.find(DIM_DMS);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "gpzwrig", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gpzw_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gpzw_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_gpzw_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌与置位校验正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌与置位校验正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//挂牌与置位校验正确率按挂牌类型
	offset = 0;
	rec_num = m_gpzw_result.count(DIM_GP);	
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_gpzw_result = m_gpzw_result.find(DIM_GP);
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "gpzwrig", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gpzw_result->second.m_ntype, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gpzw_result->second.m_fresult, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gpzw_result->second.m_strtype.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_gpzw_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "挂牌与置位校验正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "挂牌与置位校验正确率按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS状态置位正确的挂牌数目按主站
	offset = 0;
	rec_num = m_organ_gpzw.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gpzw = m_organ_gpzw.begin();
	for(; itr_organ_gpzw != m_organ_gpzw.end(); itr_organ_gpzw++)
	{
		memcpy(m_pdata + offset, "rigbrands", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpzw->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gpzw->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS状态置位正确的挂牌数目入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS状态置位正确的挂牌数目按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS状态置位正确的挂牌数目按挂牌类型
	offset = 0;
	rec_num = m_gp_gpzw.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_gpzw = m_gp_gpzw.begin();
	for(; itr_gp_gpzw != m_gp_gpzw.end(); itr_gp_gpzw++)
	{
		memcpy(m_pdata + offset, "rigbrands", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_gpzw->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_gpzw->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_gpzw->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS状态置位正确的挂牌数目入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS状态置位正确的挂牌数目按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS挂牌总数目按主站
	offset = 0;
	rec_num = m_organ_gp.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_gp = m_organ_gp.begin();
	for(; itr_organ_gp != m_organ_gp.end(); itr_organ_gp++)
	{
		memcpy(m_pdata + offset, "brands", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_gp->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_gp->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS挂牌总数目入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS挂牌总数目按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//DMS挂牌总数目按挂牌类型
	offset = 0;
	rec_num = m_gp_gp.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<ES_ORGAN_GP, float>::iterator itr_gp_gp = m_gp_gp.begin();
	for(; itr_gp_gp != m_gp_gp.end(); itr_gp_gp++)
	{
		memcpy(m_pdata + offset, "brands", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_gp_gp->first.organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_gp_gp->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "挂牌类型", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, itr_gp_gp->first.gp_type.c_str(), col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 			plog->WriteLog(2, "DMS挂牌总数目入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
// 		plog->WriteLog(1, "DMS挂牌总数目按挂牌类型当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

void StatisModel::Clear()
{
	m_organ_gpzw.clear();
	m_gp_gpzw.clear();
	m_organ_gp.clear();
	m_gp_gp.clear();
	m_gpzw_result.clear();
	m_organ_devtpgood.clear();
	m_organ_devices.clear();
	m_topogood_result.clear();
	m_organ_tpdvgood.clear();
	m_organ_tpdvtotal.clear();
	m_topogood2_result.clear();
	m_organ_rnum.clear();
	m_organ_snum.clear();
	m_autograph_result.clear();
	m_organ_loopnum.clear();
	m_topogood3_result.clear();
	v_hh.clear();
	m_vEsHh.clear();

	m_mOrganCode2DevtpgoodNum.clear();
	m_mOrganCode2DevicesNum.clear();
	m_mOrganCode2LoopNum.clear();
	m_mOrganCode2TpdvgoodNum.clear();
	m_mOrganCode2TpdvtotalNum.clear();

	m_mOrganCode2TpGoodRate.clear();
	m_mOrganCode2TpGood2Rate.clear();
	m_mOrganCode2TpGood3Rate.clear();

	//m_topogood_score.clear();
	//m_topogood2_score.clear();//
	//m_topogood3_score.clear();//
	//m_autograph_score.clear();

	//m_code_weight.clear();
}

int StatisModel::GetDevtpgoodByDayForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='devtpgoodup' and dimname!='无' and flag!=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2DevtpgoodNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetDevicesByDayForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='devicesup' and dimname!='无' and flag!=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2DevicesNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetLoopnumByDayForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='dmsloopnumup' and dimname!='无' group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2LoopNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvgoodByDayForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,count(*) dvgood from evalusystem.result.day_%04d%02d%02d "
		"where code='topogoodup' and dimname!='无' and value=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2TpdvgoodNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvtotalByDayForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,count(*) dvtotal from evalusystem.result.day_%04d%02d%02d "
		"where code='topogoodup' and dimname!='无' group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2TpdvtotalNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::CaculateTopogoodRate()
{
	//拓扑正确率按设备数
	map<int, float>::iterator itr_organ_devtpgood;
	map<int, float>::iterator itr_organ_devices;
	map<string, float>::iterator itr_code_weight;

	//按主站，按馈线的直接入库
	itr_organ_devices = m_mOrganCode2DevicesNum.begin();
	for (; itr_organ_devices != m_mOrganCode2DevicesNum.end(); itr_organ_devices++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_devices->first;

		itr_organ_devtpgood = m_mOrganCode2DevtpgoodNum.find(itr_organ_devices->first);
		if (itr_organ_devtpgood != m_mOrganCode2DevtpgoodNum.end()) {
			es_result.m_fresult = itr_organ_devtpgood->second / itr_organ_devices->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2TpGoodRate.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	//拓扑正确率按馈线
	map<int, float>::iterator itr_organ_tpdvgood;
	map<int, float>::iterator itr_organ_tpdvtotal;

	//按主站，按馈线的直接入库
	itr_organ_tpdvgood = m_mOrganCode2TpdvgoodNum.begin();
	for (; itr_organ_tpdvgood != m_mOrganCode2TpdvgoodNum.end(); itr_organ_tpdvgood++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_tpdvgood->first;

		itr_organ_tpdvtotal = m_mOrganCode2TpdvtotalNum.find(itr_organ_tpdvgood->first);
		if (itr_organ_tpdvtotal != m_mOrganCode2TpdvtotalNum.end()) {
			es_result.m_fresult = itr_organ_tpdvgood->second / itr_organ_tpdvtotal->second;
		}
		else
			es_result.m_fresult = 0;

		m_mOrganCode2TpGood2Rate.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	//拓扑正确率按合环数
	map<int, float>::iterator itr_organ_loopnum;

	//按主站，按馈线的直接入库
	itr_organ_devices = m_mOrganCode2DevicesNum.begin();
	for (; itr_organ_devices != m_mOrganCode2DevicesNum.end(); itr_organ_devices++) {
		ES_RESULT es_result;
		es_result.m_ntype = itr_organ_devices->first;

		itr_organ_loopnum = m_mOrganCode2LoopNum.find(itr_organ_devices->first);
		if (itr_organ_loopnum != m_mOrganCode2LoopNum.end()) {
			float tmp_loopnum = itr_organ_devices->second - itr_organ_loopnum->second;
			es_result.m_fresult = tmp_loopnum / itr_organ_devices->second;
		}
		else
			es_result.m_fresult = 1.0;

		m_mOrganCode2TpGood3Rate.insert(make_pair(es_result.m_ntype, es_result.m_fresult));
	}

	return 0;
}

int StatisModel::InsertTopogoodData(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth, lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];

	col_num = 6;
	//英文标识
	strcpy(col_attr[0].col_name, "CODE");
	col_attr[0].data_type = DCI_STR;
	col_attr[0].data_size = 20;

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_CODE");
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);

	//数据值
	strcpy(col_attr[2].col_name, "VALUE");
	col_attr[2].data_type = DCI_FLT;
	col_attr[2].data_size = sizeof(float);

	//数据单位
	strcpy(col_attr[3].col_name, "DATATYPE");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 10;

	//分解维度名称
	strcpy(col_attr[4].col_name, "DIMNAME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 10;

	//分解维度取值
	strcpy(col_attr[5].col_name, "DIMVALUE");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	for (int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if (statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d", year, month, day);
	else
		sprintf(table_name, "month_%04d%02d", year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//拓扑正确率按设备数按主站
	rec_num = m_mOrganCode2TpGoodRate.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_topogood_result = m_mOrganCode2TpGoodRate.begin();
	for (; itr_topogood_result != m_mOrganCode2TpGoodRate.end(); itr_topogood_result++) {
		memcpy(m_pdata + offset, "topogoodup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑正确的设备数按主站
	offset = 0;
	rec_num = m_mOrganCode2DevtpgoodNum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_organ_devtpgood = m_mOrganCode2DevtpgoodNum.begin();
	for (; itr_organ_devtpgood != m_mOrganCode2DevtpgoodNum.end(); itr_organ_devtpgood++) {
		memcpy(m_pdata + offset, "devtpgoodup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_devtpgood->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_devtpgood->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑设备总数按主站
	offset = 0;
	rec_num = m_mOrganCode2DevicesNum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_organ_devices = m_mOrganCode2DevicesNum.begin();
	for (; itr_organ_devices != m_mOrganCode2DevicesNum.end(); itr_organ_devices++) {
		memcpy(m_pdata + offset, "devicesup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_devices->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_devices->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑正确率按馈线按主站
	offset = 0;
	rec_num = m_mOrganCode2TpGood2Rate.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_topogood2_result = m_mOrganCode2TpGood2Rate.begin();
	for (; itr_topogood2_result != m_mOrganCode2TpGood2Rate.end(); itr_topogood2_result++) {
		memcpy(m_pdata + offset, "topogood2up", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑正确的馈线数按主站
	offset = 0;
	rec_num = m_mOrganCode2TpdvgoodNum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_organ_tpdbgood = m_mOrganCode2TpdvgoodNum.begin();
	for (; itr_organ_tpdbgood != m_mOrganCode2TpdvgoodNum.end(); itr_organ_tpdbgood++) {
		memcpy(m_pdata + offset, "tpdvgoodup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdbgood->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdbgood->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑馈线总数按主站
	offset = 0;
	rec_num = m_mOrganCode2TpdvtotalNum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_organ_tpdvtotal = m_mOrganCode2TpdvtotalNum.begin();
	for (; itr_organ_tpdvtotal != m_mOrganCode2TpdvtotalNum.end(); itr_organ_tpdvtotal++) {
		memcpy(m_pdata + offset, "tpdvtotalup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdvtotal->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdvtotal->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑正确率按合环按主站
	offset = 0;
	rec_num = m_mOrganCode2TpGood3Rate.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_topogood3_result = m_mOrganCode2TpGood3Rate.begin();
	for (; itr_topogood3_result != m_mOrganCode2TpGood3Rate.end(); itr_topogood3_result++) {
		memcpy(m_pdata + offset, "topogood3up", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	//拓扑合环数按主站
	offset = 0;
	rec_num = m_mOrganCode2LoopNum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if (m_pdata == NULL) {
		cout << "malloc error" << endl;
		return -1;
	}

	map<int, float>::iterator itr_organ_loopnum = m_mOrganCode2LoopNum.begin();
	for (; itr_organ_loopnum != m_mOrganCode2LoopNum.end(); itr_organ_loopnum++) {
		memcpy(m_pdata + offset, "dmsloopnumup", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_loopnum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_loopnum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if (rec_num > 0) {

		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if (!ret) {
			printf("WriteData: sql = %s; error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}

	FREE(m_pdata);

	return 0;
}

int StatisModel::GetDevtpgoodByMonthForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);

	if (tmp_byear != byear || tmp_bmon != bmonth) {
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}

	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "select organ_code,sum(value) from (%s) where code='devtpgoodup' and "
		"dimname='无' group by organ_code", tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);

		int rec, col, offset = 0;
		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col) {
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2DevtpgoodNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetDevicesByMonthForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);

	if (tmp_byear != byear || tmp_bmon != bmonth) {
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}

	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "select organ_code,sum(value) from (%s) where code='devicesup' and "
		"dimname='无' group by organ_code", tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2DevicesNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetLoopnumByMonthForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);

	if (tmp_byear != byear || tmp_bmon != bmonth) {
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}

	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "select organ_code,sum(value) from (%s) where code='dmsloopnumup' and "
		"dimname!='无' group by organ_code", tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2LoopNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvgoodByMonthForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if (tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "select organ_code,count(*) from (%s) where code='topogoodup' and "
		"dimname!='无' and value=1 group by organ_code", tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch (col) {
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}

				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2TpdvgoodNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvtotalByMonthForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon, tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);

	if (tmp_byear != byear || tmp_bmon != bmonth) {
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}

	days = tmp_bday;

	for (int i = 0; i < days; i++) {

		if (i == days - 1) {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
		else {
			sprintf(tmp_sql, "select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ", byear, bmonth, i + 1);
			tmp_str += tmp_sql;
		}
	}

	sprintf(sql, "select organ_code,count(*) from (%s) where code='topogoodup' and "
		"dimname!='无' group by organ_code", tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++) {

			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mOrganCode2TpdvtotalNum.insert(make_pair(mod.organ_code, mod.online));
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetModelInfo(ES_TIME *es_time)
{
	/************************************************************************/
	/*1.插入模型记录到image表                                               */
	/*2.查询当天的记录数，若记录数>0删除当天以前的数据，对比；否则不变动，不对比 */
	/************************************************************************/
	int ret;
	int year, month, day;
	int byear, bmonth, bday;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char tmp_sql[MAX_SQL_LEN];
	char *buffer = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#ifdef DEBUG_TEST
	m_oci->ExecSingle("call evalusystem.detail.TEST_PROCESS()",&err_info);
#endif

	//删除非当天数据
	//获取时间
	GetCurDate(&year, &month, &day);

	sprintf(tmp_sql,"select * from evalusystem.detail.dmsdv_image where gtime like '%04d-%02d-%02d%%'",
		year, month, day);
	ret = m_oci->ReadData(tmp_sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		if(rec_num > 0)
		{
			m_oci->FreeReadData(col_attr, col_num, buffer);
			sprintf(sql, "delete from evalusystem.detail.dmsdv_image where gtime not like '%04d-%02d-%02d%%'",
				year, month, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 				plog->WriteLog(2, "删除旧DMS馈线模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 					err_info.error_no, err_info.error_info, __FILE__, __LINE__);
				return -1;
			}
			sprintf(sql, "delete from evalusystem.detail.dmscb_image where gtime not like '%04d-%02d-%02d%%'",
				year, month, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 				plog->WriteLog(2, "删除旧DMS开关模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 					err_info.error_no, err_info.error_info, __FILE__, __LINE__);
				return -1;
			}
		}
		else
		{
			m_oci->FreeColAttrData(col_attr, col_num);
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "查询旧DMS模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(tmp_sql,"select * from evalusystem.detail.gpmsdv_image where gtime like '%04d-%02d-%02d%%'",
		year, month, day);
	ret = m_oci->ReadData(tmp_sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		if(rec_num > 0)
		{
			m_oci->FreeReadData(col_attr, col_num, buffer);
			sprintf(sql, "delete from evalusystem.detail.gpmsdv_image where gtime not like '%04d-%02d-%02d%%'",
				year, month, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 				plog->WriteLog(2, "删除旧GPMS馈线模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 					err_info.error_no, err_info.error_info, __FILE__, __LINE__);
				return -1;
			}
			sprintf(sql, "delete from evalusystem.detail.gpmscb_image where gtime not like '%04d-%02d-%02d%%'",
				year, month, day);
			ret = m_oci->ExecSingle(sql,&err_info);
			if(!ret)
			{
				printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 				plog->WriteLog(2, "删除旧GPMS开关模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 					err_info.error_no, err_info.error_info, __FILE__, __LINE__);
				return -1;
			}
		}
		else
		{
			m_oci->FreeColAttrData(col_attr, col_num);
		}
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
// 		plog->WriteLog(2, "查询旧GPMS模型数据失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}
#if 0
	//设备列表去重
	sprintf(sql, "call evalusystem.detail.CHECK_DMS_MODEL()");

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 		plog->WriteLog(2, "DMS模型数据对比处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "call evalusystem.detail.CHECK_GPMS_MODEL()");

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
// 		plog->WriteLog(2, "GPMS模型数据对比处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}
#endif
//#ifdef DEBUG_TEST
//	m_oci->ExecSingle("call evalusystem.detail.TEST_PROCESS()",&err_info);
//#endif

	return 0;
}

int StatisModel::GetDevtpgoodByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='devtpgood' and dimname!='无' and flag!=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_devtpgood_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_devtpgood.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑正确设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetDevicesByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='devices' and dimname!='无' and flag!=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_devices_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_devices.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetLoopnumByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.day_%04d%02d%02d where "
		"code='dmsloopnum' and dimname!='无' group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_dmsloopnum_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_loopnum.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetLoopnumByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#if 0
	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.month_%04d%02d where "
		"code='dmsloopnum' and dimname!='无' group by organ_code", byear, bmonth);
#else
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='dmsloopnum' and "
		"dimname!='无' group by organ_code",tmp_str.c_str());
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_dmsloopnum_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_loopnum.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::DeleteModel(ES_TIME *es_time)
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

	sprintf(sql, "delete from evalusystem.result.day_%04d%02d%02d where (code='devtpgood' and dimname='无') "
		"or (code='devices' and dimname='无') or (code='topogood' and dimname='无')", byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "模型冗余数据删除失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisModel::GetTpdvgoodByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,count(*) dvgood from evalusystem.result.day_%04d%02d%02d "
		"where code='topogood' and dimname!='无' and value=1 group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_tpdvgood_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_tpdvgood.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑正确馈线数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvtotalByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select organ_code,count(*) dvtotal from evalusystem.result.day_%04d%02d%02d "
		"where code='topogood' and dimname!='无' group by organ_code", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_tpdvtotal_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_tpdvtotal.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑馈线总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTPRate(int statis_type, ES_TIME *es_time)
{
	//拓扑正确率按设备数
	map<int, float>::iterator itr_organ_devtpgood;
	map<int, float>::iterator itr_organ_devices;
	map<string, float>::iterator itr_code_weight;

	//按主站，按馈线的直接入库
	itr_organ_devices = m_organ_devices.begin();
	for(; itr_organ_devices != m_organ_devices.end(); itr_organ_devices++) {
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_devices->first;

		itr_organ_devtpgood = m_organ_devtpgood.find(itr_organ_devices->first);
		if(itr_organ_devtpgood != m_organ_devtpgood.end()) {
			es_result.m_fresult = itr_organ_devtpgood->second / itr_organ_devices->second;
		}
		else
			es_result.m_fresult = 0;

		m_topogood_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_devices->first;
		itr_code_weight = m_code_weight.find("topogood");
		if(itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;
		m_topogood_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}
	
	//拓扑正确率按馈线
	map<int, float>::iterator itr_organ_tpdvgood;
	map<int, float>::iterator itr_organ_tpdvtotal;

	//按主站，按馈线的直接入库
	itr_organ_tpdvtotal = m_organ_tpdvtotal.begin();
	for(; itr_organ_tpdvtotal != m_organ_tpdvtotal.end(); itr_organ_tpdvtotal++) {
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_tpdvtotal->first;

		itr_organ_tpdvgood = m_organ_tpdvgood.find(itr_organ_tpdvtotal->first);
		if(itr_organ_tpdvgood != m_organ_tpdvgood.end()) {
			es_result.m_fresult = itr_organ_tpdvgood->second / itr_organ_tpdvtotal->second;
		}
		else
			es_result.m_fresult = 0;

		m_topogood2_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_devices->first;
		itr_code_weight = m_code_weight.find("topogood2");
		if(itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;
		m_topogood2_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	//拓扑正确率按合环数
	map<int, float>::iterator itr_organ_loopnum;

	//按主站，按馈线的直接入库
	itr_organ_devices = m_organ_devices.begin();
	for(; itr_organ_devices != m_organ_devices.end(); itr_organ_devices++) {
		ES_RESULT es_result;
		//ES_RESULT es_score;
		es_result.m_ntype = itr_organ_devices->first;

		itr_organ_loopnum = m_organ_loopnum.find(itr_organ_devices->first);
		if(itr_organ_loopnum != m_organ_loopnum.end()) {
			float tmp_loopnum = itr_organ_devices->second - itr_organ_loopnum->second;
			es_result.m_fresult = tmp_loopnum / itr_organ_devices->second;
		}
		else
			es_result.m_fresult = 1.0;

		m_topogood3_result.insert(make_pair(es_result.m_ntype, es_result.m_fresult));

		/*es_score.m_ntype = itr_organ_devices->first;
		itr_code_weight = m_code_weight.find("topogood3");
		if(itr_code_weight != m_code_weight.end())
		{
			es_score.m_fresult = es_result.m_fresult*itr_code_weight->second;
		}
		else	
			es_score.m_fresult = 0;
		m_topogood3_score.insert(make_pair(es_score.m_ntype,es_score.m_fresult));*/
	}

	return 0;
}

int StatisModel::InsertResultTPgoodRate(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];

	col_num = 6;
	//英文标识
	strcpy(col_attr[0].col_name, "CODE");
	col_attr[0].data_type = DCI_STR;
	col_attr[0].data_size = 20;

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_CODE");
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);

	//数据值
	strcpy(col_attr[2].col_name, "VALUE");
	col_attr[2].data_type = DCI_FLT;
	col_attr[2].data_size = sizeof(float);

	//数据单位
	strcpy(col_attr[3].col_name, "DATATYPE");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 10;

	//分解维度名称
	strcpy(col_attr[4].col_name, "DIMNAME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 10;

	//分解维度取值
	strcpy(col_attr[5].col_name, "DIMVALUE");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//拓扑正确率按设备数按主站
	rec_num = m_topogood_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_topogood_result = m_topogood_result.begin();
	for(; itr_topogood_result != m_topogood_result.end(); itr_topogood_result++)
	{
		memcpy(m_pdata + offset, "topogood", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_topogood_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_topogood_result = m_topogood_score.begin();
	for(; itr_topogood_result != m_topogood_score.end(); itr_topogood_result++)
	{
		memcpy(m_pdata + offset, "topogoods", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//拓扑正确的设备数按主站
	offset = 0;
	rec_num = m_organ_devtpgood.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_devtpgood = m_organ_devtpgood.begin();
	for(; itr_organ_devtpgood != m_organ_devtpgood.end(); itr_organ_devtpgood++)
	{
		memcpy(m_pdata + offset, "devtpgood", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_devtpgood->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_devtpgood->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确的设备数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确的设备数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//拓扑设备总数按主站
	offset = 0;
	rec_num = m_organ_devices.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_devices = m_organ_devices.begin();
	for(; itr_organ_devices != m_organ_devices.end(); itr_organ_devices++)
	{
		memcpy(m_pdata + offset, "devices", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_devices->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_devices->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑设备总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑设备总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);
	
	//拓扑正确率按馈线按主站
	offset = 0;
	rec_num = m_topogood2_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_topogood2_result = m_topogood2_result.begin();
	for(; itr_topogood2_result != m_topogood2_result.end(); itr_topogood2_result++)
	{
		memcpy(m_pdata + offset, "topogood2", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率按馈线入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按馈线按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_topogood2_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_topogood2_result = m_topogood2_score.begin();
	for(; itr_topogood2_result != m_topogood2_score.end(); itr_topogood2_result++)
	{
		memcpy(m_pdata + offset, "topogood2s", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood2_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率按馈线入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按馈线按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//拓扑正确的馈线数按主站
	offset = 0;
	rec_num = m_organ_tpdvgood.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_tpdbgood = m_organ_tpdvgood.begin();
	for(; itr_organ_tpdbgood != m_organ_tpdvgood.end(); itr_organ_tpdbgood++)
	{
		memcpy(m_pdata + offset, "tpdvgood", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdbgood->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdbgood->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确的馈线数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确的馈线数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//拓扑馈线总数按主站
	offset = 0;
	rec_num = m_organ_tpdvtotal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_tpdvtotal = m_organ_tpdvtotal.begin();
	for(; itr_organ_tpdvtotal != m_organ_tpdvtotal.end(); itr_organ_tpdvtotal++)
	{
		memcpy(m_pdata + offset, "tpdvtotal", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdvtotal->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_tpdvtotal->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑馈线总数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑馈线总数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//拓扑正确率按合环按主站
	offset = 0;
	rec_num = m_topogood3_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_topogood3_result = m_topogood3_result.begin();
	for(; itr_topogood3_result != m_topogood3_result.end(); itr_topogood3_result++)
	{
		memcpy(m_pdata + offset, "topogood3", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率按合环入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按合环按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_topogood3_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_topogood3_result = m_topogood3_score.begin();
	for(; itr_topogood3_result != m_topogood3_score.end(); itr_topogood3_result++)
	{
		memcpy(m_pdata + offset, "topogood3s", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_topogood3_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑正确率按合环入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑正确率按合环按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	//拓扑合环数按主站
	offset = 0;
	rec_num = m_organ_loopnum.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_organ_loopnum = m_organ_loopnum.begin();
	for(; itr_organ_loopnum != m_organ_loopnum.end(); itr_organ_loopnum++)
	{
		memcpy(m_pdata + offset, "dmsloopnum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_organ_loopnum->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_organ_loopnum->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "拓扑合环数入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "拓扑合环数按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisModel::InsertResultToMonth(ES_TIME *es_time)
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

	//删除月指标当日模型数据
	sprintf(sql, "delete from evalusystem.result.month_%04d%02d where statistime='%04d-%02d-%02d 00:00:00' "
		"and dimname!='无' and (code='topogood' or code='devtpgood' or code='devices' or code='topogood2' or "
		"code='tpdvgood' or code='tpdvtotal' or code='dmsloopnum' or code='topogood3' or code='topogoodup' or "
		"code='devtpgoodup' or code='devicesup' or code='topogood2up' or "
		"code='tpdvgoodup' or code='tpdvtotalup' or code='dmsloopnumup' or code='topogood3up')",
		byear, bmonth, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "拓扑正确率月入库处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	//向月表中插入数据
	sprintf(sql, "insert into evalusystem.result.month_%04d%02d(code,organ_code,value,datatype,updatetime,"
		"statistime,dimname,dimvalue) select code,organ_code,value,datatype,sysdate,statistime,dimname,"
		"dimvalue from evalusystem.result.day_%04d%02d%02d where dimname!='无' and (code='topogood' or "
		"code='devtpgood' or code='devices' or code='topogood2' or code='tpdvgood' or code='tpdvtotal' or "
		"code='dmsloopnum' or code='topogood3' or code='topogoodup' or "
		"code='devtpgoodup' or code='devicesup' or code='topogood2up' or code='tpdvgoodup' or code='tpdvtotalup' or "
		"code='dmsloopnumup' or code='topogood3up')", byear, bmonth, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret) {
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "拓扑正确率月入库处理失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisModel::GetDevtpgoodByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#if 0
	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.month_%04d%02d where "
		"code='devtpgood' and dimname!='无' group by organ_code", byear, bmonth);
#else
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='devtpgood' and "
		"dimname='无' group by organ_code",tmp_str.c_str());
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_devtpgood_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_devtpgood.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑正确设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetDevicesByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#if 0
	sprintf(sql, "select organ_code,sum(value) from evalusystem.result.month_%04d%02d where "
		"code='devices' and dimname!='无' group by organ_code", byear, bmonth);
#else
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='devices' and "
		"dimname='无' group by organ_code",tmp_str.c_str());
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_devices_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_devices.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型设备数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvgoodByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#if 0
	sprintf(sql, "select organ_code,count(*) from evalusystem.result.month_%04d%02d where "
		"code='topogood' and dimname!='无' and value=1 group by organ_code", byear, bmonth);
#else
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,count(*) from (%s) where code='topogood' and "
		"dimname!='无' and value=1 group by organ_code",tmp_str.c_str());
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_tpdvgood_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_tpdvgood.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑正确馈线数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetTpdvtotalByMonth(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN * 10];
	char tmp_sql[MAX_SQL_LEN * 10];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

#if 0
	sprintf(sql, "select organ_code,count(*) from evalusystem.result.month_%04d%02d where "
		"code='topogood' and dimname!='无' group by organ_code", byear, bmonth);
#else
	int tmp_byear, tmp_bmon, tmp_bday;
	int tmp_year, tmp_mon, tmp_day;
	int days;
	GetCurDate(&tmp_year, &tmp_mon, &tmp_day);
	GetBeforeTime(STATIS_MONTH, tmp_year, tmp_mon ,tmp_day, &tmp_byear, &tmp_bmon, &tmp_bday);
	if(tmp_byear != byear || tmp_bmon != bmonth)
	{
		tmp_bday = GetDaysInMonth(byear, bmonth);
	}
	days = tmp_bday;

	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where flag != 1 union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,count(*) from (%s) where code='topogood' and "
		"dimname!='无' group by organ_code",tmp_str.c_str());
#endif

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Model_tpdvtotal_Month.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_tpdvtotal.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取模型拓扑馈线总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::UpdateAutograph(ES_TIME *es_time)
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

	sprintf(sql, "update evalusystem.detail.autograph d set d.rnum=c.rnum from (select a.organ_code,a.rnum "
		"from evalusystem.detail.autograph a,(select organ_code,max(count_time) maxtime from "
		"evalusystem.detail.autograph where rnum!=0 and count_time<to_date('%04d-%02d-%02d','YYYY-MM-DD') "
		"group by organ_code) b where a.organ_code=b.organ_code and a.count_time=b.maxtime) c where "
		"d.organ_code=c.organ_code and d.rnum=0 and d.count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD')",
		byear, bmonth, bday, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "自动成图表数据更新失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	sprintf(sql, "update evalusystem.detail.autograph d set d.snum=c.snum from (select a.organ_code,a.snum "
		"from evalusystem.detail.autograph a,(select organ_code,max(count_time) maxtime from "
		"evalusystem.detail.autograph where snum!=0 and count_time<to_date('%04d-%02d-%02d','YYYY-MM-DD') "
		"group by organ_code) b where a.organ_code=b.organ_code and a.count_time=b.maxtime) c where "
		"d.organ_code=c.organ_code and d.snum=0 and d.count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD')",
		byear, bmonth, bday, byear, bmonth, bday);

	ret = m_oci->ExecSingle(sql,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
		// 		plog->WriteLog(2, "自动成图表数据更新失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	return 0;
}

int StatisModel::GetAutoRnumByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select distinct(organ_code),rnum from evalusystem.detail.autograph where "
		"count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD')", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Autograph_Rnum_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_rnum.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取自动成图率已成环网数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::GetAutoSnumByDay(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql, "select distinct(organ_code),snum from evalusystem.detail.autograph where "
		"count_time=to_date('%04d-%02d-%02d','YYYY-MM-DD')", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DMSRUNNING mod;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/Autograph_Snum_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					mod.organ_code = *(int *)(tempstr);
					break;
				case 1:
					mod.online = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_organ_snum.insert(make_pair(mod.organ_code, mod.online));

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取自动成图率应成环网数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::InsertResultAutoGraph(int statis_type, ES_TIME *es_time)
{
	int year, month, day;
	int lyear, lmonth, lday;
	lyear = es_time->year;
	lmonth = es_time->month;
	lday = es_time->day;
	GetBeforeTime(STATIS_DAY, lyear, lmonth ,lday, &year, &month, &day);

	int ret;
	int offset = 0;
	int col_num;
	int rec_num;
	int m_ndata_len = 0;
	char *m_pdata = NULL;
	ColAttr col_attr[6];
	char sql[MAX_SQL_LEN];
	char table_name[MAX_NAME_LEN];

	col_num = 6;
	//英文标识
	strcpy(col_attr[0].col_name, "CODE");
	col_attr[0].data_type = DCI_STR;
	col_attr[0].data_size = 20;

	//供电公司编码
	strcpy(col_attr[1].col_name, "ORGAN_CODE");
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);

	//数据值
	strcpy(col_attr[2].col_name, "VALUE");
	col_attr[2].data_type = DCI_FLT;
	col_attr[2].data_size = sizeof(float);

	//数据单位
	strcpy(col_attr[3].col_name, "DATATYPE");
	col_attr[3].data_type = DCI_STR;
	col_attr[3].data_size = 10;

	//分解维度名称
	strcpy(col_attr[4].col_name, "DIMNAME");
	col_attr[4].data_type = DCI_STR;
	col_attr[4].data_size = 10;

	//分解维度取值
	strcpy(col_attr[5].col_name, "DIMVALUE");
	col_attr[5].data_type = DCI_STR;
	col_attr[5].data_size = 50;

	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",year, month, day);
	else
		sprintf(table_name, "month_%04d%02d",year, month);

	sprintf(sql, "insert into evalusystem.result.%s(code,organ_code,value,datatype,updatetime,statistime,dimname,"
		"dimvalue) values(:1,:2,round(:3,4),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//自动成图率按主站
	rec_num = m_autograph_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	map<int, float>::iterator itr_autograph_result = m_autograph_result.begin();
	for(; itr_autograph_result != m_autograph_result.end(); itr_autograph_result++)
	{
		memcpy(m_pdata + offset, "autograph", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_autograph_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_autograph_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "自动成图率按主站入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "自动成图率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//得分
	/*offset = 0;
	rec_num = m_autograph_score.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_autograph_result = m_autograph_score.begin();
	for(; itr_autograph_result != m_autograph_score.end(); itr_autograph_result++)
	{
		memcpy(m_pdata + offset, "autographs", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_autograph_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_autograph_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "数值", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "自动成图率按主站入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "自动成图率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);*/

	return 0;
}

int StatisModel::ModelCheck(ES_TIME *es_time)
{
	int ret;
	int year, month, day;
	int byear, bmonth, bday;
	char cmd_dms[MAX_SQL_LEN];
	char cmd_gpms[MAX_SQL_LEN];
	char cmd[MAX_SQL_LEN];

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//sprintf(cmd_dms, "/home/d5000/fujian/src/bin/modelmerge/cimver 0 2458_eq.xml %04d%02d%02d",
	//	byear, bmonth, bday);
	//sprintf(cmd_gpms, "/home/d5000/fujian/src/bin/modelmerge/cimver 1 2458_eq.xml");

	sprintf(cmd, "/home/d5000/fujian/src/bin/modelmerge/file_scan 1 %04d%02d%02d > testmodel.log",
		byear, bmonth, bday);

	//获取模型数据
	//ret = system(cmd_dms);
	//ret = system(cmd_gpms);
	int hour,mini,sec;
	GetCurTime(&hour,&mini,&sec);
	FILE *fp = fopen("model.log","a+");
	fprintf(fp,"%d:%d:%d:cmd:%s\n",hour,mini,sec,cmd);
	fflush(fp);
	ret = system(cmd);
	fprintf(fp,"%d:%d:%d:Exc END!\n",hour,mini,sec);
	fclose(fp);
	return 0;
}

int StatisModel::getHHStatus(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	sprintf(sql,"select a.devid,a.reason from (select dv,devid,reason from evalusystem.result.tpzq where "
		"count_time = to_date('%04d-%02d-%02d','YYYY-MM-DD') and (devtype = 'dis' or devtype = 'cb') and "
		"reason like '%合环%') a,evalusystem.detail.dmscb b where a.devid = b.cb and (b.status = 0 or "
		"b.status = 30 or b.status = 40 or b.status = 42) order by a.devid;",byear,bmonth,bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_HH hh;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/ES_HH.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					hh.dv = string(tempstr);
					break;
				case 1:
					hh.reason = string(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			v_hh.push_back(hh);

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<mod.organ_code<<"\t:"<<mod.online<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取自动成图率应成环网数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::DeleteHH(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	vector<ES_HH>::iterator itr_vechh = v_hh.begin();
	for (;itr_vechh != v_hh.end();itr_vechh++) {
		sprintf(sql,"DELETE FROM EVALUSYSTEM.RESULT.TPZQ WHERE COUNT_TIME = TO_DATE('%04d-%02d-%02d','YYYY-MM-DD')"
			"AND REASON = '%s' AND DV = '%s';",byear,bmonth,bday,itr_vechh->reason.c_str(),itr_vechh->dv.c_str());

		ret = m_oci->ExecSingle(sql,&err_info);
		if(!ret) {
			printf("DELETE　TPZQ: error(%d) = %s;\n",err_info.error_no,err_info.error_info);
		}
	}
	return 0;
}

int StatisModel::getHHStatusForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	sprintf(sql, "select a.devid,a.reason from (select dv,devid,reason from evalusystem.result.tpzq where "
		"count_time = to_date('%04d-%02d-%02d','YYYY-MM-DD') and (devtype = 'dis' or devtype = 'cb') and "
		"reason like '%合环%') a,evalusystem.detail.dmscbup b where a.devid = b.cb and (b.status = 0 or "
		"b.status = 30 or b.status = 40 or b.status = 42) order by a.devid;", byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if (ret == 1) {
		m_presult = buffer;
		ES_HH hh;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;

		for (rec = 0; rec < rec_num; rec++)
		{
			for (col = 0; col < col_num; col++) {
				memset(tempstr, 0, 300);
				memcpy(tempstr, m_presult, col_attr[col].data_size);

				switch (col) {
				case 0:
					hh.dv = string(tempstr);
					break;
				case 1:
					hh.reason = string(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_vEsHh.push_back(hh);
		}
	}
	else {
		printf("sql:%s, ReadData: error(%d) = %s;\n", sql, err_info.error_no, err_info.error_info);
		return -1;
	}

	if (rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int StatisModel::DeleteHHForUp(ES_TIME *es_time)
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month, day, &byear, &bmonth, &bday);

	vector<ES_HH>::iterator itr_vechh = m_vEsHh.begin();
	for (; itr_vechh != m_vEsHh.end(); itr_vechh++) {
		sprintf(sql, "DELETE FROM EVALUSYSTEM.RESULT.TPZQUP WHERE COUNT_TIME = TO_DATE('%04d-%02d-%02d','YYYY-MM-DD')"
			"AND REASON = '%s' AND DV = '%s';", byear, bmonth, bday, itr_vechh->reason.c_str(), itr_vechh->dv.c_str());

		ret = m_oci->ExecSingle(sql, &err_info);
		if (!ret) {
			printf("DELETE　TPZQ: error(%d) = %s;\n", err_info.error_no, err_info.error_info);
		}
	}

	return 0;
}

/*int StatisModel::GetWeight()
{
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;


	sprintf(sql,"select code,weight from evalusystem.config.weight;");

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_WEIGHT weight;
		char *tempstr = (char *)malloc(30);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/MODEL_WEIGHT_Day.txt");
		ofstream outfile(file_name);
		if (!outfile.is_open())
			cout<<"open file error"<<endl;
#endif
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 30);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					weight.code = string(tempstr);
					break;
				case 1:
					weight.weight = *(double *)(tempstr)*100;
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_code_weight.insert(make_pair(weight.code, weight.weight));
#ifdef DEBUG
			outfile<<++line<<"\tcode:"<<weight.code<<"\tweight:"<<weight.weight<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取DMS配变总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}*/
//int StatisModel::InsertAutoData(ES_TIME *es_time)
//{
//	int year, month, day;
//	int byear, bmonth, bday;
//	int ret;
//	char sql[MAX_SQL_LEN];
//
//	//获取时间
//	year = es_time->year;
//	month = es_time->month;
//	day = es_time->day;
//	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);
//
//	sprintf(sql, "insert into evalusystem.result.day_%04d%02d%02d(code,organ_code,value,datatype,updatetime,"
//		"statistime,dimname,dimvalue) select 'autograph',organ_code,1,'百分比',"
//		"to_date('%04d-%02d-%02d','YYYY-MM-DD'),to_date('%04d-%02d-%02d','YYYY-MM-DD'),"
//		"'无','无' from (select organ_code from evalusystem.config.organ)",
//		byear, bmonth, bday, byear, bmonth, bday, byear, bmonth, bday);
//
//	ret = m_oci->ExecSingle(sql,&err_info);
//	if(!ret)
//	{
//		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql);
//		// 		plog->WriteLog(2, "自动成图率插入失败，error(%d) = %s, 文件名：%s, 行号：%d",
//		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
//		return -1;
//	}
//
//	return 0;
//}
