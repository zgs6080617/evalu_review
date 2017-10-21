#include "statis_district.h"



StatisDistrict::StatisDistrict(std::multimap<int, ES_ERROR_INFO> *mul_type_err)
{
	m_type_err = mul_type_err;
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisDistrict::~StatisDistrict()
{
	Clear();
	m_oci->DisConnect(&err_info);
}

int StatisDistrict::ReadDataByCode( int statis_type, ES_TIME *es_time )
{
	Clear();
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	char sql[MAX_SQL_LEN];
	char table_name[20];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	char tempstr[10000];

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	if(statis_type == STATIS_DAY)
		sprintf(table_name, "day_%04d%02d%02d",byear, bmonth, bday);
	else
		sprintf(table_name, "month_%04d%02d",byear, bmonth);

	sprintf(sql, "SELECT A.O,A.C+'Aver',A.V,B.V FROM "
		"(SELECT A.ORGAN_CODE O,SUM(A.VALUE) V,B.CODE C FROM "
		"(select * from RESULT.%s where datatype in( '个数','小时' ) and dimname = '无') A "
		"inner join "
		"(SELECT * FROM CONFIG.AVERALL WHERE FLAG = 2) B "
		"ON  A.CODE = B.DIV+'All' GROUP BY A.ORGAN_CODE,B.CODE) A "
		"INNER JOIN "
		"(SELECT A.ORGAN_CODE O,SUM(A.VALUE) V,B.CODE C   FROM "
		"(select * from RESULT.%s where datatype in( '个数','小时' ) and dimname = '无') A "
		"inner join "
		"(SELECT * FROM CONFIG.AVERALL WHERE FLAG = 2) B "
		"ON  A.CODE = B.DIVEND+'All' GROUP BY A.ORGAN_CODE,B.CODE) B "
		"ON A.C = B.C AND A.O = B.O;",
		table_name,table_name);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DISTRICT distrinct;
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
			memset(&distrinct,0x00,sizeof(ES_DISTRICT));
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0 , 10000);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					distrinct.organ_code = *(int*)(tempstr);
					break;
				case 1:
					strncpy(distrinct.code,tempstr,sizeof(distrinct.code)-1);
					break;
				case 2:
					distrinct.div = *(double *)(tempstr);
					break;
				case 3:
					distrinct.divend = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_vDistrict.push_back(distrinct);

#ifdef DEBUG
			outfile<<++line<<"\torgan_code:"<<distrinct.organ_code<<"\t:"<<distrinct.online<<endl;
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

int StatisDistrict::GetDistrictRate()
{
	vector<ES_DISTRICT>::iterator it = m_vDistrict.begin();
	for (;it != m_vDistrict.end();++it) {

		if (it->divend == 0) {

			it->result = 1.0;
		}
		else {

			it->result = it->div/it->divend;
		}

		it->value = it->result * 100;
	}

	return 0;
}

int StatisDistrict::InsertIntoResult( int statis_type, ES_TIME *es_time )
{
	if (m_vDistrict.size() == 0) {

		return 0;
	}

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
	vector<ES_DISTRICT>::iterator itr_able_result;
	map<string, int>::iterator itr_srcdv_organ;

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

	rec_num = m_vDistrict.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_able_result = m_vDistrict.begin();
	for(;itr_able_result != m_vDistrict.end();++itr_able_result)
	{
		memcpy(m_pdata + offset, itr_able_result->code, col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_able_result->organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_able_result->result, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "DA可用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DA可用率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//数值
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	offset = 0;
	itr_able_result = m_vDistrict.begin();
	for(;itr_able_result != m_vDistrict.end();++itr_able_result)
	{
		memcpy(m_pdata + offset, itr_able_result->code, col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_able_result->organ_code, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_able_result->value, col_attr[2].data_size);
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
			// 			plog->WriteLog(2, "DA可用率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "DA可用率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int StatisDistrict::CaculateDistrict( int statis_type, ES_TIME *es_time )
{
	if (ReadDataByCode(statis_type,es_time) != 0) {

		printf("ReadDataByCode fail\n");
		return -1;
	}

	if (GetDistrictRate() != 0) {

		printf("GetDistrictRate fail\n");
		return -1;
	}

	if (InsertIntoResult(statis_type,es_time) != 0) {

		printf("InsertIntoResult fail\n");
		return -1;
	}

	return 0;
}

void StatisDistrict::Clear()
{
	m_vDistrict.clear();
}
