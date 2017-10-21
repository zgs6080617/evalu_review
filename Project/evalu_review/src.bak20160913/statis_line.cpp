#include "statis_line.h"



int STATISLINES::ReadDmsLinesByDay( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN];
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

	//线路总条数
	sprintf(sql, "SELECT ORGAN.ORGAN_CODE,DECODE(A.C,NULL,0,A.C) FROM (SELECT ORGAN_CODE,COUNT(*) C "
		"FROM (SELECT DISTINCT ORGAN_CODE,LINEID FROM DETAIL.DMSLINE  WHERE COUNT_TIME = '%04d-%02d-%02d') GROUP BY ORGAN_CODE) A"
		" RIGHT JOIN CONFIG.ORGAN ORGAN ON(A.ORGAN_CODE = ORGAN.ORGAN_CODE);",byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Day.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mlines.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//挂接故障指示器线路总条数
	sprintf(sql, "SELECT ORGAN_CODE,COUNT(*) FROM (SELECT DISTINCT ORGAN_CODE,LINEID FROM EVALUSYSTEM.DETAIL.DMSLINE WHERE COUNT_TIME = '%04d-%02d-%02d' AND INDICATOR != 0) GROUP BY ORGAN_CODE;",byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Day.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mindacator.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//挂接二遥终端线路总条数
	sprintf(sql, "SELECT ORGAN_CODE,COUNT(*) FROM (SELECT DISTINCT ORGAN_CODE,LINEID FROM DETAIL.DMSLINE WHERE COUNT_TIME = '%04d-%02d-%02d' AND TWORATIO != 0) GROUP BY ORGAN_CODE;",byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Day.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mtwoterminal.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//挂接三遥终端线路总条数
	sprintf(sql, "SELECT ORGAN_CODE,COUNT(*) FROM (SELECT DISTINCT ORGAN_CODE,LINEID FROM DETAIL.DMSLINE WHERE COUNT_TIME = '%04d-%02d-%02d' AND THREERATIO != 0) GROUP BY ORGAN_CODE;",byear, bmonth, bday);

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Terminals_Day.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(int *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mthreeterminal.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tcount:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端总数失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	return 0;
}

int STATISLINES::ReadDmsLinesByMonth( ES_TIME *es_time )
{
	int year, month, day;
	int byear, bmonth, bday;
	int ret, num;
	int rec_num;
	int col_num;
	float m_fresult;
	char sql[MAX_SQL_LEN * 6];
	char tmp_sql[MAX_SQL_LEN * 2];
	string tmp_str;
	char *buffer = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	//获取时间
	year = es_time->year;
	month = es_time->month;
	day = es_time->day;
	GetBeforeTime(STATIS_DAY, year, month ,day, &byear, &bmonth, &bday);

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
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'incovline' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'incovline' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,avg(value) from (%s) where code='incovline' and dimname='无' and datatype = '百分比' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mindacator_result.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);


	//线路二遥终端覆盖率
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'twocovline' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'twocovline' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,avg(value) from (%s) where code='twocovline' and dimname='无' and datatype = '百分比' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mtwo_result.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//线路三遥终端覆盖率
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'threecovline' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '百分比' "
				" and code = 'threecovline' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,avg(value) from (%s) where code='threecovline' and dimname='无' and datatype = '百分比' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mthree_result.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	/*//挂接故指线路条数
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'incovlinenum' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'incovlinenum' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='incovlinenum' and dimname='无' and datatype = '个数' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mindacator.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//挂接二遥终端线路条数
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'twocovlinenum' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'twocovlinenum' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='twocovlinenum' and dimname='无' and datatype = '个数' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mtwoterminal.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//挂接三遥终端线路条数
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'threecovlinenum' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'threecovlinenum' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='threecovlinenum' and dimname='无' and datatype = '个数' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mthreeterminal.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);

	//线路总条数
	tmp_str.clear();
	for(int i = 0; i < days; i++)
	{
		if(i == days - 1)
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'linenum' ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
		else
		{
			sprintf(tmp_sql,"select * from evalusystem.result.day_%04d%02d%02d where dimname = '无' and datatype = '个数' "
				" and code = 'linenum' union all ",byear, bmonth, i+1);
			tmp_str += tmp_sql;
		}
	}
	sprintf(sql, "select organ_code,sum(value) from (%s) where code='linenum' and dimname='无' and datatype = '个数' "
		"group by organ_code",tmp_str.c_str());

	ret = m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buffer, &err_info);
	if(ret == 1)
	{
		m_presult = buffer;
		ES_DATIMES da;
		char *tempstr = (char *)malloc(300);
		int rec, col, offset = 0;
#ifdef DEBUG
		int line = 0;
		char file_name[1024];
		sprintf(file_name,"/home/d5000/fujian/src/platform/evalu_review/bin/T_Offline_Month.txt");
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
					da.organ_code = *(int *)(tempstr);
					break;
				case 1:
					da.count = *(double *)(tempstr);
					break;
				}
				m_presult += col_attr[col].data_size;
			}

			m_mlines.insert(make_pair(da.organ_code,da.count));

#ifdef DEBUG
			outfile<<++line<<"\tdv:"<<da.dv<<"\torgan_code:"<<da.organ_code<<"\tgtime:"<<da.count<<endl;
#endif
		}
#ifdef DEBUG
		outfile.close();
#endif
	}
	else
	{
		printf("sql:%s, ReadData: error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
		// 		plog->WriteLog(2, "获取配电终端设备月停用时间失败，error(%d) = %s, 文件名：%s, 行号：%d",
		// 			err_info.error_no, err_info.error_info, __FILE__, __LINE__);
		return -1;
	}

	if(rec_num > 0)
		m_oci->FreeReadData(col_attr, col_num, buffer);
	else
		m_oci->FreeColAttrData(col_attr, col_num);*/

	return 0;
}

int STATISLINES::CaculateLineRate()
{
	ES_RESULT es_result;

	map<int,float>::iterator it,it_line;

	//线路故指覆盖率
	it_line = m_mlines.begin();
	for (;it_line != m_mlines.end();++it_line) {

		es_result.m_ntype = it_line->first;
		it = m_mindacator.find(it_line->first);
		if (it != m_mindacator.end()) {

			if (it_line->second == 0 ) {
				
				es_result.m_fresult = 0.0;
			}
			else {

				es_result.m_fresult = static_cast<float>(it->second)/it_line->second;
			}
		}
		else {

			es_result.m_fresult = 0.0;
		}

		m_mindacator_result.insert(make_pair(es_result.m_ntype,es_result.m_fresult));
	}

	//线路二遥终端覆盖率
	it_line = m_mlines.begin();
	for (;it_line != m_mlines.end();++it_line) {

		es_result.m_ntype = it_line->first;
		it = m_mtwoterminal.find(it_line->first);
		if (it != m_mtwoterminal.end()) {

			if (it_line->second == 0 ) {

				es_result.m_fresult = 0.0;
			}
			else {

				es_result.m_fresult = static_cast<float>(it->second)/it_line->second;
			}
		}
		else {

			es_result.m_fresult = 0.0;
		}

		m_mtwo_result.insert(make_pair(es_result.m_ntype,es_result.m_fresult));
	}

	//线路三遥终端覆盖率
	it_line = m_mlines.begin();
	for (;it_line != m_mlines.end();++it_line) {

		es_result.m_ntype = it_line->first;
		it = m_mthreeterminal.find(it_line->first);
		if (it != m_mthreeterminal.end()) {

			if (it_line->second == 0 ) {

				es_result.m_fresult = 0.0;
			}
			else {

				es_result.m_fresult = static_cast<float>(it->second)/it_line->second;
			}
		}
		else {

			es_result.m_fresult = 0.0;
		}

		m_mthree_result.insert(make_pair(es_result.m_ntype,es_result.m_fresult));
	}

	return 0;
}

int STATISLINES::InsertResultData( int statis_type, ES_TIME *es_time )
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
	map<int ,float>::iterator itr_line_result;
	map<int ,float>::iterator itr_line_num;

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
		"dimvalue) values(:1,:2,round(:3,6),:4,sysdate,to_date('%04d-%02d-%02d','YYYY-MM-DD'),:5,:6)",
		table_name, year, month, day);

	//线路故指在线率按主站
	rec_num = m_mindacator_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_result = m_mindacator_result.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "incovline", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;

		itr_line_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//线路二遥终端在线率按主站
	offset = 0;
	rec_num = m_mtwo_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_result = m_mtwo_result.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "twocovline", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//线路三遥终端在线率按主站
	offset = 0;
	rec_num = m_mthree_result.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_result = m_mthree_result.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "threecovline", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_result->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_result->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "百分比", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_result++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//线路总条数
	offset = 0;
	rec_num = m_mlines.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_num = m_mlines.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "linenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_num->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_num++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//故指覆盖线路条数
	offset = 0;
	rec_num = m_mindacator.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_num = m_mindacator.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "incovlinenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_num->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_num++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//二遥终端覆盖线路条数
	offset = 0;
	rec_num = m_mtwoterminal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_num = m_mtwoterminal.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "twocovlinenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_num->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_num++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	//三遥终端覆盖线路条数
	offset = 0;
	rec_num = m_mthreeterminal.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		//plog->WriteLog(2, "malloc error, 文件名：%s, 行号：%d", __FILE__, __LINE__);
		return -1;
	}

	itr_line_num = m_mthreeterminal.begin();
	for(int i = 0; i < rec_num; i++)
	{
		memcpy(m_pdata + offset, "threecovlinenum", col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &itr_line_num->first, col_attr[1].data_size);
		offset += col_attr[1].data_size;
		memcpy(m_pdata + offset, &itr_line_num->second, col_attr[2].data_size);
		offset += col_attr[2].data_size;
		memcpy(m_pdata + offset, "个数", col_attr[3].data_size);
		offset += col_attr[3].data_size;
		memcpy(m_pdata + offset, "无", col_attr[4].data_size);
		offset += col_attr[4].data_size;
		memcpy(m_pdata + offset, "无", col_attr[5].data_size);
		offset += col_attr[5].data_size;
		//printf("indicaratio m_ntype:%d,%.4f\n",itr_online_result->second.m_ntype,itr_online_result->second.m_fresult);
		itr_line_num++;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			// 			plog->WriteLog(2, "终端在线率入库失败，error(%d) = %s, 文件名：%s, 行号：%d",
			// 				err_info.error_no, err_info.error_info, __FILE__, __LINE__);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
		// 		plog->WriteLog(1, "终端在线率按主站当日无统计数据，%04d%02d%02d, 文件名：%s, 行号：%d",
		// 			year, month, day, __FILE__, __LINE__);
	}
	FREE(m_pdata);

	return 0;
}

int STATISLINES::Operator( int statis_type,ES_TIME *es_time )
{
	Clear();

	int ret = 0;

	if (statis_type == STATIS_DAY) {

		ret = ReadDmsLinesByDay(es_time);
		if(ret < 0)
		{
			printf("ReadDmsLinesByDay fail.\n");
			return false;
		}
		//计算
		ret = CaculateLineRate();
		if(ret < 0)
		{
			printf("CaculateLineRate fail.\n");
			return false;
		}
	}
	else {

		ret = ReadDmsLinesByMonth(es_time);
		if(ret < 0)
		{
			printf("ReadDmsLinesByMonth fail.\n");
			return false;
		}
		ret = ReadDmsLinesByDay(es_time); //月显示日分子分母数据
		if(ret < 0)
		{
			printf("ReadDmsLinesByDay fail.\n");
			return false;
		}
	}

	//插入数据到统计表中
	ret = InsertResultData(statis_type, es_time);
	if(ret < 0)
	{
		printf("InsertResultData fail.\n");
		return false;
	}

	return 0;
}

void STATISLINES::Clear()
{
	m_mlines.clear();
	m_mindacator.clear();
	m_mtwoterminal.clear();
	m_mthreeterminal.clear();
	m_mindacator_result.clear();
	m_mtwo_result.clear();
	m_mthree_result.clear();
}
