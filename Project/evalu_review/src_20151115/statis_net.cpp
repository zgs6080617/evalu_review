/*******************************************************************************
* Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
* File    : statis_net.h
* Author  : lichengming
* Version : v1.0
* Function : ftp网络质量监控指标
* History
* ===============================================
* 2015-10-26  lichengming create
* ===============================================
*******************************************************************************/
#include "statis_net.h"

//网络ping检测
int net_check(const char* ipaddr);

StatisNet::StatisNet()
{
	m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);
}

StatisNet::~StatisNet()
{
	m_oci->DisConnect(&err_info);
}

int StatisNet::GetConfig()
{
	char tempstr[100];
	int num;
	int rec_num;
	int col_num;
	char sql[200];
	char *buf = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;

	sprintf(sql,"select organ_code,ipaddr from evalusystem.config.downloadschedule;");

	if(m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buf, &err_info) > 0)
	{
		m_presult = buf;
		int m_organ_code;
		char m_ipaddr[20];
		int rec, col;
		for (rec = 0; rec < rec_num; rec++)
		{
			memset(m_ipaddr,0x00,sizeof(m_ipaddr));
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0x00 , 100);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					m_organ_code = *(int*)(tempstr);
					break;
				case 1:
					strcpy(m_ipaddr,tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_ipaddr.insert(make_pair(m_organ_code,m_ipaddr));
		}
	}
	else
	{
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//更新九地市实时网络检测结果向量
int StatisNet::UpdateNetCount()
{
	map<int,string>::iterator it_ipaddr;
	map_netquality.clear();
	int m_quality;
	float m_score;
	for(it_ipaddr = map_ipaddr.begin();it_ipaddr != map_ipaddr.end();it_ipaddr++)
	{
		m_quality = net_check(it_ipaddr->second.c_str());
		map_netquality.insert(make_pair(it_ipaddr->first,m_quality));
	}
	return 0;
}

//插入九地市网络质量实时结果及当天历史指标结果到数据库
int StatisNet::InsertNetScore()
{
	int offset = 0;
	char sql[200];
	char sql_delete[200];
	char today[20];
	int col_num = 2;
	int m_ndata_len = 0;
	char *m_pdata=NULL;
	int rec_num = 0;
	int ret;
	
	memset(sql,0x00,sizeof(sql));
	memset(sql_delete,0x00,sizeof(sql_delete));
	struct ColAttr* col_attr = (ColAttr* )malloc(sizeof(ColAttr_t)*col_num);
	if(col_attr == NULL)
		return -1;
		
	//实时网络质量情况入库
	col_attr[0].data_type = DCI_INT;
	col_attr[0].data_size = sizeof(int);
	col_attr[1].data_type = DCI_INT;
	col_attr[1].data_size = sizeof(int);
	
	for(int i = 0; i < col_num; i++)
		m_ndata_len += col_attr[i].data_size;
	rec_num = map_netquality.size();
	m_pdata = (char *)malloc(m_ndata_len * rec_num);
	if(m_pdata == NULL)
	{
		cout<<"malloc error"<<endl;
		return -1;
	}
	sprintf(sql,"insert into evalusystem.manage.network_quality(organ_code,quality,date) values "
		"(:1,:2,to_date(sysdate,'YYYY-MM-DD HH24:MI:SS'));");

	map<int,int>::iterator it_netquality = map_netquality.begin();
	for(;it_netquality != map_netquality.end();it_netquality++)
	{
		memcpy(m_pdata + offset, &it_netquality->first, col_attr[0].data_size);
		offset += col_attr[0].data_size;
		memcpy(m_pdata + offset, &it_netquality->second, col_attr[1].data_size);
		offset += col_attr[1].data_size;
	}

	if(rec_num > 0)
	{
		ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
		if( !ret )
		{
			printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
			FREE(m_pdata);
			return -1;
		}
	}
	else
	{
	}
	FREE(m_pdata);

	//删除网络质量旧数据
	int year,month,day;
	GetCurDate(&year, &month, &day);
	sprintf(today,"%04d-%02d-%02d",year,month,day);
	sprintf(sql_delete,"delete from evalusystem.manage.err_file where count_file_path = '网络质量指标' and date like '%s%%';",today);
	ret = m_oci->ExecSingle(sql_delete,&err_info);
	if(!ret)
	{
		printf("ExecSingle: error(%d) = %s, sql=%s;\n",err_info.error_no,err_info.error_info,sql_delete);
		return -1;
	}
	//网络质量指标新数据入库
	if(GetNetValue() == 0)
	{
		offset = 0;
		col_attr[0].data_type = DCI_INT;
		col_attr[0].data_size = sizeof(int);
		col_attr[1].data_type = DCI_STR;
		col_attr[1].data_size = 10;
	
		for(int i = 0; i < col_num; i++)
			m_ndata_len += col_attr[i].data_size;
		rec_num = map_netscore.size();
		m_pdata = (char *)malloc(m_ndata_len * rec_num);
		if(m_pdata == NULL)
		{
			cout<<"malloc error"<<endl;
			return -1;
		}
		sprintf(sql,"insert into evalusystem.manage.err_file(date,organ_code,count_file_path,dis,source) values "
			"(to_date(sysdate,'YYYY-MM-DD HH24:MI:SS'),:1,'网络质量指标',:2,'gpms');");

		map<int,float>::iterator it_netscore = map_netscore.begin();
		for(;it_netscore != map_netscore.end();it_netscore++)
		{
			memcpy(m_pdata + offset, &it_netscore->first, col_attr[0].data_size);
			offset += col_attr[0].data_size;
			//float转string可能有问题，待测试
			char m_score[10];
			memset(m_score,0,sizeof(m_score));
			sprintf(m_score,"%.2f",it_netscore->second);
			memcpy(m_pdata + offset, m_score, col_attr[1].data_size);
			offset += col_attr[1].data_size;
		}

		if(rec_num > 0)
		{
			ret = m_oci->WriteData(sql, m_pdata, rec_num, col_num, col_attr, &err_info);
			if( !ret )
			{
				printf("WriteData: sql = %s; error(%d) = %s;\n",sql,err_info.error_no,err_info.error_info);
				FREE(m_pdata);
				return -1;
			}
		}
		else
		{
		}
		FREE(m_pdata);
	}
	
	return 0;
}

//计算九地市网络质量指标结果
int StatisNet::GetNetValue()
{
	char tempstr[100],today[20];
	int num;
	int rec_num;
	int col_num;
	char sql[200];
	char *buf = NULL;
	char *m_presult = NULL;
	struct ColAttr *col_attr = NULL;
	
	map_netscore.clear();
	int year,month,day;
	GetCurDate(&year, &month, &day);
	sprintf(today,"%04d-%02d-%02d",year,month,day);

	sprintf(sql,"select organ_code,avg(quality) avg_quality from evalusystem.manage.network_quality where date like '%s%%' group by organ_code;",today);

	if(m_oci->ReadData(sql, &rec_num, &col_num, &col_attr, &buf, &err_info) > 0)
	{
		m_presult = buf;
		int m_organ_code;
		float m_score;
		int rec, col;
		for (rec = 0; rec < rec_num; rec++)
		{
			for(col = 0; col < col_num; col++)
			{
				memset(tempstr, 0x00 , 100);
				memcpy(tempstr, m_presult, col_attr[col].data_size);
				switch(col)
				{
				case 0:
					m_organ_code = *(int*)(tempstr);
					break;
				case 1:
					m_score = *(float*)(tempstr);
					break;
				default:
					break;
				}
				m_presult += col_attr[col].data_size;
			}
			map_netscore.insert(make_pair(m_organ_code,m_score));
		}
	}
	else
	{
		return -1;
	}
	if(rec_num > 0)
		m_oci->FreeReadData(col_attr,col_num,buf);
	else
		m_oci->FreeColAttrData(col_attr, col_num);
	return 0;
}

//网络ping检测
int net_check(const char* ipaddr)
{
	FILE * fstream=NULL;
	char buf[1024];
	char tmp[128];
	char pingstr[64];
	memset(buf,0,sizeof(buf));
	memset(pingstr,0,sizeof(pingstr));
	char mstr[10];
	memset(mstr,0,sizeof(mstr));
	sprintf(pingstr,"ping -c 5 %s",ipaddr);
	printf("Organ IP:%s\n",ipaddr);
	if(NULL == (fstream = popen(pingstr,"r")))
	{
		return -1;
	}
	while(NULL != fgets(buf,sizeof(buf),fstream))
	{
		printf("%s",buf);
		if(strstr(buf,"packets transmitted")!=NULL)
		{
			printf("%s",buf);
			sprintf(tmp,"%s",buf);
		}
	}
	pclose(fstream);
	//printf("%s\n",tmp);
	const char* split=",";
	char *e_str;
	int count = 0;
	e_str=strtok(tmp,split);
	count++;
	while(e_str!=NULL)
	{
		//printf("%d:%s\n",count,e_str);
		if(count == 3)
		{
			sprintf(mstr,"%s",e_str);
			printf("%s",mstr);
		}
		e_str=strtok(NULL,split);
		count++;
	}
	char *pstart;
	char *pend;
	char score[5];
	memset(score,0,sizeof(score));
	pstart = mstr;
	pend = strstr(mstr,"%");
	printf("pstart:%s pend:%s pend-pstart:%d\n",pstart,pend,pend-pstart);
	strncpy(score,pstart,pend-pstart);
	//printf("%s\n",score);
	int mscore = atoi(score);
	//printf("%d\n",mscore);
	return 100 - mscore;
};