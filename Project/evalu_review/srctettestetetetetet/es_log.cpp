/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_log.cpp
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 日志功能实现
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#include <stdarg.h>
#include "es_log.h"
#include "es_define.h"

ESLog *ESLog::m_instance = NULL;

ESLog::ESLog()
{
	/*m_oci = new CDci;
	m_oci->Connect(DATABASE_NAME, USERNAME, PASSWORD, &err_info);*/
}

ESLog::~ESLog()
{
	//m_oci->DisConnect(&err_info);
}

ESLog *ESLog::GetInstance()
{
	if(m_instance == NULL)
	{
		m_instance = new ESLog;
	}

	return m_instance;
}

int ESLog::WriteLog(int level, const char *format, ...)
{
	char tmp[MAX_SQL_LEN];
	char sql[MAX_SQL_LEN];
	va_list ap;
	
	va_start(ap, format);
	vsprintf(tmp, format, ap);
	va_end(ap);

	sprintf(sql, "insert into evalusystem.manage.log(type,level,gtime,content) values(2,%d,sysdate,'%s')",
		level, tmp);
	return d5000.d5000_ExecSingle(sql, &err_info);
}
