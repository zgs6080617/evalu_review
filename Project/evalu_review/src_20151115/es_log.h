/*******************************************************************************
 * Copyright (c) Beijing Kedong Electric Power Control System Co.,Ltd. 2014 All Rights Reserved. 
 * File    : es_log.h
 * Author  : guoqiuye
 * Version : v1.0
 * Function : 日志功能头文件
 * History
 * ===============================================
 * 2014-08-01  guoqiuye create
 * ===============================================
*******************************************************************************/
#ifndef _ES_LOG_H_
#define _ES_LOG_H_
#include "dcisg.h"

#define plog ESLog::GetInstance()

class ESLog
{
public:
	~ESLog();
	static ESLog *GetInstance();
	int WriteLog(int level, const char *format, ...);

private:
	CDci *m_oci;
	ErrorInfo err_info;
	static ESLog *m_instance;

	ESLog();
};

#endif
