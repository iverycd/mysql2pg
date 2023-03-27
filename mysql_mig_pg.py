# -*- coding: utf-8 -*-
#  mysql_mig_pg.py linux support py36 py37
# MySQL database migration to postgresql,support highgo,vastbase,telepg,postgresql
# 现在varchar默认以字节存储，部分数据库如海量数据库varchar支持字符存储
# 多线程分表，多进程迁移数据，使用copy迁移数据
import csv
import datetime
import logging
import multiprocessing
import os
import platform
import sys
import time
import traceback
import psycopg2
import pymysql
from dbutils.pooled_db import PooledDB
from concurrent.futures import ThreadPoolExecutor
import concurrent  # 异步任务包
import argparse
import textwrap
import readConfig
from pymysql import converters
from io import StringIO
import pandas as pd
from prettytable import PrettyTable
import ctypes

config = readConfig.ReadConfig()  # 实例化
mysql_host = config.get_mysql('host')
mysql_port = int(config.get_mysql('port'))
mysql_user = config.get_mysql('user')
mysql_passwd = config.get_mysql('passwd')
mysql_database = config.get_mysql('database')
mysql_dbchar = config.get_mysql('dbchar')
ini_row_batch_size = int(config.get_mysql('row_batch_size'))
split_page_size = int(config.get_mysql('split_page_size'))  # 2023-03-05 5000
table_split_thread = int(config.get_mysql('table_split_thread'))  # 2023-03-05更改为4
read_thread = int(config.get_mysql('mysql_fenye_parallel_run'))
postgresql_host = config.get_postgresql('host')
postgresql_port = config.get_postgresql('port')
postgresql_user = config.get_postgresql('user')
postgresql_passwd = config.get_postgresql('passwd')
postgresql_database = config.get_postgresql('database')
converions = converters.conversions  # pymysql在读取bit类型时显示x00的解决办法
converions[pymysql.FIELD_TYPE.BIT] = lambda x: '0' if '\x00' else '1'  # pymysql在读取bit类型时显示x00的解决办法

pgPOOL = PooledDB(
    psycopg2,  # 使用链接数据库的模块
    2,
    host=postgresql_host,
    port=postgresql_port,
    user=postgresql_user,
    password=postgresql_passwd,
    setsession=["""set datestyle = 'YMD'"""],  # 设置时间格式兼容字符串类型的时间，如'2021-02-01 12:12:00'
    database=postgresql_database
)
postgresql_conn = pgPOOL.connection()
postgresql_cur = postgresql_conn.cursor()

MySQLPOOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=['SET AUTOCOMMIT=0', 'SET SESSION group_concat_max_len=1024000'],
    # 开始会话前执行的命令列表。使用连接池执行dml，这里需要显式指定提交，已测试通过
    ping=0,
    # ping MySQL服务端，检查是否服务可用。
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_passwd,
    database=mysql_database,
    charset=mysql_dbchar
)

mysql_conn = MySQLPOOL.connection()
mysql_cursor = mysql_conn.cursor()
mysql_cursor.arraysize = 5000
postgresql_cur.arraysize = 5000

log_path = ''
theTime = datetime.datetime.now()
theTime = str(theTime)
date_split = theTime.strip().split(' ')
date_today = date_split[0].replace('-', '_')
date_clock = date_split[-1]
date_clock = date_clock.strip().split('.')[0]
date_clock = date_clock.replace(':', '_')
date_show = date_today + '_' + date_clock
if platform.system().upper() == 'WINDOWS':
    log_path = "mig_log" + '\\' + mysql_database + '\\'
    if not os.path.isdir(log_path):
        os.makedirs(log_path)
elif platform.system().upper() == 'LINUX' or platform.system().upper() == 'DARWIN':
    log_path = "mig_log" + '/' + mysql_database + '/'
    if not os.path.isdir(log_path):
        os.makedirs(log_path)
else:
    print('can not create dir,please run on win or linux!\n')


class Logger(object):
    def __init__(self, filename='mig.log', add_flag=True,
                 stream=open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)):
        self.terminal = stream
        self.filename = filename
        self.add_flag = add_flag

    def write(self, message):
        if self.add_flag:
            with open(self.filename, 'a+', encoding='utf-8') as log:
                try:
                    self.terminal.write(message)
                    log.write(message)
                except Exception as e:
                    print(e)
        else:
            with open(self.filename, 'w', encoding='utf-8') as log:
                try:
                    self.terminal.write(message)
                    log.write(message)
                except Exception as e:
                    print(e)

    def flush(self):
        pass


def print_source_info(p_version):
    k = PrettyTable(field_names=["MysqlToPG"])
    k.align["MysqlToPG"] = "c"  # 以name字段左对齐
    k.padding_width = 1  # 填充宽度
    k.add_row(["Tool Version: " + p_version])
    k.add_row(["One Key Migration Data from MySQL to PG So Easy"])
    k.add_row(["Powered By: [DBA Group] of Infrastructure Research Center"])
    print(k.get_string(sortby="MysqlToPG", reversesort=False))
    mysql_info = MySQLPOOL._kwargs
    # print('-' * 50 + 'MySQL->postgresql' + '-' * 50)
    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
    print('\n源MySQL数据库连接信息: ' + 'ip:' + str(mysql_info['host']) + ':' + str(mysql_info['port']) + ' 数据库名称: ' + str(
        mysql_info['database']))
    if str(custom_table).upper() == 'TRUE' or str(args.data_only).upper() == 'TRUE':
        print('\n要迁移的表如下:')
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 打开文件
            for line in f:
                print(line.strip('\n').upper().split(',')[0])
    else:
        mysql_cursor.execute(
            """select count(*) from information_schema.TABLES where table_schema in (select database()) and table_type='BASE TABLE'""")
        source_table_count = mysql_cursor.fetchone()[0]
        mysql_cursor.execute(
            """select count(*) from information_schema.TABLES where table_schema in (select database()) and table_type ='VIEW'""")
        source_view_count = mysql_cursor.fetchone()[0]
        mysql_cursor.execute("""show triggers""")
        source_trigger_count = len(mysql_cursor.fetchall())
        mysql_cursor.execute(
            """select count(*) from mysql.proc where db in (select database()) and type = 'PROCEDURE' """)
        source_procedure_count = mysql_cursor.fetchone()[0]
        mysql_cursor.execute(
            """select count(*) from mysql.proc where db in (select database()) and type = 'FUNCTION' """)
        source_function_count = mysql_cursor.fetchone()[0]
        print('源表总计: ' + str(source_table_count))
        print('源视图总计: ' + str(source_view_count))
        print('源触发器总计: ' + str(source_trigger_count))
        print('源存储过程总计: ' + str(source_procedure_count))
        print('源数据库函数总计: ' + str(source_function_count))
    print(
        '\n目标postgresql数据库连接信息: ' + '用户名:' + postgresql_user + ' ip:' + postgresql_host + ':' + str(
            postgresql_port) + ' 数据库名称: ' + postgresql_database)
    is_continue = input('\n是否准备迁移数据：Y|N\n')
    if is_continue == 'Y' or is_continue == 'y':
        print('开始迁移数据')  # continue
    else:
        sys.exit()
    if platform.system().upper() == 'WINDOWS':
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)


def split_success_list(v_max_workers, p_list_success_table):
    """
    将创建表成功的list结果分为n个小list，无论指定多少进程，现在最大限制到4进程
    """
    new_list = []  # 用于存储1分为n的表，尽可能将原表分成n个list
    if v_max_workers > 4:  # 最大使用4线程分割list
        v_max_workers = 4
    if len(p_list_success_table) <= 1:
        v_max_workers = 1
    split_size = round(len(p_list_success_table) / v_max_workers)
    if split_size == 0:  # 防止在如下调用list_of_groups进行切片的时候遇到0
        split_size = 1
    new_list.append(list_of_groups(p_list_success_table, split_size))
    for idx, v_process_page in enumerate(new_list[0], 0):
        print('table page process id[', idx, ']->', v_process_page)
    return new_list


def list_of_groups(init_list, childern_list_len):
    list_of_group = zip(*(iter(init_list),) * childern_list_len)
    end_list = [list(i) for i in list_of_group]
    count = len(init_list) % childern_list_len
    end_list.append(init_list[-count:]) if count != 0 else end_list
    return end_list


# 获取postgresql的列字段类型以及字段长度以及映射数据类型到MySQL的规则
def tbl_columns(table_name):
    list_varchar = ['VARCHAR', 'CHAR']
    list_text = ['LONGTEXT', 'MEDIUMTEXT', 'TEXT', 'TINYTEXT']
    list_int = ['MEDIUMINT', 'TINYINT']
    list_non_int = ['DECIMAL', 'DOUBLE', 'FLOAT']
    list_time = ['DATETIME', 'TIMESTAMP']
    list_lob = ['TINYBLOB', 'BLOB', 'MEDIUMBLOB', 'LONGBLOB']
    # sql = """SHOW FULL COLUMNS FROM %s""" % table_name
    # 注意下面的sql，因为在开发环境需要转义所以有2个反斜杠\
    sql = """select concat('"',lower(column_name),'"'),data_type,character_maximum_length,is_nullable,case  column_default when '( \\'user\\' )' then 'user' else column_default end as column_default,numeric_precision,numeric_scale,datetime_precision,column_key,column_comment from information_schema.COLUMNS where table_schema in (select database()) and table_name='%s'""" % table_name
    mysql_cursor.execute(sql)
    output_table_col = mysql_cursor.fetchall()
    result = []
    for column in output_table_col:  # 按照游标行遍历字段
        # mysql column description
        # result.append({'column_name': column[0],  # 如下为字段的名称
        #                'data_type': column[1],  # 列字段类型
        #                'character_maximum_length': column[2], # 列字段长度范围
        #                'is_nullable': column[3],  # 是否为空
        #                'column_default': column[4],  # 字段默认值
        #                'numeric_precision': column[5],
        #                'numeric_scale': column[6],
        #                'datetime_precision': column[7],
        #                'column_key': column[8],
        #                'column_comment': column[9]
        #                }
        #               )
        if column[1].upper() in list_varchar:  # mysql中普通字符串类型均映射为postgresql的普通类型字符串
            result.append({'column_name': column[0],  # 如下为字段的名称
                           # 'data_type': column[1] + '(' + str(column[2]) + ' char)',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'data_type': column[1] + '(' + str(column[2]) + ')',
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        elif column[1].upper() in list_text:  # mysql中所有text类型均映射为postgresql的text
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'TEXT',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        #  mysql整数类型映射
        elif column[1].upper() in list_int:  # mysql中MEDIUMINT类型均映射为postgresql的int
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'INT',  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        #  mysql非整数类型数字映射
        #  mysql的decimal,double,float均映射为postgresql的decimal
        elif column[1].upper() in list_non_int:  # mysql中MEDIUMINT类型均映射为postgresql的int
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'DECIMAL' if str(column[6]).upper() == 'NONE'
                           else 'DECIMAL' + '(' + str(column[5]) + ',' + str(column[6]) + ')',
                           # 如果有精度指定精度，否则如果numeric_scale为null，直接映射为postgresql的DECIMAL
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # mysql时间类型映射
        elif column[1].upper() in list_time:  # mysql中datetime,timestamp映射为postgresql的timestamp
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'TIMESTAMP',
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # mysql大字段类型映射
        elif column[1].upper() in list_lob:  # mysql中二进制大字段映射为postgresql的bytea
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': 'BYTEA',
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )
        # 若不在以上数据类型，就原模原样使用原始ddl建表
        else:
            result.append({'column_name': column[0],  # 如下为字段的名称
                           'data_type': column[1],  # 列字段类型与长度的拼接，这里显式指定为字符存储
                           'character_maximum_length': column[2],
                           'is_nullable': column[3],  # 是否为空
                           'column_default': column[4],  # 字段默认值
                           'numeric_precision': column[5],
                           'numeric_scale': column[6],
                           'datetime_precision': column[7],
                           'column_key': column[8],
                           'column_comment': column[9]
                           }
                          )

    return result


def create_meta_table():
    global mig_start_time
    mig_start_time = datetime.datetime.now()
    output_table_name = []  # 用于存储要迁移的部分表
    if str(args.data_only).upper() == 'TRUE':
        return 1
    if custom_table.upper() == 'TRUE':
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 打开文件
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))
    else:
        tableoutput_sql = """select table_name from information_schema.tables where table_schema in (select database())  and TABLE_TYPE='BASE TABLE' """  # 查询需要导出的表
        mysql_cursor.execute(tableoutput_sql)
        output_table_name = mysql_cursor.fetchall()
    global all_table_count  # 将postgresql源表总数存入全局变量
    all_table_count = len(output_table_name)  # 无论是自定义表还是全库，都可以存入全局变量
    starttime = datetime.datetime.now()
    table_index = 0
    for row in output_table_name:
        table_name = row[0]
        print('#' * 50 + '开始创建表' + table_name + '#' * 50)
        #  将创建失败的sql记录到log文件
        logging.basicConfig(filename=log_path + 'ddl_failed_table.log')
        # 在postgresql创建表前先删除存在的表
        drop_target_table = 'drop table if exists ' + table_name
        try:
            postgresql_cur.execute(drop_target_table)
        except Exception as e:
            print(' 在目标库删除表失败,尝试级联删除' + str(e.args))
            postgresql_conn.rollback()
            try:
                postgresql_cur.execute("""drop table %s CASCADE""" % table_name)
            except Exception as e:
                print(e)
            print('级联删除成功!')
        fieldinfos = []
        structs = tbl_columns(table_name)  # 获取源表的表字段信息
        # print(structs)  # mysql中各列字段属性
        # MySQL字段类型拼接为postgresql字段类型
        for struct in structs:
            default_value = struct.get('column_default')
            is_nullable = struct.get('is_nullable')
            comment_value = struct.get('column_comment')
            if is_nullable == 'YES':  # 字段是否为空的判断
                is_nullable = ''
            else:
                is_nullable = 'not null'
            if default_value:  # 对默认值以及注释数据类型的判断，如果不是str类型，转为str类型
                default_value = """'""" + default_value + """'"""
            if comment_value:  # postgresql中无法在建表的时候添加comment
                comment_value = "'{0}'".format(comment_value) if type(comment_value) == 'str' else str(comment_value)
            fieldinfos.append('{0} {1} {2} {3}'.format(
                struct['column_name'],
                struct['data_type'],
                ('default ' + default_value) if default_value else '',
                is_nullable  # 如果is_nullable
            ),
            )
        create_table_sql = 'create table {0} ({1})'.format(table_name, ','.join(fieldinfos))  # 生成创建目标表的sql
        print(create_table_sql)
        try:
            postgresql_cur.execute(create_table_sql)
            postgresql_conn.commit()  # 需要显式commit
            print('创建完毕', time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), '\n')
            filename = log_path + 'ddl_success_table.log'  # ddl创建成功的表，记录表名到/tmp/ddl_success_table.csv
            f = open(filename, 'a', encoding='utf-8')
            f.write(table_name + '\n')
            f.close()
            list_success_table.append(table_name)  # MySQL ddl创建成功的表也存到list中

        except Exception as e:
            postgresql_conn.rollback()
            table_index = table_index + 1
            print('\n' + '/* ' + str(e.args) + ' */' + '\n')  # ddl错误输出postgresql异常
            print(traceback.format_exc())  # 如果某张表创建失败，遇到异常记录到log，会继续创建下张表
            # ddl创建失败的表名记录到文件/tmp/ddl_failed_table.log
            filename = log_path + 'ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + 'CREATE TABLE ERROR ' + str(table_index) + '-' * 50 + '\n')
            f.write('/* ' + table_name + ' */' + '\n')
            f.write(create_table_sql + '\n\n\n')
            f.close()
            ddl_failed_table_result.append(table_name)  # 将当前ddl创建失败的表名记录到ddl_failed_table_result的list中
            ddl_create_error_table = '\n' + '/* ' + str(e.args) + ' */' + '\n'
            logging.error(ddl_create_error_table)  # ddl创建失败的sql语句输出到文件/tmp/ddl_failed_table.log
            print('表' + table_name + '创建失败请检查ddl语句!\n')
    endtime = datetime.datetime.now()
    print("表创建耗时\n" + "开始时间:" + str(starttime) + '\n' + "结束时间:" + str(endtime) + '\n' + "消耗时间:" + str(
        (endtime - starttime).seconds) + "秒\n")
    print('#' * 50 + '表创建完成' + '#' * 50 + '\n\n\n')


def create_auto_column():
    global all_auto_count, all_auto_success_count, all_auto_fail_count
    output_table_name = []  # 用于存储要迁移的部分表
    postgresql_seq_sql = ''
    postgresql_add_auto = ''
    print('#' * 50 + '开始修改自增列' + '#' * 50)
    if str(args.data_only).upper() == 'TRUE':
        return 1
    if custom_table.upper() == 'TRUE':  # 根据自定义表创建自增列
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 打开文件
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            sequence_sql = """select table_name,COLUMN_NAME,upper(concat(TABLE_NAME,'_',COLUMN_NAME,'_seq')) sequence_name from information_schema. COLUMNS where
TABLE_SCHEMA in (select database()) and table_name ='%s' and EXTRA='auto_increment'""" % v_custom_table[0]
            mysql_cursor.execute(sequence_sql)
            output_name = mysql_cursor.fetchall()  # 获取所有的自增列
            all_auto_count = len(output_name)
            for v_output_name in output_name:
                table_name = v_output_name[0]  # 获取表名
                column_name = v_output_name[1]  # 获取列名
                sequence_name = v_output_name[2]  # 生成序列名称
                try:
                    mysql_cursor.execute("""SELECT Auto_increment FROM information_schema.TABLES WHERE Table_Schema in (select database())
                            AND table_name ='%s'""" % table_name)
                    auto_column_startval = mysql_cursor.fetchone()[0]  # 获取自增列起始值
                    postgresql_seq_sql = """CREATE SEQUENCE %s INCREMENT BY 1 START %s """ % (
                        sequence_name, auto_column_startval)
                    print(postgresql_seq_sql + ';')
                    postgresql_cur.execute("""DROP SEQUENCE IF EXISTS %s """ % sequence_name)
                    postgresql_cur.execute(postgresql_seq_sql)
                    postgresql_conn.commit()
                except Exception as e:
                    postgresql_conn.rollback()
                    print(postgresql_seq_sql + ';创建序列失败！' + str(e.args))
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + 'CREATE SEQUENCE ERROR ' + '-' * 50 + '\n')
                    f.write(postgresql_seq_sql + ';')
                    f.write(str(e.args) + '\n')
                    f.close()
                try:
                    postgresql_add_auto = """ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s')""" % (
                        table_name, column_name, sequence_name)
                    print(postgresql_add_auto + ';')
                    postgresql_cur.execute(postgresql_add_auto)
                    postgresql_conn.commit()
                    print('自增列修改完毕\n')
                    all_auto_success_count += 1
                except Exception as e:
                    postgresql_conn.rollback()
                    print(postgresql_add_auto + ';' + '修改表' + table_name + '列 ' + column_name + ' 自增列失败 ' + str(e.args))
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + 'ALTER AUTO COL ERROR ' + '-' * 50 + '\n')
                    f.write(postgresql_add_auto + ';\n')
                    f.write(str(e.args) + '\n')
                    f.close()
                    all_auto_fail_count += 1
    else:  # 创建全部的自增列
        sequence_sql = """select table_name,COLUMN_NAME,upper(concat(TABLE_NAME,'_',COLUMN_NAME,'_seq')) sequence_name from information_schema. COLUMNS where
TABLE_SCHEMA in (select database()) and table_name in(select t.TABLE_NAME
from information_schema. TABLES t where TABLE_SCHEMA in (select database()) and AUTO_INCREMENT is not null)  and EXTRA='auto_increment' """  # 获取自增列的表名以及列名,用于生成序列名称
        mysql_cursor.execute(sequence_sql)
        output_name = mysql_cursor.fetchall()  # 获取所有的自增列
        all_auto_count = len(output_name)
        for v_output_name in output_name:
            table_name = v_output_name[0]  # 获取表名
            column_name = v_output_name[1]  # 获取列名
            sequence_name = v_output_name[2]  # 生成序列名称
            try:
                mysql_cursor.execute("""SELECT Auto_increment FROM information_schema.TABLES WHERE Table_Schema in (select database())
                AND table_name ='%s'""" % table_name)
                auto_column_startval = mysql_cursor.fetchone()[0]  # 获取自增列起始值
                postgresql_seq_sql = """CREATE SEQUENCE %s INCREMENT BY 1 START %s """ % (
                    sequence_name, auto_column_startval)
                print(postgresql_seq_sql + ';')
                postgresql_cur.execute("""DROP SEQUENCE IF EXISTS %s """ % sequence_name)
                postgresql_cur.execute(postgresql_seq_sql)
                postgresql_conn.commit()
            except Exception as e:
                postgresql_conn.rollback()
                print(postgresql_seq_sql + ';' + '创建序列失败！' + str(e.args) + '\n')
                filename = log_path + 'ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'CREATE SEQUENCE ERROR ' + '-' * 50 + '\n')
                f.write(postgresql_seq_sql + ';')
                f.write(str(e.args) + '\n')
                f.close()
            try:
                postgresql_add_auto = """ALTER TABLE %s ALTER COLUMN %s SET DEFAULT NEXTVAL('%s')""" % (
                    table_name, column_name, sequence_name)
                print(postgresql_add_auto + ';')
                postgresql_cur.execute(postgresql_add_auto)
                postgresql_conn.commit()
                print('自增列修改完毕\n')
                all_auto_success_count += 1
            except Exception as e:
                postgresql_conn.rollback()  # 异常后需要显式回滚，否则后续会话无法执行
                print('修改表' + table_name + '列 ' + column_name + ' 自增列失败 ' + postgresql_add_auto + ';' + str(
                    e.args) + '\n')
                filename = log_path + 'ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'ALTER AUTO COL ERROR ' + '-' * 50 + '\n')
                f.write(postgresql_add_auto + ';\n')
                f.write(str(e.args) + '\n')
                f.close()
                all_auto_fail_count += 1
    print('自增列创建完毕!')


def create_view():
    list_fail_view = []
    global all_view_count, all_view_success_count, all_view_failed_count
    if str(args.data_only).upper() == 'TRUE' or str(args.custom_table).upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    mysql_cursor.execute(
        """select table_name from information_schema.views where table_schema in (select database())""")
    view_name = mysql_cursor.fetchall()
    all_view_count = len(view_name)
    index = 0
    print('#' * 50 + '开始创建' + '视图 ' + '#' * 50)
    print('create view sql:')
    for v_view_name in view_name:
        index += 1
        mysql_cursor.execute("""SHOW CREATE VIEW %s""" % v_view_name)  # 获取单个视图的定义
        view_info = mysql_cursor.fetchall()
        for v_out in view_info:  # 对单个视图的定义做文本处理，替换文本
            view_name = v_out[0]
            view_define = v_out[1]
            # print(view_name.upper())
            # print('original view sql: ' + view_define)
            format_sql1 = view_define[view_define.rfind('VIEW'):]
            # print('format_sql1: ' + format_sql1)
            format_sql2 = format_sql1.replace('`', '')
            # print('format_sql2: ' + format_sql2)
            create_view_sql = 'create or replace  ' + format_sql2  # 创建视图的原始sql
            create_view_sql_out = create_view_sql.upper() + ';'  # 对创建视图的文本全部大写以及加分号
            create_view_sql_out = create_view_sql_out.replace('CONVERT(', '')
            create_view_sql_out = create_view_sql_out.replace('USING UTF8MB4)', '')
            if view_name.upper() == 'VIEW_FRAME_OU':  # 对以下特定视图做个别处理
                create_view_sql_out = create_view_sql_out.replace('FROM (FRAME_OU JOIN FRAME_OU_EXTENDINFO) WHERE',
                                                                  'FROM FRAME_OU JOIN FRAME_OU_EXTENDINFO on')
            if view_name.upper() == 'VIEW_FRAME_USER':
                create_view_sql_out = create_view_sql_out.replace('FROM (FRAME_USER JOIN FRAME_USER_EXTENDINFO) WHERE',
                                                                  'FROM FRAME_USER JOIN FRAME_USER_EXTENDINFO on')
            if view_name.upper() == 'VIEW_PERSONAL_ELEMENT':
                create_view_sql_out = create_view_sql_out.replace(
                    'FROM ((PERSONAL_PORTAL_ELEMENT A JOIN APP_ELEMENT B) JOIN APP_PORTAL_ELEMENT C ON(((C.ELEMENTGUID = B.ROWGUID) AND (A.PTROWGUID = C.ROWGUID))))',
                    'FROM (PERSONAL_PORTAL_ELEMENT A JOIN APP_PORTAL_ELEMENT C ON A.PTROWGUID = C.ROWGUID) JOIN APP_ELEMENT B ON C.ELEMENTGUID = B.ROWGUID')
                create_view_sql_out = create_view_sql_out.replace(
                    'FROM ((PERSONAL_PORTAL_ELEMENT A JOIN APP_ELEMENT B) JOIN APP_PORTAL_ELEMENT C ON(((C.ELEMENTGUID = B.ROWGUID ) AND (A.PTROWGUID = C.ROWGUID))))',
                    'FROM (PERSONAL_PORTAL_ELEMENT A JOIN APP_PORTAL_ELEMENT C ON A.PTROWGUID = C.ROWGUID) JOIN APP_ELEMENT B ON C.ELEMENTGUID = B.ROWGUID')
            if view_name.upper() == 'VIEW_PORTAL_MYITEM':
                create_view_sql_out = create_view_sql_out.replace(
                    'FROM (PORTAL_ITEM JOIN PORTAL_MYITEM ON((PORTAL_ITEM.ROWGUID = PORTAL_MYITEM.PORTALETGUID))) WHERE (PORTAL_ITEM.DISABLED = 0)',
                    'FROM PORTAL_ITEM JOIN PORTAL_MYITEM ON PORTAL_ITEM.ROWGUID = PORTAL_MYITEM.PORTALETGUID WHERE (PORTAL_ITEM.DISABLED = 0)'
                )
                create_view_sql_out = create_view_sql_out.replace(
                    'PORTAL_ITEM.DISABLED = 0', 'PORTAL_ITEM.DISABLED = \'0\''
                )
            print('[' + str(index) + '] ' + create_view_sql_out + '\n')  # 剩余的执行原始视图
            try:
                postgresql_cur.execute(create_view_sql_out)
                postgresql_conn.commit()
                all_view_success_count += 1
            except Exception as e:
                all_view_failed_count += 1
                print('视图创建失败 ' + str(e.args))
                postgresql_conn.rollback()
                filename = log_path + 'ddl_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + 'CREATE VIEW ERROR ' + str(all_view_failed_count) + ' -' * 50 + '\n')
                f.write(create_view_sql_out + '\n')
                # f.write('\n' + str(e.args) + '\n')
                f.close()
                list_fail_view.append(create_view_sql_out)
                if view_name.upper() == 'VIEW_PERSONAL_ELEMENT':
                    print(
                        'use this fix it: FROM (PERSONAL_PORTAL_ELEMENT A JOIN APP_PORTAL_ELEMENT C ON A.PTROWGUID = C.ROWGUID) JOIN APP_ELEMENT B ON C.ELEMENTGUID = B.ROWGUID')
    print('\n视图创建完毕!\n')
    print('创建视图失败的sql如下:\n')
    for v_fail_sql in list_fail_view:
        print(v_fail_sql)


def non_split_write(table_name, select_sql, page_size, first_write, table_thread, p_row_batch_size):
    """
    处理每个表第一部分数据以及无法用分页查询(使用全表扫描)表数据的迁移
    :param table_name: 这里是每个独立进程，处理该表非分页查询的表
    :param select_sql: 拼接SQL，包含分页的第一部分或者全表扫描数据
    :param page_size: 分页查询，每页的记录数
    :param first_write: 用于区分是否写第一部分数据还是全表扫描之后的全表迁移
    :param table_thread: 外层分表任务的线程号
    :param p_row_batch_size: 控制游标的fetchmany方法，一次性从游标获取多少行数据
    :return:
    """
    err_count = 0
    pg_insert_count = 0
    mysql_con = MySQLPOOL.connection()  # 被调用的时候，需要为每个线程都独立创建数据库连接
    my_cur = mysql_con.cursor()  # MySQL连接池
    pg_conn = psycopg2.connect(database=postgresql_database, user=postgresql_user,
                               password=postgresql_passwd, host=postgresql_host, port=postgresql_port,
                               keepalives=1, keepalives_idle=5, keepalives_interval=10, keepalives_count=15)
    pg_cur = pg_conn.cursor()
    if first_write == 1:
        exec_sql = select_sql + """ %s limit %s,%s """ % (table_name, 0, page_size)
    else:
        exec_sql = select_sql + table_name
    try:
        my_cur.execute(exec_sql)  # 第一页记录
    except Exception as e:
        print(e, '查询MySQL源表数据失败，请检查是否存在该表或者表名小写', table_name)
    while True:  # 这里是对第1页的数据记录通过while循环批量插入到目标库
        rows = list(my_cur.fetchmany(p_row_batch_size))
        if not rows:
            break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
        data = pd.DataFrame(rows)
        data = data.convert_dtypes()
        output = StringIO()
        # 以下使用分号";"分隔列字段，使用双引号对单个字段包围起来，如果遇到null值，文本中使用反斜杠+N替代，空字符串仍然是没有引号包围的空字符串
        data.to_csv(output, sep=';', index=False, header=False, quotechar='\"', na_rep='\\N')
        csv_data = output.getvalue()
        try:
            # 以下使用copy的csv格式，分号分隔列字段，双引号包围单个字段，这里需要使用null参数来区分导出数据的null值
            copy_sql = "COPY " + table_name + " FROM stdin WITH CSV DELIMITER AS ';'  QUOTE AS '\"'  null as '\\N' "
            pg_cur.copy_expert(copy_sql, StringIO(csv_data))
            pg_insert_count = pg_insert_count + pg_cur.rowcount  # 每次插入的行数
            pg_conn.commit()  # 如果连接池没有配置自动提交，否则这里需要显式提交
            print(str(datetime.datetime.now()), 'table_thread[', table_thread, ']', '已写入表',
                  table_name, '行数', pg_insert_count, ' METHOD:FULL TABLE')
        except Exception as e:
            print("non_split_write error pg copy write fail:\n", e)
            pg_conn.rollback()
            err_count += 1
            filename = log_path + 'insert_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' ' + str(table_name) + ' INSERT ERROR' + '-' * 50 + '\n')
            f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
            f.write('\n' + '/* ' + str(e) + ' */' + '\n' + '\n\n')
            f.close()
    pg_conn.close()
    my_cur.close()
    mysql_con.close()


def sql_limit_write(page_shard, page_size, col_key, table_name, select_sql, page_thread, table_thread,
                    p_row_batch_size):
    """
    处理单个张表在每个独立进程上的分页查询以及数据写入，外部可使用多进程调用此方法，实现多进程同时分页查询同一个单表，多进程同时写入目标库
    :param page_shard: 单个表分页记录通过切割成分片组成的分页页码集合，例如4个进程同时读分页查询，那么切割成[[0],[1],[2],[3]],长度为4
    :param page_size: 分页查询每页查询的记录数
    :param col_key: 主键或者自增列字段
    :param table_name: 独立进程中的表名
    :param select_sql: 每个分页部分组成的拼接SQL
    :param page_thread: 当前表所在的切片进程号
    :param table_thread: 外部任务表所处的线程号
    :param p_row_batch_size: 控制获取源表数据的游标，一次性查询的结果行数目
    :return:
    """
    err_count = 0
    pg_insert_count = 0
    mysql_con2 = MySQLPOOL.connection()  # 被调用的时候，需要为每个线程都独立创建数据库连接
    my_cur2 = mysql_con2.cursor()  # MySQL连接池
    pg_conn2 = psycopg2.connect(database=postgresql_database, user=postgresql_user,
                                password=postgresql_passwd, host=postgresql_host, port=postgresql_port,
                                keepalives=1, keepalives_idle=5, keepalives_interval=10, keepalives_count=15)
    pg_cur2 = pg_conn2.cursor()
    for page_index in page_shard[page_thread]:  # 例如总共有100行记录，每页10条记录，那么需要循环10次
        cur_start_page = page_index * page_size  # cur_start_page 从0开始
        try:
            # 查询上一页数据最大的一条guid记录
            my_cur2.execute(
                """select max(%s) from (select %s from %s order by %s  limit %s,%s) aa""" % (
                    col_key, col_key, table_name, col_key, cur_start_page, page_size))
            max_guid = my_cur2.fetchone()[0]  # 第一页数据最大的一条id,guid记录
            if max_guid is not None:  # 这里需要判断下guid是否为空，否则如果是空值，会继续查询，造成重复插入
                #  根据上一步查询出的最大guid值，再获取分页记录
                my_cur2.execute(
                    select_sql + """ %s  where %s> '%s' limit %s """ % (
                        table_name, col_key, max_guid, page_size))  # 分页查询的结果集
        except Exception as e:
            print(str(e), '查询MySQL源表数据失败，请检查是否存在该表或者表名小写', table_name)
            continue  # 这里需要显式指定continue，否则某张表不存在就会跳出此函数
        while True:  # 这里是对第二页开始的数据记录通过while循环批量插入到目标库
            rows = list(my_cur2.fetchmany(p_row_batch_size))  # 这里获取的结果集行数，实际上就是分页查询每页的大小
            if not rows:
                break  # 当前表游标获取不到数据之后中断循环，返回到mig_database，可以继续下个表
            data = pd.DataFrame(rows)
            data = data.convert_dtypes()
            output = StringIO()
            data.to_csv(output, sep=';', index=False, header=False, quotechar='\"', na_rep='\\N')
            csv_data = output.getvalue()
            try:
                copy_sql = "COPY " + table_name + " FROM stdin WITH CSV DELIMITER AS ';'  QUOTE AS '\"'  null as '\\N' "
                pg_cur2.copy_expert(copy_sql, StringIO(csv_data))
                pg_insert_count = pg_insert_count + pg_cur2.rowcount  # 每次插入的行数
                pg_conn2.commit()  # 如果连接池没有配置自动提交，否则这里需要显式提交
                print(str(datetime.datetime.now()), 'table_thread[', table_thread, '] page_thread[', page_thread,
                      '] 已写入表',
                      table_name, '行数', pg_insert_count, ' METHOD:PG COPY')
            except Exception as e:
                print("sql_limit_write error pg copy write fail:\n", e)
                pg_conn2.rollback()
                err_count += 1
                filename = log_path + 'insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + str(err_count) + ' ' + str(table_name) + ' INSERT ERROR' + '-' * 50 + '\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write('\n' + '/* ' + str(e) + ' */' + '\n' + '\n\n')
                f.close()
    pg_conn2.close()
    my_cur2.close()
    mysql_con2.close()


def pre_mig_data(list_index, new_list, ini_thread_count, row_batch_size):
    """
    在迁移前拼接好源表各个字段组成查询SQL，并且自动判断是否需要使用分页查询还是全表扫描迁移数据
    :param list_index: 此方法所在外部表任务的线程号，
    :param new_list: 创建成功的表组成的新list
    :param ini_thread_count: config文件中设定的使用多少进程同时对一个表进行分页查询
    :param row_batch_size: 使用fetchmany方法一次性从游标结果集获取的结果集行数
    :return:
    """
    err_count = 0
    global insert_error_count
    ini_thread_init = ini_thread_count  # 每次新的表在计算并行时，重新初始化并行查询参数为config参数
    get_table_count = 0
    select_sql = ''
    mysql_con = MySQLPOOL.connection()
    mysql_cursor = mysql_con.cursor()  # MySQL连接池
    postgresql_conn = psycopg2.connect(database=postgresql_database, user=postgresql_user,
                                       password=postgresql_passwd, host=postgresql_host, port=postgresql_port)
    postgresql_cur = postgresql_conn.cursor()
    for v_table_name in new_list[0][int(list_index)]:
        table_name = v_table_name
        ini_thread_count = ini_thread_init  # 每次新的表在计算并行时，重新初始化并行查询参数为config参数
        #  以下生成MySQL各个字段以及对于blob类型显式使用hex函数输出
        try:
            mysql_cursor.execute("""select group_concat(col_sql) from (SELECT case data_type when 'TINYBLOB' then concat('concat(''\\\\\\\\\\\\x'',','hex(',concat('`',column_name,'`'),'))') when 'BLOB' then concat('concat(''\\\\\\\\\\\\x'',','hex(',concat('`',column_name,'`'),'))') when 'MEDIUMBLOB' then concat('concat(''\\\\\\\\\\\\x'',','hex(',concat('`',column_name,'`'),'))') when 'LONGBLOB' then concat('concat(''\\\\\\\\\\\\x'',','hex(',concat('`',column_name,'`'),'))') else concat('`',column_name,'`') end col_sql
                                FROM information_schema.COLUMNS WHERE table_schema IN (SELECT DATABASE()) AND table_name = '%s'  order by ordinal_position) div_sql""" % table_name)
            mysql_cursor_out = mysql_cursor.fetchone()[0]
            select_sql = 'select ' + mysql_cursor_out + ' from '
        except Exception as e:
            print('拼接查询字段异常，请检查源表[', table_name, ']是否存在', e)
        try:
            mysql_cursor.execute("""select count(*) from %s""" % table_name)
            get_table_count = mysql_cursor.fetchone()[0]
        except Exception as e:
            print('查询源表[', table_name, ']数据记录总数失败', e)
        if get_table_count > 0:
            try:
                mysql_cursor.execute(
                    """SELECT count(*) FROM information_schema.COLUMNS WHERE table_schema IN (SELECT DATABASE()) AND table_name = '%s' """ % table_name)
                # <<<<<<判断是否有主键>>>>>>
                mysql_cursor.execute(
                    """SELECT lower(column_name) FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY' AND table_schema IN (SELECT DATABASE ()) AND table_name='%s' """ % table_name)
                is_pk_key = mysql_cursor.rowcount  # 判断表是否有主键
                # <<<<<<判断是否是自增列>>>>>>
                mysql_cursor.execute("""select   column_name   from information_schema. COLUMNS where
                                TABLE_SCHEMA in (select database()) and table_name ='%s' and EXTRA='auto_increment'""" % table_name)
                is_auto_col = mysql_cursor.rowcount
            except Exception as e:
                print(e, '获取源表总数以及列总数失败，请检查是否存在该表或者表名小写！', table_name)
                err_count += 1
                sql_insert_error = traceback.format_exc()
                filename = log_path + 'insert_failed_table.log'
                f = open(filename, 'a', encoding='utf-8')
                f.write('-' * 50 + str(err_count) + ' ' + table_name + ' INSERT ERROR' + '-' * 50 + '\n')
                f.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()) + '\n\n')
                f.write(sql_insert_error + '\n\n')
                f.close()
                continue  # 这里需要显式指定continue，否则表不存在或者其他问题，会直接跳出for循环,使用continue继续下个表
            page_size = split_page_size  # 分页的每页记录数
            total_page_num = round((get_table_count + page_size - 1) / page_size)  # 自动计算总共有几页
            page_yema = []  # 页码的集合，,分页计算出来有6页，全进去之后是[0,1,2,3,4,5],
            for inum in range(total_page_num):
                page_yema.append(inum)
            # ini_thread_count是自己设定的,即config文件的mysql_fenye_parallel_run参数，同时有几个线程同时读limit分页，并非实际运行的进程数
            if ini_thread_count > total_page_num:
                ini_thread_count = 2  # 如果设定的进程数比分页总数大，就限定读取进程数为2
            multi_thread_input = int(total_page_num / ini_thread_count)  # 这个是每个线程即将要处理的记录数，比如表有6行，2个线程同时读取，那么每个线程就有3行数据
            # 如果只有1页记录，就规避掉整除为0的情况
            if multi_thread_input == 0:
                multi_thread_input = 1
            # 以下对MySQL分页查询页记录进行分片，用于给每个进程读取一个分片,
            # 比如6页[0,1,2,3,4,5]，分成2批,[[0,1,2],[3,4,5]]
            split_page_yema = list_of_groups(page_yema, multi_thread_input)
            # 下面是计算MySQL实际能运行分页查询的进程数
            mysql_read_thread = len(split_page_yema)
            try:  # 在迁移数据前先truncate表
                postgresql_cur.execute("""truncate table %s """ % table_name)
                postgresql_cur.execute("""commit""")
            except Exception as e:
                print(e, 'table thread ', list_index, 'truncate table ', table_name, 'failed!')
            if is_pk_key == 1:  # 表有主键，这里是主键分页
                mysql_cursor.execute(
                    """SELECT lower(column_name) FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY' AND table_schema IN (SELECT DATABASE ()) AND table_name='%s' """ % table_name)
                col_key = mysql_cursor.fetchone()[0]
                first_write = 1
                try:
                    non_split_write(table_name, select_sql, page_size, first_write, list_index,
                                    row_batch_size)  # 迁移第一部分数据
                except Exception as e:
                    print(e)
                with concurrent.futures.ProcessPoolExecutor(
                        max_workers=mysql_read_thread) as executor:  # 多线程迁移单表的各个分页切片
                    task1 = {
                        executor.submit(sql_limit_write, split_page_yema, page_size, col_key, table_name, select_sql,
                                        v_index, list_index, row_batch_size): v_index
                        # v_index:读进程序号，list_index，外部表任务线程号
                        for v_index in range(mysql_read_thread)}
                    for future1 in concurrent.futures.as_completed(task1):
                        task_name1 = task1[future1]
                        try:
                            future1.result()
                        except Exception as exc:
                            print('[ERROR] pk read process %r generated an exception: %s' % (task_name1, exc))
            if is_auto_col == 1 and is_pk_key != 1:  # 同时有主键以及自增列场景下，优先使用主键分页，其次自增列分页,这里是自增列分页
                mysql_cursor.execute(
                    """select column_name from information_schema. COLUMNS where TABLE_SCHEMA in (select database()) and table_name ='%s' and EXTRA='auto_increment'""" % table_name)
                col_key = mysql_cursor.fetchone()[0]
                first_write = 1
                try:
                    non_split_write(table_name, select_sql, page_size, first_write, list_index,
                                    row_batch_size)  # 迁移第一部分数据
                except Exception as e:
                    print(e)
                with concurrent.futures.ProcessPoolExecutor(
                        max_workers=mysql_read_thread) as executor:  # 多线程迁移单表的各个分页切片
                    task2 = {
                        executor.submit(sql_limit_write, split_page_yema, page_size, col_key, table_name, select_sql,
                                        v_index, list_index, row_batch_size): v_index
                        # v_index:读进程序号，list_index，外部表任务线程号
                        for v_index in range(mysql_read_thread)}
                    for future2 in concurrent.futures.as_completed(task2):
                        task_name2 = task2[future2]
                        try:
                            future2.result()  # 使用result方法获取调用的函数是否正常执行，若调用异常下面会抛出
                        except Exception as exc:
                            print(
                                '[ERROR] auto_increment read process %r generated an exception: %s' % (task_name2, exc))
            if is_auto_col != 1 and is_pk_key != 1:  # 既没有主键以及自增列的情况下，使用全表扫描完成插入
                first_write = 0
                try:
                    # postgresql_cur.execute("""truncate table %s """ % table_name) # 多线程的时候这里会被truncate阻塞
                    non_split_write(table_name, select_sql, page_size, first_write, list_index, row_batch_size)
                except Exception as e:
                    print(e, '查询MySQL源表数据失败，请检查是否存在该表或者表名小写', table_name)
                    continue  # 这里需要显式指定continue，否则某张表不存在就会跳出此函数


def async_work_copy(table_list, p_table_split_thread, p_row_batch_size):
    """
    使用异步任务，对创建成功的表，通过多线程同时处理多个表进行迁移
    :param table_list: 创建成功的表，通过切片方法，切割成几个分片，例如[['TABLE1'],['TABLE2']]
    :param p_table_split_thread: 同时能处理表的任务数，即表切片之后的长度
    :param p_row_batch_size: fetchmany方法每次从游标结果集一次性获取的行数
    :return:
    """
    print('async_work_copy recevied table thread total:', p_table_split_thread)
    # if str(args.data_only).upper() == 'TRUE':
    #     return 1
    print('#' * 50 + '开始数据迁移' + '#' * 50 + '\n')
    print('START MIGRATING ROW DATA! ' + str(datetime.datetime.now()) + ' \n')
    begin_time = datetime.datetime.now()
    index = list(range(0, p_table_split_thread))  # 任务序列
    try:
        # 创建迁移任务表，用来统计表插入以及完成的时间
        postgresql_cur.execute("""drop table if exists my_mig_task_info""")
        postgresql_cur.execute("""create table my_mig_task_info(table_name varchar(100),task_start_time timestamp ,
                task_end_time timestamp ,thread int,run_time int,source_table_rows int,target_table_rows int,
                is_success varchar(100))""")
        postgresql_conn.commit()
    except Exception as e:
        print(e)

    # 生成异步任务并开启
    with concurrent.futures.ThreadPoolExecutor(max_workers=p_table_split_thread) as executor:
        task = {executor.submit(pre_mig_data, v_index, table_list, read_thread, p_row_batch_size): v_index for v_index
                in index}
        for future in concurrent.futures.as_completed(task):
            task_name = task[future]
            try:
                future.result()
            except Exception as exc:
                print('[async_work_copy] %r generated an exception: %s' % (task_name, exc))
            # print('begin task' + str(task_name))
    end_time = datetime.datetime.now()
    print('FINISH MIGRATING ROW DATA! ' + str(datetime.datetime.now()) + ' \n')
    print('表数据迁移耗时：' + str((end_time - begin_time).seconds) + '\n')
    print('#' * 50 + '表数据插入完成' + '#' * 50 + '\n')


# 批量创建主键以及索引
def create_meta_constraint():
    global constraint_failed_count
    if str(args.data_only).upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    global all_constraints_count  # mysql中约束以及索引总数
    global all_constraints_success_count  # mysql中约束以及索引创建成功的计数
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_index = []  # 存储执行创建约束的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '约束以及索引 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute("""SELECT 
IF(
INDEX_NAME='PRIMARY',CONCAT('ALTER TABLE ',TABLE_NAME,' ', 'ADD ', -- 主键的判断，下面是主键的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT('INDEX ',
  INDEX_NAME,
  '  '
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT('UNIQUE INDEX ',
 INDEX_NAME
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
,
if(
UPPER(INDEX_NAME) != 'PRIMARY' and non_unique=0, -- 判断是否是唯一索引
CONCAT('CREATE UNIQUE INDEX ',index_name,'_',substr(uuid(),1,8),substr(MD5(RAND()),1,3),' ON ',table_name,'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'), -- 唯一索引的拼接sql
replace(replace(CONCAT('CREATE INDEX ',index_name,'_',substr(uuid(),1,8),substr(MD5(RAND()),1,3), ' ON ',-- 非唯一索引，普通索引的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT(' ',
  table_name,
  ''
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT(table_name,' xxx'
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'),char(13),''),char(10),'')
)

) sql_text
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA in (select database())  and table_name='%s'
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME ASC, INDEX_NAME ASC""" % v_custom_table[0])
            custom_index = mysql_cursor.fetchall()
            for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_index的list里面
                all_index.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有约束
        mysql_cursor.execute("""SELECT 
IF(
INDEX_NAME='PRIMARY',CONCAT('ALTER TABLE ',TABLE_NAME,' ', 'ADD ', -- 主键的判断，下面是主键的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT('INDEX ',
  INDEX_NAME,
  '  '
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT('UNIQUE INDEX ',
 INDEX_NAME
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');')
,
if(
UPPER(INDEX_NAME) != 'PRIMARY' and non_unique=0, -- 判断是否是唯一索引
CONCAT('CREATE UNIQUE INDEX ',index_name,'_',substr(uuid(),1,8),substr(MD5(RAND()),1,3),' ON ',table_name,'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'), -- 唯一索引的拼接sql
replace(replace(CONCAT('CREATE INDEX ',index_name,'_',substr(uuid(),1,8),substr(MD5(RAND()),1,3), ' ON ',-- 非唯一索引，普通索引的拼接sql
 IF(NON_UNIQUE = 1,
 CASE UPPER(INDEX_TYPE)
 WHEN 'FULLTEXT' THEN 'FULLTEXT INDEX'
 WHEN 'SPATIAL' THEN 'SPATIAL INDEX'
 ELSE CONCAT(' ',
  table_name,
  ''
 )
END,
IF(UPPER(INDEX_NAME) = 'PRIMARY',
 CONCAT('PRIMARY KEY '
 ),
CONCAT(table_name,' xxx'
)
)
),'(', GROUP_CONCAT(DISTINCT CONCAT('', COLUMN_NAME, '') ORDER BY SEQ_IN_INDEX ASC SEPARATOR ', '), ');'),char(13),''),char(10),'')
)

) sql_text
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA in (select database())
GROUP BY TABLE_NAME, INDEX_NAME
ORDER BY TABLE_NAME ASC, INDEX_NAME ASC""")
        all_index = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_constraints_count = len(all_index)
    if all_constraints_count == 0:
        print('无约束需要创建')
    for d in all_index:
        create_index_sql = d[0]
        print(create_index_sql)
        try:
            postgresql_cur.execute(create_index_sql)
            postgresql_conn.commit()
            print('约束以及索引创建完毕\n')
            all_constraints_success_count += 1
        except Exception as e:
            postgresql_conn.rollback()
            err_count += 1
            constraint_failed_count += 1  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
            print('约束或者索引创建失败请检查ddl语句!\n' + str(e.args))
            filename = log_path + 'ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' CONSTRAINTS CREATE ERROR' + '-' * 50 + '\n')
            f.write(create_index_sql + '\n')
            f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
            f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建约束以及索引耗时： ' + str((end_time - start_time).seconds))
    print('#' * 50 + '主键约束、索引创建完成' + '#' * 50 + '\n\n\n')


# 创建外键
def create_foreign_key():
    global fk_failed_count
    if str(args.data_only).upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    global all_fk_count  # mysql中外键总数
    global all_fk_success_count  # 外键创建成功计数
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_fk = []  # 存储执行创建外键约束的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '外键约束 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute(
                """select  table_name from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA in (select database()) and table_name ='%s' """ %
                v_custom_table[0])
            custom_index = mysql_cursor.fetchall()
            for v_out in custom_index:  # 每次将上面单表全部结果集全部存到all_fk的list里面
                all_fk.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有约束
        mysql_cursor.execute(
            """select  table_name from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS where CONSTRAINT_SCHEMA in (select database()) """)
        all_fk = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_fk_count = len(all_fk)
    if all_fk_count == 0:
        print('无约束需要创建')
    for d in all_fk:
        fk_table = d[0]
        try:
            mysql_cursor.execute("""SELECT concat('ALTER TABLE ',K.TABLE_NAME,' ADD CONSTRAINT ',K.CONSTRAINT_NAME,' FOREIGN KEY(',GROUP_CONCAT(COLUMN_NAME),')',' REFERENCES '
,K.REFERENCED_TABLE_NAME,'(',GROUP_CONCAT(REFERENCED_COLUMN_NAME),')',' ON DELETE ',DELETE_RULE,' ON UPDATE ',UPDATE_RULE)
FROM
	INFORMATION_SCHEMA.KEY_COLUMN_USAGE k INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS r
	on k.CONSTRAINT_NAME = r.CONSTRAINT_NAME
	where k.CONSTRAINT_SCHEMA in (select database()) AND r.CONSTRAINT_SCHEMA in (select database())  and k.REFERENCED_TABLE_NAME is not null
	and k.TABLE_NAME = '%s'
order by k.ORDINAL_POSITION""" % fk_table)  # 根据子表去获取创建外键的sql
            all_fk_sql = mysql_cursor.fetchall()  # 以上拼接sql结果集
            for v_all_fk_sql in all_fk_sql:  # 循环在目标数据库创建外键
                try:
                    postgresql_cur.execute(v_all_fk_sql[0])  # 目标库执行创建外键sql
                    postgresql_conn.commit()
                    print(v_all_fk_sql[0])  # 打印创建外键的sql
                    print('外键约束创建完毕\n')
                    all_fk_success_count += 1
                except Exception as e:
                    postgresql_conn.rollback()
                    err_count += 1
                    fk_failed_count += 1  # 用来统计主键或者索引创建失败的计数，只要创建失败就往list存1
                    print('外键创建失败请检查ddl语句!\n' + str(e.args))
                    filename = log_path + 'ddl_failed_table.log'
                    f = open(filename, 'a', encoding='utf-8')
                    f.write('-' * 50 + str(err_count) + ' CONSTRAINTS CREATE ERROR' + '-' * 50 + '\n')
                    f.write(v_all_fk_sql[0] + '\n')
                    f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
                    f.close()
                    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                    end_time = datetime.datetime.now()
                    print('创建外键约束耗时： ' + str((end_time - start_time).seconds))
                    print('#' * 50 + '外键约束创建完成' + '#' * 50 + '\n\n\n')
        except Exception as e:
            print('查找源库外键失败,请检查源库外键定义')


# 触发器
def create_trigger():
    global all_trigger_count  # 源数据库触发器总数
    global all_trigger_success_count  # 目标触发器创建成功数
    global trigger_failed_count  # 目标触发器创建失败数
    if str(args.data_only).upper() == 'TRUE':  # 如果指定-d选项就退出函数
        return 1
    err_count = 0
    output_table_name = []  # 迁移部分表
    all_trigger = []  # 存储执行创建触发器的结果集
    start_time = datetime.datetime.now()
    print('#' * 50 + '开始创建' + '触发器 ' + '#' * 50)
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    if custom_table.upper() == 'TRUE':  # 如果命令行参数有-c选项，仅创建部分约束
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # 读取自定义表
            for line in f:
                output_table_name.append(list(line.strip('\n').upper().split(',')))  # 将自定义表全部保存到list
        for v_custom_table in output_table_name:  # 读取第N个表查询生成拼接sql
            mysql_cursor.execute(
                """SELECT replace(upper(concat('create or replace trigger ',trigger_name,' ',action_timing,' ',event_manipulation,' on ',event_object_table,' for each row as ',action_statement)),'#','-- ') FROM information_schema.triggers WHERE trigger_schema in (select database()) and event_object_table='%s'""" %
                v_custom_table[0])
            custom_trigger = mysql_cursor.fetchall()
            for v_out in custom_trigger:  # 每次将上面单表全部结果集全部存到all_trigger的list里面
                all_trigger.append(v_out)
    else:  # 命令行参数没有-c选项，创建所有触发器
        mysql_cursor.execute(
            """SELECT replace(upper(concat('create or replace trigger ',trigger_name,' ',action_timing,' ',event_manipulation,' on ',event_object_table,' for each row as ',action_statement)),'#','-- ') FROM information_schema.triggers WHERE trigger_schema in (select database()) """)
        all_trigger = mysql_cursor.fetchall()  # 如果要每张表查使用T.TABLE_NAME = '%s',%s传进去是没有单引号，所以需要用单引号号包围
    all_trigger_count = len(all_trigger)
    if all_trigger_count == 0:
        print('无触发器要创建')
    for d in all_trigger:
        create_tri_sql = d[0]
        print(create_tri_sql)
        try:
            postgresql_cur.execute(create_tri_sql)
            postgresql_conn.commit()
            print('触发器创建完毕\n')
            all_trigger_success_count += 1
        except Exception as e:
            postgresql_conn.rollback()
            err_count += 1
            trigger_failed_count += 1
            print('触发器创建失败请检查ddl语句!\n' + str(e.args))
            filename = log_path + 'ddl_failed_table.log'
            f = open(filename, 'a', encoding='utf-8')
            f.write('-' * 50 + str(err_count) + ' TRIGGER CREATE ERROR' + '-' * 50 + '\n')
            f.write(create_tri_sql + '\n')
            f.write('\n' + '/* ' + str(e.args) + ' */' + '\n')
            f.close()
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    end_time = datetime.datetime.now()
    print('创建触发器耗时： ' + str((end_time - start_time).seconds))
    print('#' * 50 + '触发器创建完成' + '#' * 50 + '\n\n\n')


def show_proc_fun():
    index = 0
    filename = log_path + 'proc_fun_sql.sql'
    # mysql procedure
    mysql_cursor.execute("""select name from mysql.proc where db in (select DATABASE()) and type='PROCEDURE'""")
    proc_name = mysql_cursor.fetchall()
    f = open(filename, 'a', encoding='utf-8')
    f.write('--MySQL procedure sql--\n')
    for v_proc_name in proc_name:
        index += 1
        v_proc_name = v_proc_name[0]
        f = open(filename, 'a', encoding='utf-8')
        f.write('--' + str(index) + ' ' + v_proc_name + '\n')
        mysql_cursor.execute("""show create procedure %s """ % v_proc_name)
        proc_sql = mysql_cursor.fetchone()[2]
        proc_sql = proc_sql.replace('DEFINER=', '')
        proc_sql = proc_sql.replace('`root`@`%`', '')
        proc_sql = proc_sql.replace('`', '')
        f.write(proc_sql + '\n\n')
        f.close()
    # mysql function
    mysql_cursor.execute("""select name from mysql.proc where db in (select DATABASE()) and type='FUNCTION'""")
    fun_name = mysql_cursor.fetchall()
    f = open(filename, 'a', encoding='utf-8')
    f.write('--MySQL FUNCTION sql--\n')
    for v_fun_name in fun_name:
        index += 1
        v_fun_name = v_fun_name[0]
        f = open(filename, 'a', encoding='utf-8')
        f.write('--' + str(index) + ' ' + v_fun_name + '\n')
        mysql_cursor.execute("""show create function %s """ % v_fun_name)
        fun_sql = mysql_cursor.fetchone()[2]
        fun_sql = fun_sql.replace('DEFINER=', '')
        fun_sql = fun_sql.replace('`root`@`%`', '')
        fun_sql = fun_sql.replace('`', '')
        fun_sql = fun_sql.replace('CHARSET utf8', '')
        f.write(fun_sql + '\n\n')
        f.close()


# 迁移摘要
def mig_summary():
    if platform.system().upper() == 'WINDOWS':
        # exepath = os.path.dirname(os.path.abspath(__file__)) + '\\'
        exepath = os.path.dirname(os.path.realpath(sys.argv[0])) + '\\'
    else:
        exepath = os.path.dirname(os.path.abspath(__file__)) + '/'
    mig_end_time = datetime.datetime.now()
    # mysql源表信息
    mysql_tab_count = all_table_count  # 源表要迁移的表总数
    mysql_view_count = all_view_count  # 源视图总数
    mysql_auto_count = all_auto_count  # 源数据库自增列总数
    mysql_trigger_count = all_trigger_count  # 源数据库触发器总数
    mysql_index_count = all_constraints_count  # 源数据库约束以及索引总数
    mysql_fk_count = all_fk_count  # 源数据库外键总数
    # mysql源表信息

    # postgresql迁移计数
    postgresql_success_table_count = str(len(list_success_table))  # 目标数据库创建成功的表总数
    table_failed_count = len(ddl_failed_table_result)  # 目标数据库创建失败的表总数
    postgresql_success_view_count = str(all_view_success_count)  # 目标数据库视图创建成功的总数
    view_error_count = all_view_failed_count  # 目标数据库创建视图失败的总数
    postgresql_success_auto_count = str(all_auto_success_count)  # 目标数据库自增列成功的总数
    postgresql_fail_auto_count = str(all_auto_fail_count)  # 目标数据库自增列失败的总数
    postgresql_success_tri_count = str(all_trigger_success_count)  # 目标数据库创建触发器成功的总数
    postgresql_fail_tri_count = str(trigger_failed_count)  # 目标数据库创建触发器失败的总数
    postgresql_success_idx_count = str(all_constraints_success_count)  # 目标数据库创建约束以及索引成功总数
    postgresql_fail_idx_count = str(constraint_failed_count)  # 目标数据库创建失败的约束以及索引总数
    postgresql_success_fk_count = str(all_fk_success_count)  # 目标数据库外键创建成功的总数
    postgresql_fail_fk_count = str(fk_failed_count)  # 目标数据库外键创建失败的总数
    # postgresql迁移计数

    print('\033[31m*' * 50 + '数据迁移摘要' + '*' * 50 + '\033[0m\n\n\n')
    print("MySQL迁移数据到postgresql完毕\n" + "开始时间:" + str(mig_start_time) + '\n' + "结束时间:" + str(
        mig_end_time) + '\n' + "耗时:" + str(
        (mig_end_time - mig_start_time).seconds) + "秒\n")
    print('\n\n\n')
    print('目标数据库: ' + postgresql_database)
    print('1、表数量总计: ' + str(mysql_tab_count) + ' 目标表创建成功计数: ' + postgresql_success_table_count + ' 目标表创建失败计数: ' + str(
        table_failed_count))
    print(
        '2、视图数量总计: ' + str(mysql_view_count) + ' 目标视图创建成功计数: ' + postgresql_success_view_count + ' 目标视图创建失败计数: ' + str(
            view_error_count))
    print(
        '3、自增列数量总计: ' + str(
            mysql_auto_count) + ' 目标自增列创建成功计数: ' + postgresql_success_auto_count + ' 目标自增列修改失败计数: ' + str(
            postgresql_fail_auto_count))
    print('4、触发器数量总计: ' + str(
        mysql_trigger_count) + ' 触发器创建成功计数: ' + str(postgresql_success_tri_count) + ' 触发器创建失败计数: ' + str(
        postgresql_fail_tri_count))
    print('5、索引以及约束总计: ' + str(
        mysql_index_count) + ' 目标索引以及约束创建成功计数: ' + postgresql_success_idx_count + ' 目标索引以及约束创建失败计数: ' + str(
        postgresql_fail_idx_count))
    print('6、外键总计: ' + str(mysql_fk_count) + ' 目标外键创建成功计数: ' + postgresql_success_fk_count + ' 目标外键创建失败计数: ' +
          str(postgresql_fail_fk_count))
    csv_file = open(log_path + "insert_table.csv", 'w', newline='')
    # 将MySQL创建成功的表总数记录保存到csv文件
    try:
        writer = csv.writer(csv_file)
        writer.writerow(('TOTAL:', postgresql_success_table_count))
    except Exception:
        print(traceback.format_exc())
    finally:
        csv_file.close()
    if ddl_failed_table_result:  # 输出失败的对象
        print("\n\n创建失败的表如下：")
        for output_ddl_failed_table_result in ddl_failed_table_result:
            print(output_ddl_failed_table_result)
        print('\n\n\n')
    print('\n请检查创建失败的表DDL以及约束。有关更多详细信息，请参阅迁移输出信息')
    print('MySQL存储过程以及函数定义已转储到' + exepath + '' + log_path + 'proc_fun_sql.sql\n')
    print(
        '迁移日志已保存到' + exepath + '' + log_path + '\n表迁移记录请查看insert_table.csv或者在目标数据库查询表my_mig_task_info\n有关插入错误请查看ddl_failed_table.log以及insert_failed_table.log\n\n')


if __name__ == '__main__':
    multiprocessing.freeze_support()  # windows环境的多进程需要在main函数下面使用此方法，否则程序会被从头开始不断循环
    version = '1.3.16-MP'
    parser = argparse.ArgumentParser(prog='mysql_mig_postgresql',
                                     description=textwrap.dedent('''\
    EXAMPLE:
        eg:mig some table and data to postgresql:\n ./mysql_mig_pg -c \n
        eg:mig only data to postgresql:\n ./mysql_mig_pg -d true
        '''))

    parser.add_argument('--custom_table', '-c', help='mig some tables not all tables into postgresql,default false',
                        action='store_true')  # 默认是全表迁移
    parser.add_argument('--data_only', '-d', help='mig only data row', action='store_true')
    parser.add_argument('-v', '--version', action='version', version=version, help='Display version')
    args, unparsed = parser.parse_known_args()  # 只解析正确的参数列表，无效参数会被忽略且不报错，args是解析正确参数，unparsed是不被解析的错误参数，win多进程需要此写法
    # args = parser.parse_args()
    all_table_count = 0
    list_success_table = []
    ddl_failed_table_result = []
    all_constraints_count = 0  # 约束以及索引总数
    all_constraints_success_count = 0  # mysql中创建约束以及索引成功的总数
    constraint_failed_count = 0  # 用于统计主键以及索引创建失败的总数
    all_view_count = 0  # 源数据库视图总数
    all_view_success_count = 0  # 目标数据库创建成功视图总数
    all_view_failed_count = 0  # 目标数据库创建失败视图总数
    all_auto_count = 0  # 源数据库自增列总数
    all_auto_success_count = 0  # 目标数据库创建成功自增列总数
    all_auto_fail_count = 0  # 目标数据库自增列创建失败总数
    all_trigger_count = 0  # 源数据库中触发器总数
    all_trigger_success_count = 0  # 目标数据库触发器创建成功的总数
    trigger_failed_count = 0  # 目标触发器创建失败的总数
    all_fk_count = 0  # 源数据库外键总数
    fk_failed_count = 0  # 目标数据库创建失败的外键总数
    all_fk_success_count = 0  # 目标数据库外键创建成功计数
    insert_error_count = 0
    mig_start_time = ''

    if str(args.custom_table).upper() == 'TRUE' and str(args.data_only).upper() == 'TRUE':
        print('ERROR: -c AND -d OPTION CAN NOT BE USED TOGETHER!\nEXIT')
        sys.exit(0)

    # 判断命令行参数-c是否指定
    if str(args.custom_table).upper() == 'TRUE' or str(args.data_only).upper() == 'TRUE':
        custom_table = 'true'
        path_file = log_path + 'table.txt'  # 用来记录DDL创建成功的表
        if os.path.exists(path_file):
            os.remove(path_file)
        with open('custom_table.txt', 'r', encoding='utf-8') as fr, open(log_path + 'table.txt', 'w',
                                                                         encoding='utf-8') as fd:
            row_count = len(fr.readlines())
        if row_count < 1:
            print('!!!请检查当前目录custom_table.txt是否有表名!!!\n\n\n')
            time.sleep(2)
        #  在当前目录下编辑custom_table.txt，然后对该文件做去掉空行处理，输出到tmp目录
        with open('custom_table.txt', 'r', encoding='utf-8') as fr, open(log_path + 'table.txt', 'w',
                                                                         encoding='utf-8') as fd:
            for text in fr.readlines():
                if text.split():
                    fd.write(text)
    else:
        custom_table = 'false'
    sys.stdout = Logger(log_path + "mig.log", True, sys.stdout)
    print_source_info(version)
    create_meta_table()
    create_auto_column()
    if str(args.data_only).upper() == 'TRUE':
        with open(log_path + "table.txt", "r", encoding='utf-8') as f:  # -d选项指定时读取自定义表文件获取表名
            for line in f:
                list_success_table.append(line.strip('\n').upper().split(',')[0])
    print('list_success_table->', list_success_table)
    ok_tablelist = split_success_list(table_split_thread, list_success_table)
    async_work_copy(ok_tablelist, len(ok_tablelist[0]), ini_row_batch_size)  # copy write
    create_view()
    create_meta_constraint()
    create_trigger()
    create_foreign_key()
    show_proc_fun()
    mig_summary()
    postgresql_conn.close()
