# -*- coding: utf-8 -*-
# v1.11.2 2023-02-20
# MySQL database compare data and tables with postgresql
import ctypes
import datetime
import platform
import sys
import psycopg2
import pymysql
from dbutils.pooled_db import PooledDB
import os
from prettytable import PrettyTable
import readConfig

# print("当前路径 -> %s" % os.getcwd())
current_path = os.path.dirname(__file__)

# 创建日志文件夹
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
    log_path = "mig_log" + '\\' + date_show + '\\'
    current_path = current_path + '\\'
    if not os.path.isdir(log_path):
        os.makedirs(log_path)
elif platform.system().upper() == 'LINUX' or platform.system().upper() == 'DARWIN':
    log_path = "mig_log" + '/' + date_show + '/'
    current_path = current_path + '/'
    if not os.path.isdir(log_path):
        os.makedirs(log_path)
else:
    print('can not create dir,please run on win or linux!\n')

config = readConfig.ReadConfig()  # 实例化

try:
    # MySQL read config
    mysql_host = config.get_mysql('host')
    mysql_port = int(config.get_mysql('port'))
    mysql_user = config.get_mysql('user')
    mysql_passwd = config.get_mysql('passwd')
    mysql_database = config.get_mysql('database')
    mysql_dbchar = config.get_mysql('dbchar')
    # postgresql read config
    postgresql_host = config.get_postgresql('host')
    postgresql_port = config.get_postgresql('port')
    postgresql_user = config.get_postgresql('user')
    postgresql_passwd = config.get_postgresql('passwd')
    postgresql_database = config.get_postgresql('database')
except Exception as e:
    print(e, '请检查当前目录是否存在config.ini文件或者数据库连接配置错误')
    sys.exit(0)

postgresql_conn = psycopg2.connect(database=postgresql_database, user=postgresql_user,
                                   password=postgresql_passwd, host=postgresql_host, port=postgresql_port)
postgresql_cur = postgresql_conn.cursor()

MySQL_POOL = PooledDB(
    creator=pymysql,  # 使用链接数据库的模块
    maxconnections=0,  # 连接池允许的最大连接数，0和None表示不限制连接数
    mincached=10,  # 初始化时，链接池中至少创建的空闲的链接，0表示不创建
    maxcached=0,  # 链接池中最多闲置的链接，0和None不限制
    maxshared=3,
    # 链接池中最多共享的链接数量，0和None表示全部共享。PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
    blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
    maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
    setsession=['SET AUTOCOMMIT=0;'],  # 开始会话前执行的命令列表。使用连接池执行dml，这里需要显式指定提交，已测试通过
    ping=1,
    # ping MySQL服务端，检查是否服务可用。
    host=mysql_host,
    port=mysql_port,
    user=mysql_user,
    password=mysql_passwd,
    database=mysql_database,
    charset=mysql_dbchar
)

mysql_conn = MySQL_POOL.connection()
mysql_cursor = mysql_conn.cursor()
mysql_cursor.arraysize = 20000


class Logger(object):
    def __init__(self, filename=log_path + 'compare.log', stream=sys.stdout):
        self.terminal = stream
        self.log = open(filename, 'w')

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        pass


# 将迁移过程中终端输出的信息记录到文件
sys.stdout = Logger(stream=sys.stdout)


def table_prepare():
    postgresql_cur.execute("""drop table if exists data_compare""")
    postgresql_cur.execute("""create table data_compare
(id int ,
source_db_name varchar(100),
source_table_name varchar(100),
source_rows int,
db_type varchar(100),
target_table_name varchar(100),
target_rows int,
is_success varchar(10),
compare_time TIMESTAMP default CURRENT_TIMESTAMP
)""")


def check_db_exist(src_db_name, tgt_user_name):
    mysql_cursor.execute(
        """select count(distinct TABLE_SCHEMA) from information_schema.TABLES where TABLE_SCHEMA='%s' """ % src_db_name)
    src_result = mysql_cursor.fetchone()[0]
    postgresql_cur.execute("""select count(*) from pg_user where upper(usename)=upper('%s')""" % tgt_user_name)
    trg_result = postgresql_cur.fetchone()[0]
    return src_result, trg_result


def data_compare_single(sourcedb, target_user):  # 手动输入源数据库、目标数据库名称，比对全表数据
    table_id = 0
    target_rows = 0
    target_table_name = ''
    target_view_name = ''
    src_out, trg_out = check_db_exist(sourcedb, target_user)
    if src_out == 0:
        print(sourcedb, '在源数据库不存在\nEXIT!')
        sys.exit()
    elif trg_out == 0:
        print(target_user, '在目标数据库不存在此模式名\nEXIT!')
        sys.exit()
    else:  # 检查源库、目标库名称是否存在之后，开始比较
        print('开始比较全库数据差异\n源数据库名称:', sourcedb, '目标用户名:', target_user)
        print('----------------------')
        # 先根据MySQL的表名查每个表的行数
        mysql_cursor.execute(
            """select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='BASE TABLE'
             """ % sourcedb)
        out_table = mysql_cursor.fetchall()
        source_table_total = len(out_table)
        postgresql_cur.execute(
            """select count(*) from information_schema.tables where table_catalog=current_database() and table_name not in ('DATA_COMPARE','MY_MIG_TASK_INFO','hg_t_audit_log') and upper(table_type)='BASE TABLE' and table_schema not in ('pg_catalog','information_schema','utl_file')""")
        target_table_total = postgresql_cur.fetchone()[0]
        print('表总数:' + '源数据库 ' + str(source_table_total) + ' 目标数据库 ' + str(target_table_total))
        print('正在比对数据可能需要5-10min，请耐心等待!!!!!!')
        for v_out_table in out_table:
            source_table = v_out_table[0]
            table_id += 1
            mysql_cursor.execute("""select count(*) from `%s`.`%s`""" % (sourcedb, source_table))
            source_rows = mysql_cursor.fetchone()[0]  # 源表行数
            print('正在比对表['+source_table+'] '+str(table_id)+'/'+str(source_table_total))
            try:
                target_user_name = target_user
                # 这里判断下源表的名称在目标数据库是否存在
                postgresql_cur.execute(
                    """select count(*) from information_schema.tables where table_catalog=current_database() and table_name='%s' """ % source_table)
                target_table = postgresql_cur.fetchone()[0]
                if target_table > 0:
                    target_table_name = source_table  # 目标表名称与源库表名称实际相同
                    postgresql_cur.execute(
                        """select count(*) from %s""" % target_table_name)
                    target_rows = postgresql_cur.fetchone()[0]  # 目标表行数
                else:
                    target_table_name = 'TABLE NOT EXIST'  # 目标表不存在就将表命名为TABLE NOT EXIST
                    target_rows = -1
            except Exception as e:
                print(e, ' 在目标数据库查询表' + source_table + '失败')
            try:  # 将以上比对的数据保存在目标库的表里
                if (source_rows != target_rows) or (source_table.upper() != target_table_name.upper()):
                    is_success = 'N'
                else:
                    is_success = 'Y'
                postgresql_cur.execute("""insert into data_compare
                                            (id,
                                                        source_db_name,
                                                        source_table_name,
                                                        source_rows,
                                                        db_type,
                                                        target_table_name,
                                                        target_rows,
                                                        is_success
                                                        ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                    table_id, sourcedb.upper(), source_table.upper(), source_rows,
                    'TABLE',
                    target_table_name.upper(),
                    target_rows, is_success.upper()))
                postgresql_conn.commit()
            except Exception as e:
                print(e, '数据比对结果保存在目标表失败')
                postgresql_conn.rollback()
        # 视图比较
        mysql_cursor.execute(
            """select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA='%s' and TABLE_TYPE='VIEW'
             """ % sourcedb)
        out_view = mysql_cursor.fetchall()
        source_view_total = len(out_view)
        postgresql_cur.execute(
            """select count(*) from information_schema.tables where table_catalog=current_database() and upper(table_type)='VIEW' and table_schema not in ('pg_catalog','information_schema','oracle','dbms_pipe') and table_name !='pg_stat_statements' """)
        target_view_total = postgresql_cur.fetchone()[0]
        print('视图总数:' + '源数据库 ' + str(source_view_total) + ' 目标数据库 ' + str(target_view_total))
        for v_out_view in out_view:
            source_view_name = v_out_view[0]
            table_id += 1
            try:
                target_user_name = target_user
                postgresql_cur.execute(
                    """select count(*) from information_schema.tables where table_catalog=current_database() and upper(table_type)='VIEW' and table_schema not in ('pg_catalog','information_schema') and table_name='%s'""" % source_view_name)
                target_view = postgresql_cur.fetchone()[0]  # 目标视图名称
                if target_view == 0:
                    target_view_name = 'NOT EXISTS VIEW'
                else:
                    target_view_name = source_view_name
            except Exception as e:
                print(e, ' 在目标数据库查询视图失败', target_view_name)
            if source_view_name.upper() != str(target_view_name).upper():
                is_success = 'N'
            else:
                is_success = 'Y'
            try:  # 将以上比对的数据保存在目标库的表里
                postgresql_cur.execute("""insert into data_compare
                                            (id,
                                                        source_db_name,
                                                        source_table_name,
                                                        source_rows,
                                                        db_type,
                                                        target_table_name,
                                                        target_rows,
                                                        is_success
                                                        ) values(%s,'%s','%s',%s,'%s','%s',%s,'%s')""" % (
                    table_id, sourcedb.upper(), source_view_name.upper(), 0,
                    'VIEW',
                    target_view_name.upper(),
                    0, is_success.upper()))
                postgresql_conn.commit()
            except Exception as e:
                print(e, '数据比对结果保存在目标表失败')
                postgresql_conn.rollback()


if __name__ == "__main__":
    if platform.system().upper() == 'WINDOWS':
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleMode(kernel32.GetStdHandle(-10), 128)
    k = PrettyTable(field_names=["Mysql data compare with PG"])
    k.align["Mysql data compare with PG"] = "c"  # 以name字段左对齐
    k.padding_width = 1  # 填充宽度
    k.add_row(["Powered By: [DBA Group] of Infrastructure Research Center"])
    print(k.get_string(sortby="Mysql data compare with PG", reversesort=False))
    print('\n源MySQL数据库连接信息: ' + 'ip:' + mysql_host + ':' + str(mysql_port) + ' 数据库名称:' + str(mysql_database))
    print('\n目标postgresql数据库连接信息: ' + '用户名:' + postgresql_user + ' ip:' + postgresql_host + ':' + str(
        postgresql_port) + ' 数据库名称: ' + postgresql_database + '\n')
    table_prepare()
    data_compare_single(mysql_database, postgresql_user)
    print('表结果比较如下:')
    postgresql_cur.execute(
        """SELECT id,source_table_name,source_rows,db_type,target_table_name,target_rows,is_success,to_char(compare_time, 'MM-DD HH24:MI:SS') FROM data_compare""")
    data_compare_out = postgresql_cur.fetchall()
    tb = PrettyTable()
    tb.field_names = ['id', 'source_name', 'source_rows', 'type', 'object_name',
                      'target_rows', 'is_success', 'compare_time']
    tb.align['id'] = 'l'
    tb.align['source_name'] = 'l'
    tb.align['source_rows'] = 'l'
    tb.align['type'] = 'l'
    tb.align['object_name'] = 'l'
    tb.align['target_rows'] = 'l'
    tb.align['is_success'] = 'l'
    for v_data_compare_out in data_compare_out:
        tb.add_row(list(v_data_compare_out))
    print(tb)
    """
    输出表迁移失败或者创建失败的对象
    """
    print('以下数据迁移失败或者对象不存在:')
    postgresql_cur.execute(
        """SELECT id,source_table_name,source_rows,db_type,target_table_name,target_rows,is_success,target_rows-source_rows,to_char(compare_time, 'MM-DD HH24:MI:SS') FROM data_compare WHERE  is_success='N'""")
    data_compare_out = postgresql_cur.fetchall()
    tb = PrettyTable()
    tb.field_names = ['id', 'source_name', 'source_rows', 'type', 'object_name',
                      'target_rows', 'is_success', 'delta', 'compare_time']
    tb.align['id'] = 'l'
    tb.align['source_name'] = 'l'
    tb.align['source_rows'] = 'l'
    tb.align['type'] = 'l'
    tb.align['object_name'] = 'l'
    tb.align['target_rows'] = 'l'
    tb.align['is_success'] = 'l'
    tb.align['delta'] = 'l'
    for v_data_compare_out in data_compare_out:
        tb.add_row(list(v_data_compare_out))
    print(tb)
    print('数据比较已结束，请查看目标表"' + postgresql_user + '.DATA_COMPARE"获取详细信息')
    postgresql_cur.close()
    postgresql_conn.close()
