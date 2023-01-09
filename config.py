import os


basedir = os.path.abspath(os.path.dirname(__file__))

# 日志存储路径
logger_file = os.path.join(basedir, "logs/run.log")


# 数据库链接配置, 与 impala.dbapi 创建链接时的参数一致
impala_connect_options = dict(
    host="itlubber.art",
    port=10000,
    user="hadoop",
    password="hadoop",
    database="work_zxysbl",
    auth_mechanism="PLAIN",
    kerberos_service_name="hive",
)

# 数据库链接池配置
impala_pool_options = dict(
    maxconnections=3,
    mincached=1,
    maxcached=0,
    maxshared=3,
    blocking=True,
    maxusage=None,
    setsession=[],
    ping=1,
)
