################## mongoDB配置 ############################

# ip
MONGO_MESSAGECENTER_IP = "10.8.1.10"
# 端口
MONGO_MESSAGECENTER_PORT = "27017"
# 账号
MONGO_MESSAGECENTER_USERNAME = "root"
# 密码
MONGO_MESSAGECENTER_PASSWORD = "gold123456"
# 数据库
MONGO_MESSAGECENTER_DB_NAME = "messageCenter"
# 表
MONGO_MESSAGECENTER_TABLE_LARGESCREENENTITY = "LargeScreenEntity"
# 数据库连接
MONGO_MESSAGECENTER_CONNECT = "mongodb://"+ MONGO_MESSAGECENTER_USERNAME + ":" + MONGO_MESSAGECENTER_PASSWORD+"@"+MONGO_MESSAGECENTER_IP+":"+MONGO_MESSAGECENTER_PORT+"/"+MONGO_MESSAGECENTER_DB_NAME

################## mongoDB配置 ############################