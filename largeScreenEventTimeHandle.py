# -*- coding: utf-8 -*-

# @File    : ——————
# @Date    : 2019-12-09
# @Author  : Wu weigang


import pymongo
from bson.objectid import ObjectId
import logging
from logging import log
import json
from builtins import set
from _ast import In, If
from _overlapped import NULL
from json.decoder import NaN
from _datetime import datetime

class LargeScreenEntityHandle():
    def __init__(self):
#         mongodb://root:gold123456@10.8.1.10:27017/messageCenter
        
        self.client = pymongo.MongoClient('mongodb://root:gold123456@10.8.1.10:27017/messageCenter') # 连接准生产数据库
        self.db = self.client['messageCenter'] # 连接表
        self.collection = self.db['LargeScreenEntity'] # 指定集合
        self.CONSTANT_EVENTTIME = 'eventTime';
        
        
    def field_event_time_init(self):
#         storage = self.collection.find({'messageType': 'STORAGE'},{'_id': False})
        storage = self.collection.find({'messageType': 'BUSINESS'},{'_id': False})
#         storage = self.collection.find({"_id": ObjectId("5de9f3d6219d5821dc031e08")},{'_class':False});
#         storage = self.collection.find({"messageContent":{'$regex':'阿里cc'} },{'_class':False});
#         storage = self.collection.find({"messageContent":{'$regex':'无锡市引源不锈钢有限公司'} },{'_class':False});
        count = storage.count();
#         log.info(count);
        print ("总计查询到   %s 条数据！"%(count));
        
        error_list = list();
        start_time = datetime.now();
        print('开始处理》》》   %s'%(start_time));
        # 循环获取content中的eventTime设置给新字段
        for index,i in enumerate(storage):
            try:
                contentJson = json.loads(i.get('messageContent'));
                if (self.CONSTANT_EVENTTIME in contentJson):
                    # messageContent中有eventTime，则增加eventTime字段
                    i[self.CONSTANT_EVENTTIME] = contentJson[self.CONSTANT_EVENTTIME];
                    self.collection.update({'_id':i.get('_id')},{'$set':i});
                else:
                    # messageContent没中有eventTime，则eventTime字段设为null
                    i[self.CONSTANT_EVENTTIME] = '';
                    self.collection.update({'_id':i.get('_id')},{'$unset':{'eventTime':''}});
                print('处理第 %s 条数据完成！剩余条数%s'%(index + 1,count-index-1));
            except Exception as e:
                error_msg = '处理第 %s 条数据发生异常：%s 需要手动处理！异常数据：%s'%(index + 1,e,i);
                print(error_msg);
                error_list.append(error_msg);
                pass
            continue
        end_time = datetime.now();
        print('结束处理《《《   %s'%(end_time));
        print('总计用时：  %s'%(end_time-start_time));
        
        if  error_list:
            print('以下数据处理时发生异常需要手工矫正: ');
            for error in error_list:
                print(error);

if __name__ == '__main__':
    handle = LargeScreenEntityHandle()
    handle.field_event_time_init()
