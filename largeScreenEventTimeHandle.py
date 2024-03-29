# -*- coding: utf-8 -*-

# @File    : ——————
# @Date    : 2019-12-09
# @Author  : Wu weigang

from _datetime import datetime
import json
import pymongo
import settings
# 处理LargeScreenEntity表的相应逻辑
class LargeScreenEntityHandle():

    def __init__(self):
        
        self.client = pymongo.MongoClient(settings.MONGO_MESSAGECENTER_CONNECT);
        self.db = self.client[settings.MONGO_MESSAGECENTER_DB_NAME]
        self.collection = self.db[settings.MONGO_MESSAGECENTER_TABLE_LARGESCREENENTITY]
        self.CONSTANT_EVENTTIME = 'eventTime';
    
    def field_event_time_init_batch(self):
#         storage = self.collection.find({'messageType': 'STORAGE'},{'_class': False})
#         storage = self.collection.find({'messageType': 'BUSINESS'},{'_class': False})
        storage = self.collection.find({}, {'_class': False})
#         storage = self.collection.find({"_id": ObjectId("5de9f3d6219d5821dc031e08")},{'_class':False});
#         storage = self.collection.find({"messageContent":{'$regex':'阿里cc'} },{'_class':False});
#         storage = self.collection.find({"messageContent":{'$regex':'无锡市引源不锈钢有限公司'} },{'_class':False});
        count = storage.count();
        bulk = pymongo.bulk.BulkOperationBuilder(self.collection, ordered=False)
        error_list = list();
        start_time = datetime.now();
        print('开始处理》》》   %s' % (start_time));
        # 循环获取content中的eventTime设置给新字段
        for index, i in enumerate(storage):
            try:
                contentJson = json.loads(i.get('messageContent'));
                if (self.CONSTANT_EVENTTIME in contentJson):
                    # messageContent中有eventTime，则增加eventTime字段
                    i[self.CONSTANT_EVENTTIME] = contentJson[self.CONSTANT_EVENTTIME];
                    bulk.find({"_id":i["_id"]}).upsert().update_one({'$set':i })
                else:
                    # messageContent没中有eventTime，则eventTime字段设为null
                    i[self.CONSTANT_EVENTTIME] = '';
#                     self.collection.update({'_id':i.get('_id')},{'$unset':{'eventTime':''}});
                    bulk.find({"_id":i["_id"]}).upsert().update_one({'$unset':{'eventTime':''}})

                print('处理第 %s 条数据完成！剩余条数%s' % (index + 1, count - index - 1));
            except Exception as e:
                error_msg = '处理第 %s 条数据发生异常：%s 需要手动处理！异常数据：%s' % (index + 1, e, i);
                print(error_msg);
                error_list.append(error_msg);
                pass
            continue
        
        result = bulk.execute()
        
        print ("总计查询到   %s 条数据！" % (count));
        print('modify_count>>%s' % (result.get("nModified")));
#         insert_count = result.get("nUpserted") + result.get("nInserted")
#         print('insert_count>>%s:'%(insert_count));
        end_time = datetime.now();
        print('结束处理《《《   %s' % (end_time));
        print('总计用时：  %s' % (end_time - start_time));
        
        if  error_list:
            print('以下数据处理时发生异常需要手工矫正: ');
            for error in error_list:
                print(error);

    
if __name__ == '__main__':
    handle = LargeScreenEntityHandle()
    handle.field_event_time_init_batch()
