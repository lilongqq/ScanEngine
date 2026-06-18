# UDS 策略下发模板

```json
{
    "scheduler": {
        "op": "start"
    },
    "identity": "6a121001978b4aeabceca703fdc3af6f",
    "cls": "strategy.Discovery",
    "strategy": {
        "iterator": {
            "auth": {
                "endpoint": "http://27.10.112.130:18082",
                "sk": "oASkDITUBxC8LVeXxQjUYMpnqFAvPe4yNssujPo0",
                "ak": "4QDF27EEW9V5MPK0P2XM"
            },
            "resources": {
                "start_time":  "2023-01-01 00:00:00",
                "end_time":  "2025-04-08 00:00:00",
                "system_code":  "99test001",
                "deploy_code":  "99",
                "meta_data_type":  "FJGH_MT_GJBZ",
                "file_type":  "pdf",
                "page_size": 10
            }
        },
        "action": {
            "method": "file",
            "url_enabled": "0",
            "cls": "dataprocessing.storage.MinioStorage",
            "url_distinguish_count": "",
            "url_distinguish_type": ""
        },
        "cls": "UDS",
        "filters": {},
        "rules_id": "bac782ed656236dd95164756c76c802b"
    }
}
```

## 需要web下发的字段说明
### 必填字段
1. endpoint
    认证服务地址
    http://27.10.112.130:18082,
2. sk
    认证服务sk
    oASkDITUBxC8LVeXxQjUYMpnqFAvPe4yNssujPo0
3. ak
    认证服务ak
    4QDF27EEW9V5MPK0P2XM
4. start_time
    开始时间
    2023-01-01 00:00:00
5. end_time
    结束时间
    2025-04-08 00:00:00

6. system_code
    系统代码
    99test001
7. deploy_code
    部署代码
    99
8. page_size
    分页大小
    10


### 选填字段

8. meta_data_type
    元数据类型
    FJGH_MT_GJBZ
9. file_type
    文件类型
    pdf


## resources 字段说明
resource中children字段为数组，每个元素为一个资源，每个资源的字段说明如下：
1. system_code
    系统代码，每个资源的系统代码必须不同，对应不同的业务系统资源
    99test001
2. deploy_code
    部署代码， 同上
    99
3. start_time
    扫描数据的时间范围，具体每个系统的时间是否全局一致，需要根据具体业务场景进行判断，问产品的负责人，做成什么样，如果在页面认证位置传输，那么每个资源的时间范围必须一致就好了，
    如果支持个资源，时间范围不一致，那么就需要在资源中传输时间范围
4. end_time
    同上

8. meta_data_type
    元数据类型，和服务端过滤有关，可选字段，没有过滤需求就不传。
    FJGH_MT_GJBZ
9. file_type
    文件类型， 同上
    pdf

## 文件属性
我在data_p_file,传输的文件属性如下（没写JobID， RulesID等公共属性， 保持不变），
数据库上事件需要保存所有相关属性， 可以根据需要按需展示。
```json
{
    "type": "file",
    "creatorId": "99test001",
    "creatorName": "舆情监测系统",
    "updaterName": "舆情监测系统",
    "updateTime": 1741678787000,
    "version": 1,
    "bizMetas": {
        "FJGH_MT_GJBZ_NAME": "李四",
        "FJGH_MT_GJBZ_YEAR": "",
        "meta_type_name": "FJGH_MT_GJBZ",
        "meta_type_id": "2b5d0e5ef39724ea76a191d3e4a8c6d1"
    },
    "updaterId": "99test001",
    "createTime": 1741678787000,
    "dataId": "75ece0291c9b4664afd6d48d63f9b18c",
    "commonMetas": {},
    "file": {
            "fileName": "测试文件",
            "size": 178754,
            "createTime": 1741678787000,
            "id": "e1bd072592de41999f7a624409784d60",
            "suffix": "docx"
        },
    "name": "测试文件.docx",
    "path": "75ece0291c9b4664afd6d48d63f9b18c_1/测试文件.docx",
    "size": 178754,
    "version_id": "75ece0291c9b4664afd6d48d63f9b18c_1",
    "system_code": "99test001",
    "deploy_code": "99"
}
```