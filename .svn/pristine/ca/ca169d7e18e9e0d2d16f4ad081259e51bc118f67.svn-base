# GridFS 策略下发模板
```json
{
    "scheduler": {
        "op": "start"
    },
    "identity": "381f6ad541134cc3b360714164cad2e2",
    "cls": "strategy.Discovery",
    "strategy": {
        "iterator": {
            "auth": { //这个认证和MONGO一样
                "password": "",
                "port": "27017",
                "host": "192.168.37.88",
                "username": ""
            },
            "resources": { //这里有变化，返回的列表只包含GridFS，跳过了普通的MONGO数据
                "children": [
                    {
                        "type": "file",
                        "name": "21a_filetype",
                        "path": "21a_filetype",
                        "layer": "schema",
                        "schema_name": "21a_filetype",
                        "children": [
                            {
                                "name": "fs", //这里就是跳过普通数据库资源的地方，fs表示fs.files, fs.chunks两个集合，文件是在这里存储的
                                "path": "21a_filetype/fs",
                                "schema_name": "21a_filetype",
                                "layer": "table"
                            }
                        ],
                        "schema_def": "userDef"
                    }
                ] ,
                "name": "",
            }
        },
        "action": {
            "method": "file",
            "url_enabled": "0",
            "cls": "dataprocessing.storage.MinioStorage",
            "url_distinguish_count": "",
            "url_distinguish_type": ""
        },
        "filters": {},
        "cls": "GRIDFS", //这里的类型是GRIDFS，测试链接也是一样的
        "rules_id": "d479488fa318339fb1a6e1066b50434d"
    }
}
```

## 备注说明
GridFS是一个存储在MONGODB上的文件系统，和对象存储类似，而不能当作是一个数据库。
经过GridFS接口的组织合并，输出的是文件，而不是数据库的表。

