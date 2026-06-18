# FTP 策略下发模板
```json
{
    "scheduler": {
        "op": "start"
    },
    "identity": "7f62e89381ec43368b96d9b0cf0af0ad",
    "cls": "strategy.Discovery",
    "strategy": {
        "iterator": {
            "auth": {
                "password": "Spinfo0123",
                "port_p": false,
                "port": 21,
                "host": "192.168.37.109",
                "ssl_wrap": false,
                "encoding": "UTF-8",
                "timeout": 30,
                "username": "root",
                "mode": "MLSD", // 默认MLSD, 可选LIST
                "parser": "Unix" // 默认Unix， 可选MS
            },
            "resources": {
                "children": [
                    {
                        "path": "liuxl",
                        "children": [
                            {
                                "path": "/liuxl/内嵌OCR识别_16",
                                "children": [],
                                "name": "内嵌OCR识别_16",
                                "type": "file",
                                "last_write_time": 1451580495
                            }
                        ],
                        "name": "liuxl",
                        "type": "file",
                        "last_write_time": 1713398366
                    }
                ],
                "name": ""
            }
        },
        "action": {
            "method": "file",
            "url_enabled": "0",
            "cls": "dataprocessing.storage.MinioStorage",
            "url_distinguish_count": "",
            "url_distinguish_type": ""
        },
        "cls": "FTP",
        "filters": {
            "size": {
                "gt": 0
            }
        },
        "rules_id": "af1e3377f0bf35e3b6da604ca0fc6848"
    }
}
```

## 新增的auth字段说明
1. mode
包含两个选项
- MLSD
此选项是现代的FTP协议支持的命令，提取文件属性等性能比较高，优先使用。
- LIST
此选项是legacy的FTP协议支持的命令，提取文件属性等性能较低，当不支持MLSD时使用此选项。

2. parser
包含两个选项, 这个参数只有在mode为LIST时才有效
- Unix
这个选项是Unix系统的FTP服务器，用于解析提取文件属性，需要注意的是很多服务器软件即时在windows上运行，但是返回的属性是Unix的格式（比如Server U）。
所以优先使用此选项
- MS
这个选项是Windows系统的FTP服务器，用于解析提取文件属性，需要注意的是很多服务器软件即时在windows上运行，但是返回的属性是Unix的格式（比如Server W）。