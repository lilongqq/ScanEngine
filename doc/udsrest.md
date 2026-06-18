# UDSREST 策略下发模板
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
                "ak": "4QDF27EEW9V5MPK0P2XM"
            },
            "resources": {
                "last_write_time": {
                    "gt": 1745303400,   // 文件修改时间大于2025-04-22T14:30:00，10位的时间戳，单位秒
                    "lt": 1745307000    // 文件修改时间小于2025-04-22T15:30:00，这是从服务端过滤的，可以理解为起止时间
                },
                "max_keys": 1000, // 分页文件大小
                "filters": {} // 过滤条件，目前目前客户没实现，传个无内容对象就行
            },
            "request_limit": { // 这个示例模块表示1秒的请求次数不允许超过10次，目前只给国网非结构化API用，当无限制时，不用传这个节点
                "period": 1, // 时间周期，单位秒
                "counts": 10 // 请求次数
            }
        },
        "action": {
            "method": "file",
            "url_enabled": "0",
            "cls": "dataprocessing.storage.MinioStorage",
            "url_distinguish_count": "",
            "url_distinguish_type": ""
        },
        "cls": "UDSREST",
        "filters": {},
        "rules_id": "bac782ed656236dd95164756c76c802b"
    }
}
```
