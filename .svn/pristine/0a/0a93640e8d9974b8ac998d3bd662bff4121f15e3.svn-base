# 致远 OA (Seeyon) 扫描任务定义

## 整体流程

1. web 侧通过 `POST /opa` 下发任务定义
2. `StrategyFactory → Scheduler → Discovery → SeeyonIterator`
3. SeeyonIterator 两条腿走路：
   - **DBProxy (Thrift)**：连接客户 MySQL，分页查文件列表表拿到文件 ID
   - **Seeyon OA (HTTP REST)**：用 token + ctp_file_id 逐个下载文件
4. 下载的文件经 md5 计算后推送到 Kafka → MinioStorage

**类映射** (`classmap.py`)：

| key | protocol（单文件） | iterator（批量扫描） |
|---|---|---|
| `SEEYON` | `SeeyonClient` | `SeeyonIterator` |

---

## 一、完整任务定义

web 侧 `POST /opa` 的 JSON body：

```yaml
identity: "7ca97684077f4c3f9baddf8ba3bcfc40"   # 任务唯一 ID，web 侧生成
op: create                                       # create / stop / manual_pause / manual_resume / secondary

scheduler:
  date_time: ""      # cron 表达式，空 = 立即执行
  op: start

cls: strategy.Discovery   # 固定

strategy:
  cls: SEEYON              # classmap key，必须是 SEEYON（不是全路径类名）
  rules_id: "ae3d0b892c8539edbb5b774adf9b7340"  # 规则 ID，增量 diff 用

  iterator:
    auth:
      # ---- OA 连接（获取 token + 下载文件）----
      base_url:   "http://192.190.20.41:8080"   # OA 服务器地址（含端口，不含路径）
      userName:   "restUser"                      # OA REST 用户名
      password:   "your_password"                 # OA REST 密码
      login_name: "loginName"                     # OA 登录名（token 必须绑定人员）

      # ---- DBProxy 连接（查文件列表表）----
      # 约定：db_ 前缀，SeeyonIterator 自动去前缀传给 DBProxyBatch
      # db_dbtype → dbtype, db_username → username, db_password → password, db_url → url
      db_dbtype:   "mysql"
      db_username: "root"
      db_password: "Spinfo@MySql@0123"
      db_url:      "jdbc:mysql://192.190.20.41:3306/"

      # ---- 限速（可选）----
      # 每分钟 DBProxy 调用次数上限
      # 不传/空 → 默认 500 次/分钟
      # "0" → 不限速
      rate_limit: "10"

    # resources: web 侧「连接测试 → 选库 → 选表 → 选字段」构建的树
    # SeeyonIterator 递归找 layer=table 节点，提取 schema/table/columns/主键
    resources:
      name: ""
      is_binary: true
      filters: {}
      children:
        - name: mysql
          is_binary: true
          schema_name: mysql
          layer: schema
          schema_def: userDef
          children:
            - name: ctp_file
              is_binary: true
              schema_name: mysql
              table_name: ctp_file
              layer: table
              children:
                - name: ID
                  layer: column
                  typeName: BIGINT
                  classification: NUMBER
                  pkColumn: "YES"       # 数据库主键
                  userPk: "YES"         # 用户指定主键
                  isPath: "NO"
                  isNullable: "NO"
                  remark: "文件唯一ID，对应 ctp_file_id"

                - name: FILE_NAME
                  layer: column
                  typeName: VARCHAR
                  classification: STRING
                  isPath: "NO"
                  userPk: "NO"
                  remark: "文件名"

                - name: FILE_SIZE
                  layer: column
                  typeName: BIGINT
                  classification: NUMBER
                  isPath: "NO"
                  userPk: "NO"
                  remark: "文件大小（字节）"

                - name: UPDATE_DATE
                  layer: column
                  typeName: DATETIME
                  classification: DATE
                  isPath: "NO"
                  userPk: "NO"
                  remark: "最后更新时间"

                - name: MIME_TYPE
                  layer: column
                  typeName: VARCHAR
                  classification: STRING
                  isPath: "YES"         # DBProxy 标记含 / 的列为 isPath=YES
                  userPk: "NO"
                  remark: "MIME类型"

                - name: TYPE
                  layer: column
                  typeName: INT
                  classification: NUMBER
                  isPath: "NO"
                  userPk: "NO"
                  remark: "文件类型（0=附件）"

                - name: CATEGORY
                  layer: column
                  typeName: VARCHAR
                  classification: STRING
                  isPath: "NO"
                  userPk: "NO"
                  remark: "分类"

    # 以下为可选的显式覆盖（一般不需要传，从 resources 树自动提取）
    # schema_name: mysql         # 显式指定 schema
    # mysql_table: ctp_file      # 显式指定表名（默认 CTP_FILE）
    # ctp_id_field: ID           # 显式指定文件 ID 列（不传则自动推断）

  filters:
    time: ""     # 时间过滤，空 = 不过滤
    size: ""     # 大小过滤
    type: ""     # 类型过滤

  action:
    cls: dataprocessing.storage.MinioStorage
    method: file
```

---

## 二、主键列推断规则

`_detect_ctp_id_field` 优先级从高到低：

| 优先级 | 条件 | 说明 |
|---|---|---|
| 1 | 显式传入 `ctp_id_field` | 最高优先 |
| 2 | `pkColumn=YES` 或 `userPk=YES` | 数据库/用户指定主键 |
| 3 | `isPath=YES` | 含路径分隔符的列（注意 MIME_TYPE 会误命中） |
| 4 | 第一列 | 兜底 |

> 例：ID 列有 `pkColumn=YES` + `userPk=YES`，MIME_TYPE 有 `isPath=YES`
> → 选 **ID**（规则 2 优先于规则 3）

---

## 三、字段映射

DBProxy 返回的列名（大写匹配）→ 文件节点属性：

| 数据库列 | 节点属性 | 说明 |
|---|---|---|
| `FILE_NAME` | `file_name` | 文件名 |
| `FILE_SIZE` | `size` | 文件大小（自动转 int） |
| `UPDATE_DATE` | `last_write_time` | 更新时间 |
| `MIME_TYPE` | `mime_type` | MIME 类型 |
| `TYPE` | `ctp_type` | 文件类型 |
| `CATEGORY` | `category` | 分类 |

不在映射表中的列保留在原始 dict，不映射到节点属性。

---

## 四、`/download` 接口

单文件下载（隔离/还原时用），`POST /download`：

```json
{
    "cls": "SEEYON",
    "auth": {
        "base_url": "http://192.190.20.41:8080",
        "userName": "restUser",
        "password": "xxx",
        "login_name": "loginName"
    },
    "ctp_file_id": "1001",
    "file_name": "测试文档.docx"
}
```

执行流程：
1. 获取 token：`POST {base_url}/seeyon/rest/token/`
2. 下载文件：`GET {base_url}/seeyon/rest/attachment/file/{ctp_file_id}?fileName={file_name}&token={token}`

---

## 五、常见问题

| 现象 | 原因 | 解决 |
|---|---|---|
| DBProxy 查询报 `错误:null` | `schema_name` 为空 | 确认 resources 树中 table 节点有 `schema_name` |
| 下载 401 `Invalid token` | token 过期 / loginName 无效 | 检查 `login_name` 是否是有效 OA 用户；V10.0+ 此接口已禁用 |
| `id_field` 选了 MIME_TYPE | `isPath=YES` 误命中 | 已修复：`pkColumn/userPk` 优先级高于 `isPath` |
| Connection refused (Docker) | 容器内 `127.0.0.1` 指向自身 | 用宿主机 IP 或 `host.docker.internal` |
| `cls` 报类找不到 | 填了全路径类名 | `strategy.cls` 必须填 `SEEYON`（classmap key） |
| `TypeError: unexpected keyword argument 'login_name'` | web 侧用 `cls=DBProxy` | 必须用 `cls=SEEYON`，不是 `DBProxy` |

---

## 六、相关文件

- `ScanEngine/crawling/files/seeyoncrawler.py` — 迭代器 + 客户端主文件
- `ScanEngine/common/classmap.py` — `SEEYON` 注册
- `ScanEngine/server/restserver.py` — `/opa`、`/download` 接口
- `ScanEngine/crawling/database/dbproxy/dbpcrawler.py` — DBProxy 客户端
- `致远OA服务检查报告.md` — OA 接口对照检查 + 调试日志记录
