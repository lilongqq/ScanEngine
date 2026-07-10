# 致远 OA 服务检查报告

## 一、参考资料

- `token获取信息.docx` - 致远 V5/V8.2 REST 接口调用规范
- `文件下载信息.docx` - 文档服务管理接口
- `人员信息.docx` - 组织模型管理接口（暂未使用）
- `ScanEngine/crawling/files/seeyoncrawler.py` - 当前实现

> **客户版本**：致远 OA V8.2（属于 V9.0SP1 及以前版本）

---

## 二、问题清单

### 🔴 问题 1：Token 有效期设置错误

**文档原话**（[token获取信息.docx](token获取信息.docx) 第 5 节）：
> Token 的生命周期为 **15 分钟**，如果 15 分钟无调用，Token 将失效，失效以后调用返回 401，提示"Invalid token, please authenticate again"。

**当前代码**（[seeyoncrawler.py:69](ScanEngine/crawling/files/seeyoncrawler.py#L69)）：
```python
self.token_expires_in = 23 * 60   # 23 分钟 ❌
```

**影响**：当前代码的 token 续期窗口比实际有效期长 8 分钟。在 token 已失效的情况下仍会使用旧 token 调用文件下载接口，导致 401 错误。

**修复**：
```python
self.token_expires_in = 15 * 60   # 15 分钟（与文档一致）
```

---

### 🟡 问题 2：Token 刷新时机偏激进

**当前代码**（[seeyoncrawler.py:74-79](ScanEngine/crawling/files/seeyoncrawler.py#L74-L79)）：
```python
if self.token and (current_time - self.last_refresh_time) < (self.token_expires_in - 60):
    return self.token
```

提前 60 秒刷新，剩余有效期 14 分钟。

**建议**：保持 60 秒安全余量即可，无需调整。修复问题 1 后这条自动正确。

---

### ✅ 问题 3：文件下载接口（已正确）

**文档原话**（[文件下载信息.docx](文件下载信息.docx)）：
> 此下载文件接口，从 V10.0 之后的版本，已经禁止使用。**V9.0SP1 及以前的版本可以使用**。
>
> `GET http://ip:port/seeyon/rest/attachment/file/{ctp_file_id}?fileName={文件名}&token={}`

**当前代码**（[seeyoncrawler.py:144-160](ScanEngine/crawling/files/seeyoncrawler.py#L144-L160)）：
```python
def get_file(self, node):
    ctp_file_id = node.get('ctp_file_id')
    token = node.get('token') or self.token or self.auth.get_token()
    params = {'fileName': node.get('file_name', ''), 'token': token}
    f = SpooledTemporaryFile(max_size=16 * 1024 * 1024)
    resp = self.session.get(
        f'{self.base_url}/seeyon/rest/attachment/file/{ctp_file_id}',
        params=params, stream=True, verify=False
    )
```

**结论**：8.2 版本可用，URL 路径、查询参数、token 位置均与文档一致。✅ 无需修改。

---

### ✅ 问题 4：Token 获取（已正确）

**文档原话**：
> 基于 POST 的认证示例（推荐）
> ```
> POST /seeyon/rest/token/
> Body: { "userName": "...", "password": "...", "loginName": "..." }
> ```

**当前代码**（[seeyoncrawler.py:80-97](ScanEngine/crawling/files/seeyoncrawler.py#L80-L97)）：完全符合 POST 方式 + loginName 模拟登录 + Content-Type: application/json。

**结论**：✅ 无需修改。

---

### 🔴 问题 8：NFS 隔离写回缺少 store_file / empty_file 方法

**发现方式**：`log/Rest.log` 中 29 处 `AttributeError: 'NFSOne' object has no attribute 'empty_file'`，29 处 `'NFSOne' object has no attribute 'store_file'`

**影响**：`/separate` 接口和 `/restore` 接口全部返回 500（服务器内部异常），NFS 隔离功能完全不可用。

**修复**：向三个代码库（ScanEngine、ScanEngineSlave、ScanEngineMaster）的 `NFS` 基类添加 `store_file`、`empty_file`、`delete_file` 方法。

---

### 🔴 问题 9：store_file — CREATE 响应句柄 KeyError 风险

**发现方式**：代码审查

**原因**：NFS CREATE 响应中 `post_op_fh3` 的 `handle` 键仅在 `present == 1` 时存在。原代码直接访问 `create3res['resok']['obj']['handle']['data']`，当服务器返回 `present == 0` 时抛 `KeyError`。

**修复**：先检查 `present` 标志，缺失时通过 `nfsv3.lookup` 获取文件句柄。

---

### 🔴 问题 10：store_file — pyNfsClient write() 损坏二进制数据

**发现方式**：代码审查 + 阅读 pyNfsClient 0.1.5 源码

**原因**：`NFSv3.write()` 内部调用 `str_to_bytes(content)`，Python 3 中对 `bytes` 输入执行 `str(b'...').encode()`（先转字符串表示再 UTF-8 编码），将任意二进制数据损坏为 ASCII 表示串。

**修复**：新增 `_write_binary()` 方法，绕过 `str_to_bytes`，直接调用 pyNfsClient 内部 XDR packer 和 `nfs_request`，保证二进制写入完整性。

---

### 🟡 问题 11：DBProxy BLOB 轨迹缺失日志不足

**发现方式**：`log/Rest.log` 中 6 处 `DBProxyError: trace empty`，无任何上下文，无法定位具体表/行。

**影响**：POST /download 返回 HTTP 500，无法判断哪些表/字段触发问题，排查困难。

**修复**：`dbpcrawler.py` 的 `get_file()` 在抛出异常前记录 WARNING，包含 schema、table、pk 列名、pk 值。

---

### ✅ 问题 5：文档列表字段映射（已正确）

**文档对象属性**（[文件下载信息.docx](文件下载信息.docx)）：
| 文档字段 | 当前 `_parse_doc_list` | 状态 |
|---|---|---|
| `fr_size` | `'fr_size': doc.get('fr_size')` | ✅ |
| `fr_mine_type` | `'fr_mine_type': doc.get('fr_mine_type')` | ✅ |
| `fr_create_time` | `'fr_create_time': doc.get('fr_create_time')` | ✅ |
| `is_folder` | 参与 `'type': 'file' if not doc.get('is_folder') else 'dir'` | ✅ |
| `fr_id`, `fr_name` | 已映射 | ✅ |
| `fr_create_username` | 已映射 | ✅ |

**结论**：✅ 无需修改。

---

### ⚠️ 问题 6：Token 绑定人员校验

**文档原话**（[文件下载信息.docx](文件下载信息.docx)）：
> token 必须绑定人员，请求 token 必须 loginName 参数。

**当前代码**：[seeyoncrawler.py:83](ScanEngine/crawling/files/seeyoncrawler.py#L83) 中获取 token 时已传 `loginName`，**OK**。

但**没做绑定结果校验**：当前代码只读取 `result.get('id')` 作为 token，**没检查 `bindingUser` 字段**。如果 loginName 拼写错误或人员不存在，OA 会返回 token 但 `bindingUser` 为空，此时下载文件会 401。

**建议（可选优化）**：在拿到 token 后，校验 `bindingUser` 是否存在，缺失则记 error 日志。

---

### 🟡 问题 7：CTP_FILE 表查询字段未做字段名校验

**当前代码**（[seeyoncrawler.py:19](ScanEngine/crawling/files/seeyoncrawler.py#L19)）：
```python
_CTP_FIELDS = ('ID', 'FILE_NAME', 'FILE_SIZE', 'UPDATE_DATE', 'MIME_TYPE', 'TYPE', 'CATEGORY')
```

**风险**：客户的 CTP_FILE 表如果字段名有差异（大小写、空格），DBProxy 查询会失败。

**建议**：schema_name 改成客户可配置即可（已支持），字段名保持硬编码（致远标准表不会有差异）。

---

## 三、待修改项

| 优先级 | 位置 | 修改 | 状态 |
|---|---|---|---|
| 🔴 高 | [seeyoncrawler.py:69](ScanEngine/crawling/files/seeyoncrawler.py#L69) | `23 * 60` → `15 * 60` | ✅ 已修复 |
| 🟡 中 | [seeyoncrawler.py:88-93](ScanEngine/crawling/files/seeyoncrawler.py#L88-L93) | 拿到 token 后校验 `bindingUser` | ✅ 已修复 |
| 🔴 高 | [seeyoncrawler.py:242-245](ScanEngine/crawling/files/seeyoncrawler.py#L242-L245) | 401 后未重置 token，后续所有下载连续失败 | ✅ 已修复（2026-06-09） |
| 🔴 高 | [seeyoncrawler.py:232](ScanEngine/crawling/files/seeyoncrawler.py#L232) | 文件下载无 timeout，OA 卡死时线程永久挂起 | ✅ 已修复（2026-06-09） |
| 🟡 中 | [seeyoncrawler.py:380-386](ScanEngine/crawling/files/seeyoncrawler.py#L380-L386) | DBProxy 连接未显式关闭，依赖 GC | ✅ 已修复（2026-06-09） |
| 🟡 中 | [seeyoncrawler.py:86-90](ScanEngine/crawling/files/seeyoncrawler.py#L86-L90) | DBProxy 返回 size 是字符串，未转 int | ✅ 已修复（2026-06-09） |
| 🟢 低 | [seeyoncrawler.py:280](ScanEngine/crawling/files/seeyoncrawler.py#L280) | docstring 主键优先级描述与代码不一致 | ✅ 已修复（2026-06-09） |
| 🟢 低 | [defination.py:840](ScanEngine/defination.py#L840) | seeyon 定义 cls 用了全路径类名而非 classmap key | ✅ 已修复（2026-06-09） |
| 🔴 高 | [nfscrawler.py] NFS 基类 | 添加 store_file / empty_file / delete_file | ✅ 已修复（2026-06-15） |
| 🔴 高 | [nfscrawler.py] store_file | CREATE present=0 时 KeyError → 增加 lookup 回退 | ✅ 已修复（2026-06-15） |
| 🔴 高 | [nfscrawler.py] store_file | write() 损坏二进制数据 → 改用 _write_binary | ✅ 已修复（2026-06-15） |
| 🟡 中 | [dbpcrawler.py] get_file | trace empty 改为带 schema/table/pk 上下文的 WARNING | ✅ 已修复（2026-06-15） |

## 四、已修复详情

### 修复 1：Token 有效期

**文件**：[seeyoncrawler.py:69](ScanEngine/crawling/files/seeyoncrawler.py#L69)

```diff
- self.token_expires_in = 23 * 60
+ self.token_expires_in = 15 * 60
```

### 修复 2：Token 绑定人员校验

**文件**：[seeyoncrawler.py:88-93](ScanEngine/crawling/files/seeyoncrawler.py#L88-L93)

```diff
  if resp.status_code == 200:
-     self.token = resp.json().get('id')
+     resp_data = resp.json()
+     self.token = resp_data.get('id')
+     if not resp_data.get('bindingUser'):
+         self.logger.error('token response missing bindingUser: loginName=%s may be invalid', self.login_name)
      self.last_refresh_time = current_time
      return self.token
```

---

## 四-2、联调修复详情（2026-06-09）

### 修复 3：SeeyonIterator 完整重构 — 动态解析 resources 树

**问题**：原代码写死了 CTP_FILE 表名和 7 个固定字段，客户换表名或换字段就无法运行。同时 web 侧传入的 column 节点被当成文件节点处理，导致 `ctp_file_id=None`。

**修复**：完整重构 SeeyonIterator，新增三个辅助函数：
- `_find_table_node(node)` — 递归查找 resources 树中 `layer='table'` 的节点
- `_row_to_node(fields, row, ctp_id_field)` — 根据实际字段列表将 DBProxy 行数据转为文件节点
- `_detect_ctp_id_field(col_nodes, explicit)` — 从列定义推断主键列，优先级：显式指定 > pkColumn/userPk=YES > isPath=YES > 第一列

**文件**：[seeyoncrawler.py](ScanEngine/crawling/files/seeyoncrawler.py)

### 修复 4：主键推断优先级修正

**问题**：`_detect_ctp_id_field` 原先 isPath=YES 优先于 pkColumn/userPk=YES，导致 MIME_TYPE（含 `/`，被 DBProxy 标记为 isPath=YES）被错误选为 ID 列。

**修复**：交换优先级，pkColumn/userPk=YES 在 isPath=YES 之前。

```diff
  def _detect_ctp_id_field(col_nodes, explicit=None):
-     # isPath=YES 优先
+     # pkColumn/userPk=YES 优先
      for c in col_nodes:
          if c.get('pkColumn') == 'YES' or c.get('userPk') == 'YES':
              return c['name']
      for c in col_nodes:
          if c.get('isPath') == 'YES':
              return c['name']
```

### 修复 5：401 后自动重置 token

**问题**：文件下载收到 401 后，`SeeyonAuth` 仍认为 token 在有效期内（14 分钟窗口），不会刷新。后续所有文件全部 401 失败，持续到 token 自然过期。

**修复**：收到 401 时主动置空 token 和 last_refresh_time，下次调用 `get_token()` 会立即重新获取。

**文件**：[seeyoncrawler.py:242-245](ScanEngine/crawling/files/seeyoncrawler.py#L242-L245)

```diff
  f.close()
+ if resp.status_code == 401:
+     self.auth.token = None
+     self.auth.last_refresh_time = 0
  self.logger.error(...)
  raise Exception(...)
```

### 修复 6：文件下载增加 timeout

**问题**：`SeeyonClient.get_file()` 的 `session.get()` 没有设置 `timeout`，OA 服务卡死时线程永久阻塞。

**修复**：增加 `timeout=(10, 300)`，连接超时 10 秒，读取超时 300 秒（考虑大文件）。

**文件**：[seeyoncrawler.py:232](ScanEngine/crawling/files/seeyoncrawler.py#L232)

```diff
  resp = self.session.get(
      url,
      params=params,
      stream=True,
      verify=False,
+     timeout=(10, 300)
  )
```

### 修复 7：DBProxy 连接显式关闭

**问题**：`SeeyonIterator` 创建的 `DBProxyBatch` 连接迭代结束后不主动关闭，全靠 `__del__` 垃圾回收，可能导致连接泄漏。

**修复**：为 SeeyonIterator 增加 `__del__` 方法，主动 logout + close。

**文件**：[seeyoncrawler.py:380-386](ScanEngine/crawling/files/seeyoncrawler.py#L380-L386)

```python
def __del__(self):
    if self._db_client:
        try:
            self._db_client.logout()
            self._db_client.close()
        except Exception:
            pass
```

### 修复 8：size 字段类型转换

**问题**：DBProxy 返回的字段值全是字符串，`node['size']` 是 `'10240'` 而非整数。下游 `hincrby` 虽然能处理字符串，但不规范。

**修复**：`_row_to_node` 中对 `size` 字段做 `int()` 转换。

**文件**：[seeyoncrawler.py:86-90](ScanEngine/crawling/files/seeyoncrawler.py#L86-L90)

```diff
  for col_name, value in d.items():
      alias = _FIELD_ALIAS.get(col_name.upper())
      if alias and value is not None:
+         if alias == 'size':
+             try:
+                 value = int(value)
+             except (ValueError, TypeError):
+                 value = 0
          node[alias] = value
```

### 修复 9：rate_limit 限速支持

**新增功能**：web 侧可通过 auth 中的 `rate_limit` 字段控制 DBProxy 查询频率。

| `rate_limit` 值 | 行为 |
|---|---|
| 不传 / 空 | 默认 500 次/分钟 |
| `"0"` | 不限速 |
| `"10"` | 每次查询间隔至少 6 秒（60/10） |

**文件**：[seeyoncrawler.py:35-49](ScanEngine/crawling/files/seeyoncrawler.py#L35-L49)（`_RateLimiter` 类），[seeyoncrawler.py:321-322](ScanEngine/crawling/files/seeyoncrawler.py#L321-L322)（初始化），[seeyoncrawler.py:345](ScanEngine/crawling/files/seeyoncrawler.py#L345)（查询前等待）

### 修复 10：defination.py cls 修正

**问题**：`defination.py` 中 seeyon 定义的 `strategy.cls` 填了全路径类名 `crawling.files.seeyoncrawler.SeeyonIterator`，但 `Discovery.__init__` 通过 `classmap.iterator.get(cls)` 查找，key 应该是 `'SEEYON'`。

**修复**：

```diff
- 'cls': 'crawling.files.seeyoncrawler.SeeyonIterator',
+ 'cls': 'SEEYON',
```

### 联调验证结果

mock 环境（mock_seeyon.py 模拟 OA 服务）+ 真实 DBProxy 联调，全链路跑通：

```
DBProxy query: schema=mysql, table=ctp_file → 返回 4 行
  1001 测试文档.docx  → status=200 下载成功 → kafka[file_topic]
  1002 测试表格.xlsx  → status=200 下载成功 → kafka[file_topic]
  1003 测试图片.png   → status=200 下载成功 → kafka[file_topic]
  1004 测试PDF.pdf    → status=200 下载成功 → kafka[file_topic]
count: 4, size: 117760
```

---

## 五、相关文件

- [ScanEngine/crawling/files/seeyoncrawler.py](ScanEngine/crawling/files/seeyoncrawler.py) - 致远 OA 主文件（迭代器 + 客户端）
- [ScanEngine/common/classmap.py](ScanEngine/common/classmap.py) - `SEEYON` 注册
- [ScanEngine/server/restserver.py](ScanEngine/server/restserver.py) - `/opa`、`/download` 接口
- [ScanEngine/crawling/database/dbproxy/dbpcrawler.py](ScanEngine/crawling/database/dbproxy/dbpcrawler.py) - DBProxy 客户端
- [ScanEngine/defination.py](ScanEngine/defination.py) - 本地调试任务定义（含 seeyon 示例）
- [ScanEngine/doc/seeyon.md](ScanEngine/doc/seeyon.md) - 致远 OA 任务定义文档（字段说明 + 常见问题）
- [mock_seeyon.py](mock_seeyon.py) - 模拟 OA REST 服务（联调用）
- [token获取信息.docx](token获取信息.docx) - Token 接口规范
- [文件下载信息.docx](文件下载信息.docx) - 文档/下载接口规范
- [人员信息.docx](人员信息.docx) - 组织模型接口（暂未使用）

---

## 六、临时调试日志 ✅ 已删除（2026-06-29）

所有 `[DEBUG-SEEYON]` 前缀的调试日志已从以下文件中全部删除：

- [ScanEngine/server/restserver.py](ScanEngine/server/restserver.py) — `download()`、`operate_job()` 方法
- [ScanEngine/crawling/files/seeyoncrawler.py](ScanEngine/crawling/files/seeyoncrawler.py) — `SeeyonIterator.__init__()`、`__next__()`、`get_file()`、`SeeyonClient.get_file()`
- [ScanEngine/crawling/database/dbproxy/dbpcrawler.py](ScanEngine/crawling/database/dbproxy/dbpcrawler.py) — `DBProxyIterator.__init__()`

---

## 七、NFS 隔离 / DBProxy BLOB 修复详情（2026-06-15）

### 修复 11：NFS 基类补全 store_file / empty_file / delete_file

**问题来源**：`log/Rest.log` — 29 处 `AttributeError: 'NFSOne' object has no attribute 'empty_file'`

**影响接口**：`POST /separate`（隔离）、`POST /restore`（回写）

**修改文件**：
- [ScanEngine/crawling/files/nfscrawler.py](ScanEngine/crawling/files/nfscrawler.py)
- [MyCluster/ScanEngineSlave/crawling/files/nfscrawler.py](MyCluster/ScanEngineSlave/crawling/files/nfscrawler.py)
- [MyCluster/ScanEngineMaster/crawling/files/nfscrawler.py](MyCluster/ScanEngineMaster/crawling/files/nfscrawler.py)

三个文件均在 `NFS` 基类 `get_file()` 后新增三个方法：

| 方法 | 用途 |
|---|---|
| `store_file(node, f)` | 将文件写入隔离目标路径（restore 阶段调用） |
| `empty_file(node)` | 在源路径创建同名空文件占位（separate 阶段 placeholder=1 时调用） |
| `delete_file(node / path)` | 删除文件，支持 `MutableMapping` 和 `str` 两种参数 |

---

### 修复 12：store_file — CREATE present=0 时增加 lookup 回退

**问题**：NFS `CREATE` 响应中 `obj` 是 `post_op_fh3` 结构，`handle` 键仅在 `present == 1` 时存在。原代码直接访问会在 `present == 0` 时抛 `KeyError`。

**修复**（三个文件一致）：

```diff
- file_fh = create3res['resok']['obj']['handle']['data']
+ obj = create3res['resok']['obj']
+ if obj.get('present'):
+     file_fh = obj['handle']['data']
+ else:
+     lookup3res = self.nfsv3.lookup(dir_fh, file_name)
+     if lookup3res['status'] != NFS3_OK:
+         raise OSError('Failed to lookup file {} after create'.format(file_name))
+     file_fh = lookup3res['resok']['object']['data']
```

---

### 修复 13：store_file — 绕过 pyNfsClient str_to_bytes 二进制数据损坏

**问题**：pyNfsClient 0.1.5 的 `NFSv3.write()` 内部调用：
```python
data = str_to_bytes(content)   # utils.py
# Python 3 实现：return str(str_v).encode()
# bytes 输入：str(b'\xff\x00') → "b'\\xff\\x00'" → .encode() → 错误字节序列
```
任何非纯 ASCII 的二进制文件写入后均损坏。

**修复**：新增 `_write_binary()` 方法，复制 `NFSv3.write()` 的 RPC 调用逻辑，但将 `data_bytes` 直接传入 `write3args`，绕过 `str_to_bytes`：

```python
def _write_binary(self, file_fh, offset, data_bytes, stable_how=2):
    from pyNfsClient.pack import nfs_pro_v3Packer, nfs_pro_v3Unpacker
    from pyNfsClient.rtypes import write3args, nfs_fh3
    from pyNfsClient.const import NFS3_PROCEDURE_WRITE
    packer = nfs_pro_v3Packer()
    packer.pack_write3args(write3args(
        file=nfs_fh3(file_fh),
        offset=offset,
        count=len(data_bytes),
        stable=stable_how,
        data=data_bytes          # 直接传 bytes，pack_opaque 原生接受
    ))
    res = self.nfsv3.nfs_request(NFS3_PROCEDURE_WRITE, packer.get_buffer(), self.nfsv3.auth)
    unpacker = nfs_pro_v3Unpacker(res)
    return unpacker.unpack_write3res()
```

`store_file` 写入循环改为：
```diff
- write3res = self.nfsv3.write(file_fh, offset, len(chunk), chunk, 2)
+ write3res = self._write_binary(file_fh, offset, chunk)
```

---

### 修复 14：DBProxy BLOB 轨迹缺失补充上下文日志

**问题来源**：`log/Rest.log` — 6 处 `DBProxyError: trace empty`，无法定位到具体表/行。

**修改文件**：
- [ScanEngine/crawling/database/dbproxy/dbpcrawler.py](ScanEngine/crawling/database/dbproxy/dbpcrawler.py)
- [MyCluster/ScanEngineSlave/crawling/database/dbproxy/dbpcrawler.py](MyCluster/ScanEngineSlave/crawling/database/dbproxy/dbpcrawler.py)
- [MyCluster/ScanEngineMaster/crawling/database/dbproxy/dbpcrawler.py](MyCluster/ScanEngineMaster/crawling/database/dbproxy/dbpcrawler.py)

```diff
  if not trace or not trace.file_key:
-     raise DBProxyError('trace empty')
+     self.logger.warning('BLOB文件轨迹缺失: %s.%s pk[%s]=%s',
+                         schema, table, pk_col, pk_val)
+     raise DBProxyError('BLOB文件轨迹缺失: {}.{} pk[{}]={}'.format(
+                         schema, table, pk_col, pk_val))
```

日志示例：
```
WARNING dbpcrawler BLOB文件轨迹缺失: dbo.FILE_TABLE pk[FILE_ID]=10023
```
