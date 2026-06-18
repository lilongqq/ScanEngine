# 隔离接口修复说明

> 日期：2026-06-16
> 涉及服务：ScanEngine（含单 SE 部署、MyCluster Master/Slave）
> 影响范围：文件隔离（separate）和文件还原（restore）接口

## 问题概述

手动隔离文件时，接口返回 5000（服务器内部异常），但部分场景下隔离文件已经成功复制到目标服务器，源文件未被替换为占位符；部分场景下服务直接卡死。

## 根本原因

经排查，发现以下 4 个 Bug：

### Bug 1：NFS `empty_file` 挂载子目录失败（status=2 NOENT）

**位置**：`crawling/files/nfscrawler.py:empty_file`

**症状**：调用 `separate` 接口隔离 NFS 源文件时，`empty_file` 调用 `mount.mnt(dir_path)` 失败，NFS 服务器返回 `status=2`（NOENT），导致整个接口返回 5000。

**根因**：
- NFS `mnt()` 必须传入**导出根路径**（如 `/opt/BZCP`），不能传子目录（如 `/opt/BZCP/2.1/2.1-b/样本文件`）
- NFS 服务器只导出根路径，子目录对 mountd 不可见
- 旧代码用 `os.path.dirname(path)` 算出子目录直接 `mnt()`，必然失败

**修复**：
1. `get_nodes` 在遍历子目录时，把父目录的 `handle` 作为 `dir_handle` 注入到子节点
2. `empty_file` 优先用 `node['dir_handle']` 直接操作；没有 `dir_handle` 时回退到 `mnt(dir_path)`（快速失败，不阻塞）

```python
# get_nodes 中注入 dir_handle
for node in nodes:
    node['path'] = os.path.join(path, node['name'])
    node['dir_handle'] = dir_handle

# empty_file 优先用 dir_handle
if node.get('dir_handle'):
    dir_fh = bytes.fromhex(node['dir_handle'])
else:
    mount3res = self.mount.mnt(dir_path)
    if mount3res['status'] != MNT3_OK:
        raise NFSError(mount3res)
    dir_fh = mount3res['mountinfo']['fhandle']
```

### Bug 2：SFTP `store_file` 使用 `f.getvalue()` 报错

**位置**：`crawling/files/sftpcrawler.py:store_file`

**症状**：FTP → SFTP 隔离时，文件复制到 SFTP 目标失败，接口返回 4000（远程隔离服务器连接失败）。

**根因**：
- `f` 是 `SpooledTemporaryFile` 对象（不是 `BytesIO`），**没有 `getvalue()` 方法**
- `remote_file.write(f.getvalue())` 抛 `AttributeError`
- 异常被 `ClientPool.wrapper` 的 `except` 静默吞掉，日志无任何记录

**修复**：改用 `shutil.copyfileobj(f, remote_file)`，支持任意 file-like 对象：

```python
import shutil

remote_file = self.client.open(sep_path + '/' + file_name, 'wb')
f.seek(0)
shutil.copyfileobj(f, remote_file)
remote_file.close()
```

### Bug 3：SFTP `empty_file` 写空字符串导致占位符失败

**位置**：`crawling/files/sftpcrawler.py:empty_file`

**症状**：SFTP 占位符创建失败，源文件未被替换为空文件。

**根因**：
- `remote_file.write('')` 在 paramiko SFTP 客户端上不抛异常，但**不会真正清空文件**
- 文件仍保持原内容，源文件访问未被切断

**修复**：`open('wb')` 直接 close 创建 0 字节文件：

```python
remote_file = self.client.open(node['path'], 'wb')
remote_file.close()
```

### Bug 4：`separate` 异常导致 5000

**位置**：`server/restserver.py:separate`

**症状**：即使隔离文件已成功复制到目标，只要 `empty_file` 失败，整个接口返回 5000，前端误判为"隔离失败"。

**根因**：`empty_file`（占位符）失败被外层 `except Exception` 捕获，等同于整体失败。

**修复**：`empty_file` 单独 try/catch，失败只记日志，不影响主流程返回 2000：

```python
if placeholder:
    try:
        src_client.empty_file(node)
    except Exception:
        self.logger.error(traceback.format_exc())
else:
    src_client.delete_file(node)
return self.json_data({'code': 2000, 'message': 'succeed'})
```

## 调试日志清理

为方便排障，临时添加了详细调试日志，问题定位后清理：

| 位置 | 清理内容 |
|------|---------|
| `crawling/files/ftpcrawler.py` | 删除 `RETR` / `path` / `mlsd` / 每条目录项日志 |
| `crawling/files/sftpcrawler.py` | 删除 `getfo` / `path` / `node` 日志 |
| `crawling/files/smbcrawler.py` | 删除 `store_file` 内部所有 INFO 日志、`delete_file` 调试日志 |
| `common/conntool.py` | 删除 `ClientPool.wrapper` 8 条 INFO 日志、`WeakCache` `cache/create singleton` 日志 |

## 配置调整

### 屏蔽 paramiko 库的 INFO 日志

**位置**：`config/ScanEngine.yaml`

paramiko 库默认 INFO 级别会输出大量连接细节日志（`[chan 0] Opened sftp connection` 等），干扰业务日志查看。设为 WARNING：

```yaml
loggers:
  paramiko:
    level: WARNING
    propagate: true
```

## 涉及文件清单

| 文件 | 修改类型 |
|------|---------|
| `ScanEngine/crawling/files/nfscrawler.py` | 功能修复 + 调试日志清理 |
| `ScanEngine/crawling/files/sftpcrawler.py` | 功能修复 + 调试日志清理 |
| `ScanEngine/crawling/files/smbcrawler.py` | 跳过 `createDirectory` + 调试日志清理 |
| `ScanEngine/crawling/files/ftpcrawler.py` | 调试日志清理 |
| `ScanEngine/server/restserver.py` | `separate` 异常隔离 |
| `ScanEngine/common/conntool.py` | 调试日志清理 |
| `config/ScanEngine.yaml` | paramiko logger 调整 |
| `MyCluster/ScanEngineMaster/crawling/files/nfscrawler.py` | 同步修复 |
| `MyCluster/ScanEngineMaster/crawling/files/sftpcrawler.py` | 同步清理 |
| `MyCluster/ScanEngineMaster/crawling/files/smbcrawler.py` | 同步清理 |
| `MyCluster/ScanEngineMaster/common/conntool.py` | 同步清理 |
| `MyCluster/ScanEngineMaster/server/restserver.py` | 同步修复 |
| `MyCluster/ScanEngineMaster/config/ScanEngine.yaml` | 同步配置 |
| `MyCluster/ScanEngineSlave/**` | 同 Master |

## 部署说明

1. 拉取最新代码
2. 重启 ScanEngine 服务（单 SE 部署）
3. 重启 MyCluster Master 和 Slave 服务
4. 验证日志中 `paramiko.transport.sftp` 类的日志不再出现
5. 验证 `WeakCache` / `ClientPool.wrapper` INFO 日志不再出现
6. 实际跑一次 NFS、FTP→SFTP、FTP→SMB 隔离，确认接口返回 2000 且源文件被替换为空文件

## 验证方法

- **NFS 隔离**：观察 `nfscrawler.py` 日志，`empty_file` 不再走 `mnt()`，直接用 `dir_handle` 操作
- **SFTP 隔离**：观察 `sftpcrawler.py`，`store_file` 调 `shutil.copyfileobj`，日志无 `getvalue` 报错
- **隔离接口**：调用 `/separate` 接口，预期返回 `{'code': 2000, 'message': 'succeed'}`
- **占位符**：检查 NFS/SFTP 源服务器，原文件被替换为 0 字节空文件

## 注意事项

- **NFS 隔离的占位符功能**依赖 `dir_handle` 字段（由 `get_nodes` 注入到 node 中）。如果上游服务（如消费 Kafka 的前端）转发隔离请求时**未透传 `dir_handle` 字段**，`empty_file` 会回退到 `mnt()` 并快速失败，**占位符会失效但不影响主流程**
- **SMB 隔离** `createDirectory` 步骤已跳过，要求隔离目录必须在 SMB 服务器上手动预先创建
