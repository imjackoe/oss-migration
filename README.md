# OSS Migration Tool

![Python Version](https://img.shields.io/badge/python-3.7%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

阿里云OSS数据迁移工具，支持多线程、断点续传、智能跳过的对象存储数据迁移解决方案。

## 🌟 功能特性

- **跨存储桶迁移**：支持不同OSS存储桶之间的数据迁移
- **路径重定向**：自定义源路径到目标路径的映射规则
- **智能跳过**：
  - 自动跳过已存在文件（需启用`--skip-exists`）
  - 跳过源目标路径相同的文件
- **性能优化**：
  - 多线程并发传输（默认20线程）
  - 自适应分页策略（默认每页500对象）
  - 流量控制自动重试（429错误处理）
- **可视化监控**：
  - 实时进度显示（处理速度/剩余时间）
  - 成功率统计报表
- **日志系统**：
  - 成功/失败操作详细记录
  - 自动生成带时间戳的日志文件

## 🚀 快速开始

### 前置要求

- Python 3.7+
- 阿里云OSS访问权限（AccessKey）

### 安装步骤

1. 克隆仓库：
```bash
git clone https://github.com/imjackoe/oss-migration.git
cd oss-migration
python oss_migration.py
```

2. 安全依赖：
```bash
pip install -r requirements.txt
```

3. 配置环境变量（创建.env文件）：
```bash
OSS_ACCESS_KEY_ID=your_access_key_id
OSS_ACCESS_KEY_SECRET=your_access_key_secret
OSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com  # 根据实际情况修改
```

### ⚙️ 基本使用

#### 基础迁移命令
```bash
python oss_migration.py \
  --src oss://source-bucket/source/path/ \
  --dest oss://target-bucket/target/path/
```

#### 带参数示例
```bash
python oss_migration.py \
  --src oss://src-bucket/data/2023/ \
  --dest oss://dest-bucket/archive/ \
  --page-size 1000 \
  --workers 50 \
  --skip-exists
```

#### 参数说明

| 参数            | 类型    | 必填 | 默认值 | 说明                                                                 |
|-----------------|---------|------|--------|----------------------------------------------------------------------|
| `--src`         | string  | 是   | 无     | 源路径，格式：`oss://bucket-name/path/`                             |
| `--dest`        | string  | 是   | 无     | 目标路径，格式：`oss://bucket-name/path/`                           |
| `--page-size`   | int     | 否   | 500    | 分页大小（范围：100-1000），影响每次列举对象的数量                  |
| `--workers`     | int     | 否   | 20     | 并发线程数（范围：5-100），根据网络带宽和服务器性能调整             |
| `--skip-exists` | flag    | 否   | False  | 启用目标存在性检查，存在相同路径文件时自动跳过（增加API调用次数）   |

## ⚙️ 配置选项

### 环境变量配置

| 变量名                  | 必填 | 默认值                          | 示例                                   | 说明                                                                 |
|-------------------------|------|---------------------------------|----------------------------------------|----------------------------------------------------------------------|
| `OSS_ACCESS_KEY_ID`     | 是   | 无                              | LTAI5txxxxxxxxxxxx                     | 阿里云访问密钥ID                                                    |
| `OSS_ACCESS_KEY_SECRET` | 是   | 无                              | KZoGwxxxxxxxxxxxxxxxxxxxxxxx          | 阿里云访问密钥Secret                                                |
| `OSS_ENDPOINT`          | 否   | oss-cn-hangzhou.aliyuncs.com    | oss-cn-shanghai.aliyuncs.com           | OSS服务端点，根据存储桶地域修改                                      |
| `MAX_WORKERS`           | 否   | 20                              | 50                                     | 最大并发线程数（建议值：5-100）                                     |
| `PAGE_SIZE`             | 否   | 500                             | 1000                                   | 分页大小（范围：100-1000）                                          |

## 📊 日志系统

### 日志路径

```bash
logs/
├── [源bucket]-[源路径]_to_[目标bucket]-[目标路径]_success.log
└── [源bucket]-[源路径]_to_[目标bucket]-[目标路径]_fail.log
```

### 日志示例

```log
[SKIP][Fri Apr 19 22:15:03 2025] oss://dest-bucket/images/photo.jpg
[SUCCESS][Fri Apr 19 22:15:05 2025] docs/report.pdf -> archive/2023/report.pdf
```

## 💡 最佳实践

### 测试运行

```bash
# 使用小规模配置验证
python oss_migration.py \
  --src oss://test-bucket/ \
  --dest oss://test-bucket-copy/ \
  --workers 5 \
  --page-size 100
```

### 生产环境迁移

```bash
# 最大化性能配置
python oss_migration.py \
  --src oss://prod-bucket/app-data/ \
  --dest oss://backup-bucket/2023-backup/ \
  --page-size 1000 \
  --workers 100 \
  --skip-exists
```

### 定时任务集成

```bash
# 每天凌晨2点执行增量备份
0 2 * * * /usr/bin/python3 /opt/oss-migration/oss_migration.py \
  --src oss://prod-bucket/daily-data/ \
  --dest oss://backup-bucket/archives/ \
  --skip-exists
```

## ⚠️ 注意事项

### 数据安全
- 执行迁移前建议在测试环境验证路径规则
- 确保目标存储桶有足够的存储空间
- 敏感操作建议先启用`--skip-exists`进行空跑测试

### 权限要求
| 操作类型       | 源存储桶权限          | 目标存储桶权限          |
|---------------|-----------------------|-------------------------|
| 列表对象       | `oss:ListObjects`     | 无需                    |
| 读取对象       | `oss:GetObject`       | 无需                    |
| 写入对象       | 无需                  | `oss:PutObject`         |
| 跨桶复制       | `oss:GetObject`       | `oss:PutObject`         |

### 成本优化
- 每10万次操作成本估算：
  - ListObjects: ¥0.1
  - CopyObject: ¥1.0 
  - HeadObject（存在检查）: ¥0.1
- 推荐批量操作时关闭存在检查（`--skip-exists`）

### 网络要求
- 建议在阿里云ECS同地域执行迁移
- 跨地域迁移需保证至少100Mbps带宽
- 单线程速度参考：5-15MB/s

## 🤝 参与贡献

### 贡献流程
1. 在GitHub提交Issue描述问题或建议
2. Fork仓库并创建特性分支：
   ```bash
   git checkout -b feat/your-feature
   ```
3. 提交符合规范的代码变更：
   ```bash
    git commit -m "feat: add xxx feature" 
    git commit -m "fix: resolve xxx issue"
   ```
4. 推送分支并创建Pull Request
5. 通过CI测试和Code Review后合并

## 🤝 贡献方向

### 主要贡献领域
- **协议扩展**  
  添加对AWS S3、Google Cloud Storage等其他云存储的支持
- **性能优化**  
  实现零拷贝传输、压缩传输等高效传输模式
- **安全增强**  
  支持客户端加密、完整性校验等安全特性
- **监控集成**  
  添加Prometheus监控指标导出功能
- **测试覆盖**  
  编写单元测试/集成测试用例，提升测试覆盖率

### 其他贡献方式
- 完善项目文档（中文/英文）
- 翻译国际化资源
- 修复已报告的Issues
- 优化代码结构
- 添加性能基准测试

## 📜 License

[![MIT License](https://img.shields.io/github/license/imjackoe/oss-migration?style=flat-square)](https://opensource.org/licenses/MIT)
