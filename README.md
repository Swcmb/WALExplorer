# WALExplorer


![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)![License](https://img.shields.io/badge/license-MIT-green.svg)![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15.15-blue.svg)

**一个功能完整的 PostgreSQL WAL 文件解析工具**

[功能特性](#功能特性) • [快速开始](#快速开始) • [使用文档](#使用文档) • [架构设计](#架构设计)

---

## 📖 项目简介

WALExplorer 是一个用 Python 开发的 PostgreSQL WAL（Write-Ahead Logging）文件解析工具，旨在帮助开发者和数据库管理员深入理解 PostgreSQL 的 WAL 机制，从 WAL 文件中提取有用的信息，并将其转换为可执行的 SQL 语句。

### 为什么选择 WALExplorer？

- ✨ **纯 Python 实现**：无需编译，易于部署和维护
- 🔄 **双格式支持**：同时支持二进制和文本格式 WAL 文件
- 🎯 **模块化设计**：清晰的代码结构，易于扩展
- 💾 **SQL 输出**：直接生成可执行的 SQL 语句
- 🚀 **零依赖**：仅使用 Python 标准库
- 📊 **事务跟踪**：完整的事务状态管理

---

## 🎯 功能特性

### 核心功能

| 功能 | 描述 |
|------|------|
| **WAL 文件解析** | 解析 PostgreSQL 二进制格式 WAL 文件 |
| **文本格式支持** | 解析 pg_waldump 输出的文本格式 |
| **DML 提取** | 提取 INSERT、UPDATE、DELETE 操作 |
| **DDL 解析** | 解析 CREATE、ALTER、DROP 操作 |
| **事务管理** | 跟踪事务状态、子事务和事务边界 |
| **SQL 生成** | 生成可执行的 SQL 语句文件 |
| **灵活过滤** | 按资源管理器 ID 或事务 ID 过滤记录 |

### 支持的操作类型

- ✅ INSERT 操作（单行和多行）
- ✅ UPDATE 操作（包括 HOT 更新）
- ✅ DELETE 操作
- ✅ CREATE DATABASE/TABLESPACE
- ✅ DROP DATABASE/TABLESPACE
- ✅ 事务提交和回滚
- ✅ 子事务管理

---

## 🚀 快速开始

### 环境要求

- Python 3.8 或更高版本
- PostgreSQL 15.15（用于测试和验证）

### 安装

```bash
# 克隆项目
git clone https://github.com/Swcmb/WALExplorer
cd WALExplorer

# 无需安装依赖，仅使用 Python 标准库
```

### 验证安装

```bash
# 检查 Python 版本
python --version

# 查看帮助信息
python main.py --help
```

### 基本使用

```bash
# 解析二进制 WAL 文件
python main.py /path/to/wal/file

# 解析文本格式 WAL 文件
python main.py /path/to/wal/file.txt --text-format

# 输出到文件
python main.py /path/to/wal/file -o output.sql

# 只解析 Heap 记录（DML 操作）
python main.py /path/to/wal/file --rmgr 10

# 只解析指定事务
python main.py /path/to/wal/file --xid 1234
```

---

## 📚 使用文档

### 命令行参数

| 参数 | 简写 | 描述 | 示例 |
|------|------|------|------|
| `wal_file` | - | WAL 文件路径（必需） | `/path/to/wal/file` |
| `--output` | `-o` | 输出文件路径 | `-o output.sql` |
| `--rmgr` | - | 资源管理器 ID（0-24） | `--rmgr 10` |
| `--xid` | - | 事务 ID | `--xid 1234` |
| `--text-format` | - | 解析文本格式 | `--text-format` |
| `--verbose` | `-v` | 显示详细信息 | `-v` |
| `--version` | - | 显示版本 | `--version` |
| `--help` | `-h` | 显示帮助 | `--help` |

### 资源管理器 ID

| ID | 名称 | 描述 |
|----|------|------|
| 0 | XLOG | 事务日志记录 |
| 1 | Transaction | 事务管理 |
| 4 | Database | 数据库操作（DDL） |
| 5 | Tablespace | 表空间操作（DDL） |
| 9 | Heap2 | 扩展堆操作 |
| 10 | Heap | 堆操作（DML） |
| 11 | Btree | B 树索引 |
| 15 | Sequence | 序列 |

### 输出示例

```sql
-- WALExplorer 生成的 SQL 语句
-- 生成时间: 2025-12-28 11:50:00
-- 记录数量: 8

BEGIN;

-- 事务ID: 500
INSERT INTO user_table_1234 (column1, column2) VALUES ('value1', 'value2');

-- 事务ID: 500
UPDATE user_table_1234 SET column1 = 'new_value' WHERE ctid = '(0,1)';

-- 事务ID: 500
DELETE FROM user_table_1234 WHERE ctid = '(0,2)';

COMMIT;  -- 事务ID: 500
```

---

## 🏗️ 架构设计

### 项目结构

```
WALExplorer/
├── core/                      # 核心解析模块
│   ├── wal_parser.py         # WAL 文件解析核心
│   ├── xlog_reader.py        # XLOG 记录读取器
│   └── transaction_manager.py # 事务管理器
├── parsers/                   # 解析器模块
│   ├── heap_parser.py        # Heap 记录解析（DML）
│   └── ddl_parser.py         # DDL 记录解析
├── utils/                     # 工具模块
│   ├── binary_reader.py      # 二进制数据读取工具
│   ├── lsn_utils.py          # LSN 处理工具
│   └── wal_text_parser.py    # WAL 文本格式解析器
├── output/                    # 输出模块
│   └── sql_formatter.py      # SQL 格式化输出
├── main.py                    # 主程序入口
└── README.md                  # 项目文档
```

## 🔧 开发指南

### 添加新的记录类型解析器

1. 在 `parsers/` 目录下创建新的解析器文件
2. 继承或参考现有解析器的实现
3. 在 `SQLFormatter` 中添加相应的格式化逻辑

### 运行测试

```bash
# 运行所有测试
python -m pytest tests/

# 运行特定测试
python -m pytest tests/test_wal_parser.py
```

### 代码风格

项目遵循 PEP 8 代码风格指南：

```bash
# 检查代码风格
flake8 .

# 自动格式化
black .
```

---

## ❓ 常见问题

### Q: WALExplorer 支持哪些 PostgreSQL 版本？

A: 当前版本针对 PostgreSQL 15.15 设计，其他版本可能存在兼容性问题。

### Q: 如何处理大文件？

A: WALExplorer 采用流式读取模式，可以处理任意大小的 WAL 文件。

### Q: 输出的 SQL 语句可以直接执行吗？

A: 可以，但建议先在测试环境中验证，确保数据一致性。

### Q: 如何调试解析问题？

A: 使用 `--verbose` 参数获取详细的调试信息。

### Q: 支持哪些数据类型？

A: 当前支持基本数据类型，复杂类型正在开发中。

---

## 🤝 贡献指南

我们欢迎任何形式的贡献！

### 如何贡献

1. Fork 本项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

### 贡献规范

- 遵循现有的代码风格
- 添加必要的测试用例
- 更新相关文档
- 确保所有测试通过

---

## 📄 许可证

本项目采用 PostgreSQL 许可证 - 详见 [LICENSE](LICENSE) 文件

---

## 🙏 致谢

本项目的开发参考了以下项目：

- [WalMiner](https://github.com/666pulse/walminer) - PostgreSQL WAL 解析工具
- [XLogMiner](https://gitee.com/movead/XLogMiner) - PostgreSQL WAL 日志解析工具

---

## ⚠️ 免责声明

本工具仅用于防御性安全任务，如安全分析、检测规则编写、漏洞解释等。请勿用于恶意目的。使用者需自行承担使用本工具产生的所有后果。

---

**如果这个项目对您有帮助，请给一个 ⭐️ Star**

Made with ❤️ by WALExplorer Team
