# WALExplorer

WALExplorer 是一个用于解析 PostgreSQL WAL（Write-Ahead Logging）文件的 Python 工具，可以从 WAL 文件中提取 SQL 语句，包括 DML 和 DDL 操作。

## 功能特性

- **WAL 文件解析**：解析 PostgreSQL WAL 文件（支持二进制和文本格式）
- **DML 语句提取**：提取 INSERT、UPDATE、DELETE 等数据操作语句
- **DDL 语句解析**：解析基本的 CREATE、ALTER、DROP 等数据定义语句
- **系统表 DML 提取**：提取 DDL 引发的系统表相关 DML 语句
- **事务管理**：跟踪和管理事务状态
- **结构化输出**：生成可执行的 SQL 语句文件
- **灵活过滤**：支持按资源管理器 ID 或事务 ID 过滤记录

## 项目结构

```
WALExplorer/
├── core/                      # 核心解析模块
│   ├── wal_parser.py         # WAL 文件解析核心
│   ├── xlog_reader.py        # XLOG 记录读取器
│   └── transaction_manager.py # 事务管理
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

## 环境要求

- Python 3.8+
- PostgreSQL 15.15（用于测试和验证）

## 部署指南

### 1. 克隆或下载项目

```bash
git clone <repository-url>
cd WALExplorer
```

### 2. 安装依赖

本项目仅使用 Python 标准库，无需安装第三方依赖：

```bash
# 查看依赖文件
cat requirements.txt

# 如需使用 pip 安装（虽然本项目无需任何第三方依赖）
pip install -r requirements.txt
```

### 3. 验证安装

```bash
# 检查 Python 版本
python --version

# 运行主程序查看帮助信息
python main.py --help
```

### 4. 运行示例

```bash
# 解析 WAL 文件
python main.py /path/to/your/wal/file
```

## 安装和使用

### 基本使用

```bash
# 解析二进制 WAL 文件并输出 SQL 语句
python main.py /path/to/wal/file

# 解析文本格式 WAL 文件
python main.py /path/to/wal/file.txt

# 输出到指定文件
python main.py /path/to/wal/file -o output.sql

# 只解析 Heap 记录（DML 操作）
python main.py /path/to/wal/file --rmgr 10

# 只解析指定事务的记录
python main.py /path/to/wal/file --xid 1234

# 显示详细信息
python main.py /path/to/wal/file --verbose
```

### 命令行参数

- `wal_file`：要解析的 WAL 文件路径（必需）
- `-o, --output`：输出文件路径（可选，默认输出到标准输出）
- `--rmgr`：只解析指定资源管理器 ID 的记录（0-24）
- `--xid`：只解析指定事务 ID 的记录
- `--verbose, -v`：显示详细信息
- `--version`：显示版本信息

### 资源管理器 ID

| ID  | 名称        | 描述                     |
|-----|-------------|--------------------------|
| 0   | XLOG        | 事务日志记录             |
| 1   | Transaction | 事务管理                 |
| 4   | Database    | 数据库操作（DDL）        |
| 5   | Tablespace  | 表空间操作（DDL）        |
| 9   | Heap2       | 扩展堆操作               |
| 10  | Heap        | 堆操作（DML）            |
| 11  | Btree       | B 树索引                 |
| 15  | Sequence    | 序列                     |

## 输出示例

```sql
-- WALExplorer 生成的 SQL 语句
-- 生成时间: 2025-12-28 11:50:00
-- 记录数量: 8

BEGIN;

INSERT INTO user_table_1234 (column1, column2) VALUES ('value1', 'value2');

UPDATE user_table_1234 SET column1 = 'new_value' WHERE ctid = '(0,1)';

DELETE FROM user_table_1234 WHERE ctid = '(0,2)';

COMMIT;  -- 事务ID: 500
```

## 技术实现

### 核心组件

1. **二进制数据读取器**：处理 WAL 文件的二进制格式
2. **LSN 工具**：处理 PostgreSQL 的日志序列号
3. **WAL 解析器**：解析 WAL 文件结构和记录
4. **事务管理器**：跟踪事务状态和边界
5. **解析器**：
   - Heap 解析器：处理 DML 操作
   - DDL 解析器：处理数据定义操作
6. **SQL 格式化器**：生成可执行的 SQL 语句

### 设计原则

- **可维护性**：模块化设计，清晰的代码结构
- **可扩展性**：易于添加新的记录类型解析器
- **错误处理**：完善的异常处理和错误恢复

## 限制和注意事项

1. **PostgreSQL 版本**：当前针对 PostgreSQL 15.15 设计
2. **DDL 支持**：仅支持基本的 DDL 语句解析
3. **平台支持**：优先保证功能正确性，性能为次要考虑

## 参考资源

本项目的开发参考了以下项目：

- **WalMiner**：一个用 C 语言实现的 PostgreSQL WAL 解析工具，可作为 PostgreSQL 扩展安装。GitHub: https://github.com/666pulse/walminer
- **XLogMiner**：PostgreSQL WAL 日志解析工具。Gitee: https://gitee.com/movead/XLogMiner

## 开发和扩展

### 添加新的记录类型解析器

1. 在 `parsers/` 目录下创建新的解析器文件
2. 实现解析逻辑，继承基础解析器类
3. 在 `sql_formatter.py` 中添加对应的 SQL 生成逻辑
4. 更新资源管理器 ID 映射

### 自定义输出格式

1. 在 `output/` 目录下创建新的格式化器
2. 实现格式化接口
3. 在主程序中添加格式选择选项

## 故障排除

### 常见问题

1. **导入错误**：确保 Python 路径正确，使用绝对导入
2. **编码问题**：在 Windows 环境下可能遇到编码问题，建议使用 UTF-8
3. **权限问题**：确保有读取 WAL 文件的权限

### 调试模式

使用 `--verbose` 参数获取详细的调试信息：

```bash
python main.py /path/to/wal/file --verbose
```

## 许可证

本项目采用 MIT 许可证。

## 贡献

欢迎提交问题和改进建议。在提交代码前，请确保：

1. 代码通过所有测试
2. 遵循现有的代码风格
3. 添加必要的文档和注释

---

**注意**：本工具仅用于防御性安全任务，如安全分析、检测规则编写、漏洞解释等。请勿用于恶意目的。