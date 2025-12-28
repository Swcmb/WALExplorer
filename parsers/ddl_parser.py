"""
DDL记录解析器
负责解析PostgreSQL的DDL操作记录，提取数据定义语句信息
"""

from typing import Dict, Any, List, Optional
from utils.binary_reader import BinaryReader
from core.wal_parser import XLogRecord


class DDLInfo:
    """
    DDL操作信息基类
    """
    
    def __init__(self, operation_type: str):
        self.operation_type = operation_type
        self.schema_name = "public"
        self.object_name = ""
        self.object_oid = 0
        self.sql_statement = ""
        self.is_system_object = False


class CreateTableInfo(DDLInfo):
    """
    创建表操作信息
    """
    
    def __init__(self):
        super().__init__("CREATE TABLE")
        self.table_name = ""
        self.columns = []
        self.constraints = []
        self.tablespace_name = ""
        self.owner = ""


class DropTableInfo(DDLInfo):
    """
    删除表操作信息
    """
    
    def __init__(self):
        super().__init__("DROP TABLE")
        self.table_name = ""
        self.drop_behavior = "RESTRICT"  # RESTRICT 或 CASCADE


class AlterTableInfo(DDLInfo):
    """
    修改表操作信息
    """
    
    def __init__(self):
        super().__init__("ALTER TABLE")
        self.table_name = ""
        self.alter_actions = []


class CreateIndexInfo(DDLInfo):
    """
    创建索引操作信息
    """
    
    def __init__(self):
        super().__init__("CREATE INDEX")
        self.index_name = ""
        self.table_name = ""
        self.columns = []
        self.index_type = "btree"
        self.unique = False
        self.concurrently = False


class DropIndexInfo(DDLInfo):
    """
    删除索引操作信息
    """
    
    def __init__(self):
        super().__init__("DROP INDEX")
        self.index_name = ""
        self.drop_behavior = "RESTRICT"


class CreateSchemaInfo(DDLInfo):
    """
    创建模式操作信息
    """
    
    def __init__(self):
        super().__init__("CREATE SCHEMA")
        self.schema_name = ""
        self.owner = ""


class DropSchemaInfo(DDLInfo):
    """
    删除模式操作信息
    """
    
    def __init__(self):
        super().__init__("DROP SCHEMA")
        self.schema_name = ""
        self.drop_behavior = "RESTRICT"


class DatabaseInfo(DDLInfo):
    """
    数据库操作信息
    """
    
    def __init__(self, operation_type: str):
        super().__init__(operation_type)
        self.database_name = ""
        self.owner = ""
        self.tablespace_name = ""
        self.options = {}


class TablespaceInfo(DDLInfo):
    """
    表空间操作信息
    """
    
    def __init__(self, operation_type: str):
        super().__init__(operation_type)
        self.tablespace_name = ""
        self.owner = ""
        self.location = ""


class DDLParser:
    """
    DDL记录解析器
    """
    
    def __init__(self):
        """
        初始化DDL解析器
        """
        # 系统表OID映射
        self.system_tables = {
            1247: 'pg_class',      # pg_class OID
            1249: 'pg_attribute',  # pg_attribute OID
            1255: 'pg_proc',       # pg_proc OID
            1260: 'pg_type',       # pg_type OID
            1261: 'pg_constraint', # pg_constraint OID
            1262: 'pg_inherits',   # pg_inherits OID
            2396: 'pg_trigger',    # pg_trigger OID
            2609: 'pg_description', # pg_description OID
            2606: 'pg_index',      # pg_index OID
            2611: 'pg_depend',     # pg_depend OID
            2612: 'pg_db_role_setting', # pg_db_role_setting OID
            2964: 'pg_auth_members', # pg_auth_members OID
            3455: 'pg_shdepend',   # pg_shdepend OID
            3592: 'pg_shseclabel', # pg_shseclabel OID
            3786: 'pg_extension',  # pg_extension OID
            3079: 'pg_enum',       # pg_enum OID
            2836: 'pg_authid',     # pg_authid OID
            1213: 'pg_tablespace', # pg_tablespace OID
            1214: 'pg_database',   # pg_database OID
        }
        
        # 事务状态跟踪
        self.pending_ddl_operations = {}
    
    def parse_ddl_record(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析DDL记录
        
        Args:
            record: XLOG记录
            
        Returns:
            DDL操作信息
        """
        # 检查是否是系统表操作
        if not self._is_system_table_operation(record):
            return None
        
        # 根据资源管理器类型解析
        if record.xl_rmid == 4:  # Database
            return self._parse_database_record(record)
        elif record.xl_rmid == 5:  # Tablespace
            return self._parse_tablespace_record(record)
        elif record.xl_rmid == 10:  # Heap
            return self._parse_heap_ddl_record(record)
        elif record.xl_rmid == 11:  # Btree
            return self._parse_btree_ddl_record(record)
        else:
            return None
    
    def _is_system_table_operation(self, record: XLogRecord) -> bool:
        """
        检查是否是系统表操作
        
        Args:
            record: XLOG记录
            
        Returns:
            如果是系统表操作返回True
        """
        if not record.blocks:
            return False
        
        for block in record.blocks:
            if 'relfilenode' in block:
                relnode = block['relfilenode']
                if relnode['relNode'] in self.system_tables:
                    return True
        
        return False
    
    def _parse_database_record(self, record: XLogRecord) -> Optional[DatabaseInfo]:
        """
        解析数据库记录
        
        Args:
            record: 数据库记录
            
        Returns:
            数据库操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # CREATE DATABASE
            db_info = DatabaseInfo("CREATE DATABASE")
            db_info.database_name = self._extract_database_name(record)
            db_info.owner = self._extract_database_owner(record)
            db_info.tablespace_name = self._extract_database_tablespace(record)
            return db_info
        elif info == 0x10:  # DROP DATABASE
            db_info = DatabaseInfo("DROP DATABASE")
            db_info.database_name = self._extract_database_name(record)
            return db_info
        elif info == 0x20:  # ALTER DATABASE
            db_info = DatabaseInfo("ALTER DATABASE")
            db_info.database_name = self._extract_database_name(record)
            db_info.options = self._extract_database_options(record)
            return db_info
        else:
            return None
    
    def _parse_tablespace_record(self, record: XLogRecord) -> Optional[TablespaceInfo]:
        """
        解析表空间记录
        
        Args:
            record: 表空间记录
            
        Returns:
            表空间操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # CREATE TABLESPACE
            ts_info = TablespaceInfo("CREATE TABLESPACE")
            ts_info.tablespace_name = self._extract_tablespace_name(record)
            ts_info.owner = self._extract_tablespace_owner(record)
            ts_info.location = self._extract_tablespace_location(record)
            return ts_info
        elif info == 0x10:  # DROP TABLESPACE
            ts_info = TablespaceInfo("DROP TABLESPACE")
            ts_info.tablespace_name = self._extract_tablespace_name(record)
            return ts_info
        elif info == 0x20:  # ALTER TABLESPACE
            ts_info = TablespaceInfo("ALTER TABLESPACE")
            ts_info.tablespace_name = self._extract_tablespace_name(record)
            return ts_info
        else:
            return None
    
    def _parse_heap_ddl_record(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析Heap DDL记录
        
        Args:
            record: Heap记录
            
        Returns:
            DDL操作信息
        """
        if not record.blocks:
            return None
        
        # 检查是否是pg_class操作
        for block in record.blocks:
            if 'relfilenode' in block:
                relnode = block['relfilenode']
                if relnode['relNode'] == 1247:  # pg_class
                    return self._parse_pg_class_operation(record)
                elif relnode['relNode'] == 1249:  # pg_attribute
                    return self._parse_pg_attribute_operation(record)
                elif relnode['relNode'] == 2606:  # pg_index
                    return self._parse_pg_index_operation(record)
        
        return None
    
    def _parse_btree_ddl_record(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析Btree DDL记录（索引相关）
        
        Args:
            record: Btree记录
            
        Returns:
            DDL操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # CREATE INDEX
            index_info = CreateIndexInfo()
            index_info.index_name = self._extract_index_name(record)
            index_info.table_name = self._extract_index_table_name(record)
            index_info.columns = self._extract_index_columns(record)
            index_info.unique = self._is_unique_index(record)
            return index_info
        elif info == 0x10:  # DROP INDEX
            index_info = DropIndexInfo()
            index_info.index_name = self._extract_index_name(record)
            return index_info
        else:
            return None
    
    def _parse_pg_class_operation(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析pg_class操作
        
        Args:
            record: pg_class操作记录
            
        Returns:
            DDL操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # INSERT - 创建表
            table_info = CreateTableInfo()
            table_info.table_name = self._extract_table_name_from_pg_class(record)
            table_info.columns = self._extract_table_columns(record)
            table_info.constraints = self._extract_table_constraints(record)
            return table_info
        elif info == 0x10:  # DELETE - 删除表
            table_info = DropTableInfo()
            table_info.table_name = self._extract_table_name_from_pg_class(record)
            return table_info
        elif info == 0x20:  # UPDATE - 修改表
            table_info = AlterTableInfo()
            table_info.table_name = self._extract_table_name_from_pg_class(record)
            table_info.alter_actions = self._extract_alter_actions(record)
            return table_info
        else:
            return None
    
    def _parse_pg_attribute_operation(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析pg_attribute操作
        
        Args:
            record: pg_attribute操作记录
            
        Returns:
            DDL操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # INSERT - 添加列
            table_info = AlterTableInfo()
            table_info.table_name = self._extract_table_name_from_pg_attribute(record)
            table_info.alter_actions = [
                {
                    'action': 'ADD COLUMN',
                    'column_name': self._extract_column_name(record),
                    'column_type': self._extract_column_type(record)
                }
            ]
            return table_info
        elif info == 0x10:  # DELETE - 删除列
            table_info = AlterTableInfo()
            table_info.table_name = self._extract_table_name_from_pg_attribute(record)
            table_info.alter_actions = [
                {
                    'action': 'DROP COLUMN',
                    'column_name': self._extract_column_name(record)
                }
            ]
            return table_info
        else:
            return None
    
    def _parse_pg_index_operation(self, record: XLogRecord) -> Optional[DDLInfo]:
        """
        解析pg_index操作
        
        Args:
            record: pg_index操作记录
            
        Returns:
            DDL操作信息
        """
        info = record.get_info()
        
        if info == 0x00:  # INSERT - 创建索引
            index_info = CreateIndexInfo()
            index_info.index_name = self._extract_index_name_from_pg_index(record)
            index_info.table_name = self._extract_index_table_name_from_pg_index(record)
            index_info.columns = self._extract_index_columns_from_pg_index(record)
            return index_info
        elif info == 0x10:  # DELETE - 删除索引
            index_info = DropIndexInfo()
            index_info.index_name = self._extract_index_name_from_pg_index(record)
            return index_info
        else:
            return None
    
    # 以下方法用于从WAL记录中提取具体信息
    # 这些是简化版本，实际实现需要根据PostgreSQL的WAL格式进行详细解析
    
    def _extract_database_name(self, record: XLogRecord) -> str:
        """提取数据库名"""
        return "database_name"
    
    def _extract_database_owner(self, record: XLogRecord) -> str:
        """提取数据库所有者"""
        return "postgres"
    
    def _extract_database_tablespace(self, record: XLogRecord) -> str:
        """提取数据库表空间"""
        return "pg_default"
    
    def _extract_database_options(self, record: XLogRecord) -> Dict[str, Any]:
        """提取数据库选项"""
        return {}
    
    def _extract_tablespace_name(self, record: XLogRecord) -> str:
        """提取表空间名"""
        return "tablespace_name"
    
    def _extract_tablespace_owner(self, record: XLogRecord) -> str:
        """提取表空间所有者"""
        return "postgres"
    
    def _extract_tablespace_location(self, record: XLogRecord) -> str:
        """提取表空间位置"""
        return "/path/to/tablespace"
    
    def _extract_index_name(self, record: XLogRecord) -> str:
        """提取索引名"""
        return "index_name"
    
    def _extract_index_table_name(self, record: XLogRecord) -> str:
        """提取索引对应的表名"""
        return "table_name"
    
    def _extract_index_columns(self, record: XLogRecord) -> List[str]:
        """提取索引列"""
        return ["column1", "column2"]
    
    def _is_unique_index(self, record: XLogRecord) -> bool:
        """检查是否是唯一索引"""
        return False
    
    def _extract_table_name_from_pg_class(self, record: XLogRecord) -> str:
        """从pg_class记录中提取表名"""
        return "table_name"
    
    def _extract_table_columns(self, record: XLogRecord) -> List[Dict[str, Any]]:
        """提取表列信息"""
        return [
            {'name': 'id', 'type': 'integer', 'not_null': True},
            {'name': 'name', 'type': 'varchar', 'not_null': False}
        ]
    
    def _extract_table_constraints(self, record: XLogRecord) -> List[Dict[str, Any]]:
        """提取表约束信息"""
        return []
    
    def _extract_alter_actions(self, record: XLogRecord) -> List[Dict[str, Any]]:
        """提取ALTER操作"""
        return []
    
    def _extract_table_name_from_pg_attribute(self, record: XLogRecord) -> str:
        """从pg_attribute记录中提取表名"""
        return "table_name"
    
    def _extract_column_name(self, record: XLogRecord) -> str:
        """提取列名"""
        return "column_name"
    
    def _extract_column_type(self, record: XLogRecord) -> str:
        """提取列类型"""
        return "varchar"
    
    def _extract_index_name_from_pg_index(self, record: XLogRecord) -> str:
        """从pg_index记录中提取索引名"""
        return "index_name"
    
    def _extract_index_table_name_from_pg_index(self, record: XLogRecord) -> str:
        """从pg_index记录中提取表名"""
        return "table_name"
    
    def _extract_index_columns_from_pg_index(self, record: XLogRecord) -> List[str]:
        """从pg_index记录中提取索引列"""
        return ["column1", "column2"]
    
    def generate_sql_statement(self, ddl_info: DDLInfo) -> str:
        """
        根据DDL信息生成SQL语句
        
        Args:
            ddl_info: DDL操作信息
            
        Returns:
            SQL语句
        """
        if isinstance(ddl_info, CreateTableInfo):
            return self._generate_create_table_sql(ddl_info)
        elif isinstance(ddl_info, DropTableInfo):
            return self._generate_drop_table_sql(ddl_info)
        elif isinstance(ddl_info, AlterTableInfo):
            return self._generate_alter_table_sql(ddl_info)
        elif isinstance(ddl_info, CreateIndexInfo):
            return self._generate_create_index_sql(ddl_info)
        elif isinstance(ddl_info, DropIndexInfo):
            return self._generate_drop_index_sql(ddl_info)
        elif isinstance(ddl_info, DatabaseInfo):
            return self._generate_database_sql(ddl_info)
        elif isinstance(ddl_info, TablespaceInfo):
            return self._generate_tablespace_sql(ddl_info)
        else:
            return f"-- Unknown DDL operation: {ddl_info.operation_type}"
    
    def _generate_create_table_sql(self, table_info: CreateTableInfo) -> str:
        """生成CREATE TABLE SQL"""
        columns_sql = []
        for column in table_info.columns:
            col_sql = f"{column['name']} {column['type']}"
            if column.get('not_null', False):
                col_sql += " NOT NULL"
            columns_sql.append(col_sql)
        
        sql = f"CREATE TABLE {table_info.table_name} (\n"
        sql += ",\n".join(f"    {col}" for col in columns_sql)
        sql += "\n);"
        
        return sql
    
    def _generate_drop_table_sql(self, table_info: DropTableInfo) -> str:
        """生成DROP TABLE SQL"""
        sql = f"DROP TABLE {table_info.table_name}"
        if table_info.drop_behavior == "CASCADE":
            sql += " CASCADE"
        sql += ";"
        
        return sql
    
    def _generate_alter_table_sql(self, table_info: AlterTableInfo) -> str:
        """生成ALTER TABLE SQL"""
        sql = f"ALTER TABLE {table_info.table_name}"
        
        for action in table_info.alter_actions:
            if action['action'] == 'ADD COLUMN':
                sql += f"\n  ADD COLUMN {action['column_name']} {action['column_type']}"
            elif action['action'] == 'DROP COLUMN':
                sql += f"\n  DROP COLUMN {action['column_name']}"
        
        sql += ";"
        
        return sql
    
    def _generate_create_index_sql(self, index_info: CreateIndexInfo) -> str:
        """生成CREATE INDEX SQL"""
        sql = "CREATE "
        if index_info.unique:
            sql += "UNIQUE "
        if index_info.concurrently:
            sql += "CONCURRENTLY "
        
        sql += f"INDEX {index_info.index_name} ON {index_info.table_name}"
        sql += f" ({', '.join(index_info.columns)});"
        
        return sql
    
    def _generate_drop_index_sql(self, index_info: DropIndexInfo) -> str:
        """生成DROP INDEX SQL"""
        sql = f"DROP INDEX {index_info.index_name}"
        if index_info.drop_behavior == "CASCADE":
            sql += " CASCADE"
        sql += ";"
        
        return sql
    
    def _generate_database_sql(self, db_info: DatabaseInfo) -> str:
        """生成数据库SQL"""
        if db_info.operation_type == "CREATE DATABASE":
            sql = f"CREATE DATABASE {db_info.database_name}"
            if db_info.owner:
                sql += f" OWNER {db_info.owner}"
            if db_info.tablespace_name:
                sql += f" TABLESPACE {db_info.tablespace_name}"
            sql += ";"
        elif db_info.operation_type == "DROP DATABASE":
            sql = f"DROP DATABASE {db_info.database_name};"
        elif db_info.operation_type == "ALTER DATABASE":
            sql = f"ALTER DATABASE {db_info.database_name};"
        else:
            sql = f"-- Unknown database operation: {db_info.operation_type}"
        
        return sql
    
    def _generate_tablespace_sql(self, ts_info: TablespaceInfo) -> str:
        """生成表空间SQL"""
        if ts_info.operation_type == "CREATE TABLESPACE":
            sql = f"CREATE TABLESPACE {ts_info.tablespace_name}"
            if ts_info.owner:
                sql += f" OWNER {ts_info.owner}"
            if ts_info.location:
                sql += f" LOCATION '{ts_info.location}'"
            sql += ";"
        elif ts_info.operation_type == "DROP TABLESPACE":
            sql = f"DROP TABLESPACE {ts_info.tablespace_name};"
        elif ts_info.operation_type == "ALTER TABLESPACE":
            sql = f"ALTER TABLESPACE {ts_info.tablespace_name};"
        else:
            sql = f"-- Unknown tablespace operation: {ts_info.operation_type}"
        
        return sql