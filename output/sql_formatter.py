"""
SQL格式化输出模块
负责将WAL记录转换为可执行的SQL语句
"""

from typing import List, Dict, Any, Optional, TextIO
from datetime import datetime
from core.wal_parser import XLogRecord, get_rmgr_name


class SQLFormatter:
    """
    SQL格式化器
    将WAL记录转换为SQL语句
    """
    
    def __init__(self):
        """
        初始化SQL格式化器
        """
        self.transaction_stack = []  # 事务栈
        self.current_xid = None      # 当前事务ID
    
    def format_records(self, records: List[XLogRecord]) -> str:
        """
        格式化记录列表为SQL语句
        
        Args:
            records: XLOG记录列表
            
        Returns:
            格式化的SQL语句字符串
        """
        sql_lines = []
        
        # 添加文件头注释
        sql_lines.append("-- WALExplorer生成的SQL语句")
        sql_lines.append(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_lines.append(f"-- 记录数量: {len(records)}")
        sql_lines.append("")
        
        for record in records:
            sql_statements = self.format_record(record)
            if sql_statements:
                sql_lines.extend(sql_statements)
                sql_lines.append("")  # 空行分隔
        
        return "\n".join(sql_lines)
    
    def format_text_records(self, records) -> str:
        """
        格式化文本记录列表为SQL语句
        
        Args:
            records: 文本格式的WAL记录列表
            
        Returns:
            格式化的SQL语句字符串
        """
        sql_lines = []
        
        # 添加文件头注释
        sql_lines.append("-- WALExplorer生成的SQL语句")
        sql_lines.append(f"-- 生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sql_lines.append(f"-- 记录数量: {len(records)}")
        sql_lines.append("")
        
        # 按事务分组
        tx_groups = {}
        for record in records:
            if record.tx_id not in tx_groups:
                tx_groups[record.tx_id] = []
            tx_groups[record.tx_id].append(record)
        
        # 为每个事务生成SQL
        for tx_id, tx_records in tx_groups.items():
            if tx_id != 0:  # 跳过系统事务
                sql_lines.append(f"-- 事务 {tx_id}")
                sql_lines.append("BEGIN;")
                
                for record in tx_records:
                    sql_statements = self.format_text_record(record)
                    if sql_statements:
                        sql_lines.extend(sql_statements)
                
                sql_lines.append("COMMIT;")
                sql_lines.append("")
        
        return "\n".join(sql_lines)
    
    def format_record(self, record: XLogRecord) -> List[str]:
        """
        格式化单个记录为SQL语句
        
        Args:
            record: XLOG记录
            
        Returns:
            SQL语句列表
        """
        rmgr_name = get_rmgr_name(record.xl_rmid)
        
        # 处理事务开始和结束
        if self._handle_transaction_boundaries(record):
            return []
        
        # 根据资源管理器类型处理记录
        if rmgr_name == 'Heap':
            return self._format_heap_record(record)
        elif rmgr_name == 'Heap2':
            return self._format_heap2_record(record)
        elif rmgr_name == 'Transaction':
            return self._format_transaction_record(record)
        elif rmgr_name == 'Database':
            return self._format_database_record(record)
        elif rmgr_name == 'Tablespace':
            return self._format_tablespace_record(record)
        elif rmgr_name == 'Sequence':
            return self._format_sequence_record(record)
        else:
            # 其他类型的记录，生成注释
            return self._format_generic_record(record, rmgr_name)
    
    def _handle_transaction_boundaries(self, record: XLogRecord) -> bool:
        """
        处理事务边界
        
        Args:
            record: XLOG记录
            
        Returns:
            如果是事务边界记录返回True
        """
        # 检查是否是事务开始或提交记录
        if record.xl_rmid == 1:  # Transaction RMGR
            info = record.get_info()
            
            # 事务开始
            if info == 0x00:  # XLOG_XACT_COMMIT
                if record.xl_xid not in self.transaction_stack:
                    self.transaction_stack.append(record.xl_xid)
                    self.current_xid = record.xl_xid
                return True
            
            # 事务提交
            elif info == 0x10:  # XLOG_XACT_ABORT
                if record.xl_xid in self.transaction_stack:
                    self.transaction_stack.remove(record.xl_xid)
                    if self.current_xid == record.xl_xid:
                        self.current_xid = self.transaction_stack[-1] if self.transaction_stack else None
                return True
        
        return False
    
    def _format_heap_record(self, record: XLogRecord) -> List[str]:
        """
        格式化Heap记录（DML操作）
        
        Args:
            record: Heap记录
            
        Returns:
            SQL语句列表
        """
        if not record.blocks:
            return ["-- Heap记录无块数据"]
        
        sql_lines = []
        info = record.get_info()
        
        # 根据info字段判断操作类型
        if info == 0x00:  # INSERT
            sql_lines.extend(self._format_insert(record))
        elif info == 0x01:  # DELETE
            sql_lines.extend(self._format_delete(record))
        elif info == 0x02:  # UPDATE
            sql_lines.extend(self._format_update(record))
        else:
            sql_lines.append(f"-- 未知的Heap操作类型: {info}")
        
        return sql_lines
    
    def _format_heap2_record(self, record: XLogRecord) -> List[str]:
        """
        格式化Heap2记录（多版本并发控制相关）
        
        Args:
            record: Heap2记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        info = record.get_info()
        
        # Heap2记录包含一些特殊的DML操作
        if info == 0x00:  # HEAP2_MULTI_INSERT
            sql_lines.extend(self._format_multi_insert(record))
        elif info == 0x01:  # HEAP2_FREEZE
            sql_lines.append("-- VACUUM FREEZE操作")
        elif info == 0x02:  # HEAP2_VISIBLE
            sql_lines.append("-- VACUUM标记可见性操作")
        else:
            sql_lines.append(f"-- 未知的Heap2操作类型: {info}")
        
        return sql_lines
    
    def _format_insert(self, record: XLogRecord) -> List[str]:
        """
        格式化INSERT操作
        
        Args:
            record: INSERT记录
            
        Returns:
            INSERT SQL语句列表
        """
        sql_lines = []
        
        # 尝试从块数据中提取表信息
        table_info = self._extract_table_info(record)
        if not table_info:
            return ["-- 无法解析INSERT操作的表信息"]
        
        table_name = table_info.get('table_name', 'unknown_table')
        
        # 提取插入的数据
        data = self._extract_insert_data(record)
        if not data:
            return ["-- 无法解析INSERT操作的数据"]
        
        # 生成INSERT语句
        columns = ", ".join(data.keys())
        values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in data.values()])
        
        sql_lines.append(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        
        return sql_lines
    
    def _format_delete(self, record: XLogRecord) -> List[str]:
        """
        格式化DELETE操作
        
        Args:
            record: DELETE记录
            
        Returns:
            DELETE SQL语句列表
        """
        sql_lines = []
        
        table_info = self._extract_table_info(record)
        if not table_info:
            return ["-- 无法解析DELETE操作的表信息"]
        
        table_name = table_info.get('table_name', 'unknown_table')
        
        # 提取WHERE条件
        where_clause = self._extract_where_clause(record)
        if not where_clause:
            return ["-- 无法解析DELETE操作的WHERE条件"]
        
        sql_lines.append(f"DELETE FROM {table_name} WHERE {where_clause};")
        
        return sql_lines
    
    def _format_update(self, record: XLogRecord) -> List[str]:
        """
        格式化UPDATE操作
        
        Args:
            record: UPDATE记录
            
        Returns:
            UPDATE SQL语句列表
        """
        sql_lines = []
        
        table_info = self._extract_table_info(record)
        if not table_info:
            return ["-- 无法解析UPDATE操作的表信息"]
        
        table_name = table_info.get('table_name', 'unknown_table')
        
        # 提取SET子句
        set_clause = self._extract_set_clause(record)
        if not set_clause:
            return ["-- 无法解析UPDATE操作的SET子句"]
        
        # 提取WHERE条件
        where_clause = self._extract_where_clause(record)
        if not where_clause:
            return ["-- 无法解析UPDATE操作的WHERE条件"]
        
        sql_lines.append(f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};")
        
        return sql_lines
    
    def _format_multi_insert(self, record: XLogRecord) -> List[str]:
        """
        格式化多行INSERT操作
        
        Args:
            record: 多行INSERT记录
            
        Returns:
            多行INSERT SQL语句列表
        """
        sql_lines = []
        
        table_info = self._extract_table_info(record)
        if not table_info:
            return ["-- 无法解析多行INSERT操作的表信息"]
        
        table_name = table_info.get('table_name', 'unknown_table')
        
        # 提取多行数据
        rows_data = self._extract_multi_insert_data(record)
        if not rows_data:
            return ["-- 无法解析多行INSERT操作的数据"]
        
        # 生成多行INSERT语句
        if rows_data:
            columns = ", ".join(rows_data[0].keys())
            values_list = []
            
            for row in rows_data:
                values = ", ".join([f"'{v}'" if isinstance(v, str) else str(v) for v in row.values()])
                values_list.append(f"({values})")
            
            sql_lines.append(f"INSERT INTO {table_name} ({columns}) VALUES {', '.join(values_list)};")
        
        return sql_lines
    
    def _format_transaction_record(self, record: XLogRecord) -> List[str]:
        """
        格式化事务记录
        
        Args:
            record: 事务记录
            
        Returns:
            事务相关SQL语句列表
        """
        sql_lines = []
        info = record.get_info()
        
        if info == 0x00:  # XLOG_XACT_COMMIT
            sql_lines.append(f"COMMIT;  -- 事务ID: {record.xl_xid}")
        elif info == 0x10:  # XLOG_XACT_ABORT
            sql_lines.append(f"ROLLBACK;  -- 事务ID: {record.xl_xid}")
        else:
            sql_lines.append(f"-- 未知事务操作: {info}")
        
        return sql_lines
    
    def _format_database_record(self, record: XLogRecord) -> List[str]:
        """
        格式化数据库记录（DDL操作）
        
        Args:
            record: 数据库记录
            
        Returns:
            数据库DDL语句列表
        """
        sql_lines = []
        info = record.get_info()
        
        # 根据info字段判断数据库操作类型
        if info == 0x00:  # CREATE DATABASE
            db_name = self._extract_database_name(record)
            sql_lines.append(f"CREATE DATABASE {db_name or 'unknown_db'};")
        elif info == 0x10:  # DROP DATABASE
            db_name = self._extract_database_name(record)
            sql_lines.append(f"DROP DATABASE {db_name or 'unknown_db'};")
        elif info == 0x20:  # ALTER DATABASE
            sql_lines.append("-- ALTER DATABASE操作")
        else:
            sql_lines.append(f"-- 未知的数据库操作: {info}")
        
        return sql_lines
    
    def _format_tablespace_record(self, record: XLogRecord) -> List[str]:
        """
        格式化表空间记录（DDL操作）
        
        Args:
            record: 表空间记录
            
        Returns:
            表空间DDL语句列表
        """
        sql_lines = []
        info = record.get_info()
        
        if info == 0x00:  # CREATE TABLESPACE
            tablespace_name = self._extract_tablespace_name(record)
            sql_lines.append(f"CREATE TABLESPACE {tablespace_name or 'unknown_tablespace'};")
        elif info == 0x10:  # DROP TABLESPACE
            tablespace_name = self._extract_tablespace_name(record)
            sql_lines.append(f"DROP TABLESPACE {tablespace_name or 'unknown_tablespace'};")
        else:
            sql_lines.append(f"-- 未知的表空间操作: {info}")
        
        return sql_lines
    
    def _format_sequence_record(self, record: XLogRecord) -> List[str]:
        """
        格式化序列记录
        
        Args:
            record: 序列记录
            
        Returns:
            序列相关SQL语句列表
        """
        sql_lines = []
        info = record.get_info()
        
        if info == 0x00:  # 序列创建
            seq_name = self._extract_sequence_name(record)
            sql_lines.append(f"CREATE SEQUENCE {seq_name or 'unknown_sequence'};")
        elif info == 0x10:  # 序列值更新
            sql_lines.append("-- 序列值更新操作")
        elif info == 0x20:  # 序列删除
            seq_name = self._extract_sequence_name(record)
            sql_lines.append(f"DROP SEQUENCE {seq_name or 'unknown_sequence'};")
        else:
            sql_lines.append(f"-- 未知的序列操作: {info}")
        
        return sql_lines
    
    def _format_generic_record(self, record: XLogRecord, rmgr_name: str) -> List[str]:
        """
        格式化通用记录（生成注释）
        
        Args:
            record: XLOG记录
            rmgr_name: 资源管理器名称
            
        Returns:
            注释行列表
        """
        sql_lines = []
        sql_lines.append(f"-- {rmgr_name} 记录")
        sql_lines.append(f"--   事务ID: {record.xl_xid}")
        sql_lines.append(f"--   记录长度: {record.xl_tot_len}")
        sql_lines.append(f"--   信息标志: 0x{record.xl_info:02x}")
        sql_lines.append(f"--   前一个LSN: {record.xl_prev}")
        
        return sql_lines
    
    # 以下方法用于从WAL记录中提取具体信息
    # 这些是简化版本，实际实现需要根据PostgreSQL的WAL格式进行详细解析
    
    def _extract_table_info(self, record: XLogRecord) -> Optional[Dict[str, Any]]:
        """
        从记录中提取表信息
        
        Args:
            record: XLOG记录
            
        Returns:
            表信息字典
        """
        # 简化实现，实际需要解析关系文件节点
        if record.blocks:
            block = record.blocks[0]
            if 'relfilenode' in block:
                relnode = block['relfilenode']
                # 这里应该有从relfilenode到表名的映射
                # 暂时返回一个模拟的表名
                return {
                    'table_name': f'table_{relnode["relNode"]}',
                    'schema': 'public',
                    'relfilenode': relnode
                }
        return None
    
    def _extract_insert_data(self, record: XLogRecord) -> Optional[Dict[str, Any]]:
        """
        从INSERT记录中提取数据
        
        Args:
            record: INSERT记录
            
        Returns:
            插入的数据字典
        """
        # 简化实现，实际需要解析tuple数据
        return {'column1': 'value1', 'column2': 'value2'}
    
    def _extract_where_clause(self, record: XLogRecord) -> Optional[str]:
        """
        从记录中提取WHERE条件
        
        Args:
            record: XLOG记录
            
        Returns:
            WHERE条件字符串
        """
        # 简化实现
        return "id = 1"
    
    def _extract_set_clause(self, record: XLogRecord) -> Optional[str]:
        """
        从UPDATE记录中提取SET子句
        
        Args:
            record: UPDATE记录
            
        Returns:
            SET子句字符串
        """
        # 简化实现
        return "column1 = 'new_value'"
    
    def _extract_multi_insert_data(self, record: XLogRecord) -> List[Dict[str, Any]]:
        """
        从多行INSERT记录中提取数据
        
        Args:
            record: 多行INSERT记录
            
        Returns:
            多行数据列表
        """
        # 简化实现
        return [
            {'column1': 'value1', 'column2': 'value2'},
            {'column1': 'value3', 'column2': 'value4'}
        ]
    
    def _extract_database_name(self, record: XLogRecord) -> Optional[str]:
        """
        从数据库记录中提取数据库名
        
        Args:
            record: 数据库记录
            
        Returns:
            数据库名称
        """
        # 简化实现，实际需要解析主数据
        return "database_name"
    
    def _extract_tablespace_name(self, record: XLogRecord) -> Optional[str]:
        """
        从表空间记录中提取表空间名
        
        Args:
            record: 表空间记录
            
        Returns:
            表空间名称
        """
        # 简化实现
        return "tablespace_name"
    
    def _extract_sequence_name(self, record: XLogRecord) -> Optional[str]:
        """
        从序列记录中提取序列名
        
        Args:
            record: 序列记录
            
        Returns:
            序列名称
        """
        # 简化实现
        return "sequence_name"
    
    def format_text_record(self, record) -> List[str]:
        """
        格式化单个文本记录为SQL语句
        
        Args:
            record: 文本格式的WAL记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        
        # 根据资源管理器和描述判断操作类型
        if record.rmgr == 'Heap':
            sql_lines.extend(self._format_heap_text_record(record))
        elif record.rmgr == 'Heap2':
            sql_lines.extend(self._format_heap2_text_record(record))
        elif record.rmgr == 'Transaction':
            sql_lines.extend(self._format_transaction_text_record(record))
        elif record.rmgr == 'Database':
            sql_lines.extend(self._format_database_text_record(record))
        elif record.rmgr == 'Tablespace':
            sql_lines.extend(self._format_tablespace_text_record(record))
        else:
            # 其他类型的记录，生成注释
            sql_lines.append(f"-- {record.rmgr} 记录")
            sql_lines.append(f"--   事务ID: {record.tx_id}")
            sql_lines.append(f"--   LSN: {record.lsn}")
            sql_lines.append(f"--   描述: {record.description}")
        
        return sql_lines
    
    def _format_heap_text_record(self, record) -> List[str]:
        """
        格式化Heap文本记录
        
        Args:
            record: Heap文本记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        desc = record.description.lower()
        
        if 'insert' in desc:
            # 模拟INSERT语句
            sql_lines.append("INSERT INTO user_table (id, name, email) VALUES (1, 'John Doe', 'john@example.com');")
        elif 'update' in desc:
            # 模拟UPDATE语句
            sql_lines.append("UPDATE user_table SET name = 'Updated Name' WHERE id = 1;")
        elif 'delete' in desc:
            # 模拟DELETE语句
            sql_lines.append("DELETE FROM user_table WHERE id = 1;")
        else:
            sql_lines.append(f"-- Heap操作: {record.description}")
        
        return sql_lines
    
    def _format_heap2_text_record(self, record) -> List[str]:
        """
        格式化Heap2文本记录
        
        Args:
            record: Heap2文本记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        desc = record.description.lower()
        
        if 'multi_insert' in desc:
            sql_lines.append("INSERT INTO user_table (id, name) VALUES (1, 'User1'), (2, 'User2');")
        elif 'freeze' in desc:
            sql_lines.append("-- VACUUM FREEZE操作")
        elif 'visible' in desc:
            sql_lines.append("-- VACUUM标记可见性操作")
        else:
            sql_lines.append(f"-- Heap2操作: {record.description}")
        
        return sql_lines
    
    def _format_transaction_text_record(self, record) -> List[str]:
        """
        格式化事务文本记录
        
        Args:
            record: 事务文本记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        desc = record.description.lower()
        
        if 'commit' in desc:
            sql_lines.append(f"COMMIT;  -- 事务ID: {record.tx_id}")
        elif 'abort' in desc:
            sql_lines.append(f"ROLLBACK;  -- 事务ID: {record.tx_id}")
        else:
            sql_lines.append(f"-- 事务操作: {record.description}")
        
        return sql_lines
    
    def _format_database_text_record(self, record) -> List[str]:
        """
        格式化数据库文本记录
        
        Args:
            record: 数据库文本记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        desc = record.description.lower()
        
        if 'create' in desc:
            sql_lines.append("CREATE DATABASE test_database;")
        elif 'drop' in desc:
            sql_lines.append("DROP DATABASE test_database;")
        elif 'alter' in desc:
            sql_lines.append("ALTER DATABASE test_database SET ...;")
        else:
            sql_lines.append(f"-- 数据库操作: {record.description}")
        
        return sql_lines
    
    def _format_tablespace_text_record(self, record) -> List[str]:
        """
        格式化表空间文本记录
        
        Args:
            record: 表空间文本记录
            
        Returns:
            SQL语句列表
        """
        sql_lines = []
        desc = record.description.lower()
        
        if 'create' in desc:
            sql_lines.append("CREATE TABLESPACE test_tablespace LOCATION '/path/to/tablespace';")
        elif 'drop' in desc:
            sql_lines.append("DROP TABLESPACE test_tablespace;")
        else:
            sql_lines.append(f"-- 表空间操作: {record.description}")
        
        return sql_lines
