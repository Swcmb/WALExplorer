"""
Heap记录解析器
负责解析PostgreSQL的Heap和Heap2记录，提取DML操作信息
"""

import struct
from typing import Dict, Any, List, Optional, Tuple
from utils.binary_reader import BinaryReader
from core.wal_parser import XLogRecord


class HeapTupleData:
    """
    Heap元组数据结构
    """
    
    def __init__(self, reader: BinaryReader):
        """
        从二进制数据中解析Heap元组
        
        Args:
            reader: 二进制数据读取器
        """
        self.t_xmin = reader.read_uint32()      # 插入事务ID
        self.t_xmax = reader.read_uint32()      # 删除事务ID
        self.t_cid = reader.read_uint32()       # 命令ID
        self.t_xmin_committed = reader.read_uint8()  # xmin提交状态
        self.t_xmax_committed = reader.read_uint8()  # xmax提交状态
        self.t_infomask2 = reader.read_uint16()  # 信息掩码2
        self.t_infomask = reader.read_uint16()   # 信息掩码
        self.t_hoff = reader.read_uint8()        # 头部偏移量
        self.t_bits = None                       # NULL位图
        
        # 解析NULL位图（如果存在）
        if self.t_infomask & 0x0001:  # HEAP_HASNULL
            bit_len = (self.t_hoff - (23 + 1)) * 8  # 计算位图长度
            if bit_len > 0:
                self.t_bits = reader.read_bytes((bit_len + 7) // 8)
        
        # 元组数据从t_hoff位置开始
        self.data_start = reader.tell()
        self.data_length = 0  # 需要根据上下文确定


class HeapInsertInfo:
    """
    Heap插入操作信息
    """
    
    def __init__(self):
        self.table_oid = 0          # 表OID
        self.table_name = ""        # 表名
        self.schema_name = ""       # 模式名
        self.columns = []           # 列信息
        self.values = []            # 插入的值
        self.is_catalog_update = False  # 是否是系统表更新


class HeapDeleteInfo:
    """
    Heap删除操作信息
    """
    
    def __init__(self):
        self.table_oid = 0          # 表OID
        self.table_name = ""        # 表名
        self.schema_name = ""       # 模式名
        self.where_conditions = []  # WHERE条件
        self.is_catalog_update = False  # 是否是系统表更新


class HeapUpdateInfo:
    """
    Heap更新操作信息
    """
    
    def __init__(self):
        self.table_oid = 0          # 表OID
        self.table_name = ""        # 表名
        self.schema_name = ""       # 模式名
        self.old_values = {}        # 旧值
        self.new_values = {}        # 新值
        self.where_conditions = []  # WHERE条件
        self.is_catalog_update = False  # 是否是系统表更新
        self.is_hot_update = False  # 是否是HOT更新


class HeapParser:
    """
    Heap记录解析器
    """
    
    # Heap操作类型常量
    XLOG_HEAP_INSERT = 0x00
    XLOG_HEAP_DELETE = 0x10
    XLOG_HEAP_UPDATE = 0x20
    XLOG_HEAP_HOT_UPDATE = 0x40
    
    # Heap2操作类型常量
    XLOG_HEAP2_MULTI_INSERT = 0x00
    XLOG_HEAP2_FREEZE = 0x10
    XLOG_HEAP2_CLEAN = 0x20
    XLOG_HEAP2_VISIBLE = 0x30
    XLOG_HEAP2_MULTI_INSERT_PURGE = 0x40
    XLOG_HEAP2_LOCK_UPDATED = 0x50
    
    def __init__(self):
        """
        初始化Heap解析器
        """
        self.catalog_tables = {
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
            6100: 'pg_replication_origin', # pg_replication_origin OID
            6000: 'pg_replication_slot', # pg_replication_slot OID
            6001: 'pg_replication_slot', # pg_replication_slot OID
            6002: 'pg_replication_slot', # pg_replication_slot OID
            6003: 'pg_replication_slot', # pg_replication_slot OID
            6004: 'pg_replication_slot', # pg_replication_slot OID
            6005: 'pg_replication_slot', # pg_replication_slot OID
            6006: 'pg_replication_slot', # pg_replication_slot OID
            6007: 'pg_replication_slot', # pg_replication_slot OID
            6008: 'pg_replication_slot', # pg_replication_slot OID
            6009: 'pg_replication_slot', # pg_replication_slot OID
            6010: 'pg_replication_slot', # pg_replication_slot OID
        }
    
    def parse_heap_record(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析Heap记录
        
        Args:
            record: XLOG记录
            
        Returns:
            解析结果字典
        """
        info = record.get_info()
        
        if info == self.XLOG_HEAP_INSERT:
            return self._parse_insert(record)
        elif info == self.XLOG_HEAP_DELETE:
            return self._parse_delete(record)
        elif info == self.XLOG_HEAP_UPDATE:
            return self._parse_update(record)
        elif info == self.XLOG_HEAP_HOT_UPDATE:
            return self._parse_hot_update(record)
        else:
            return {'operation': 'unknown', 'info': info}
    
    def parse_heap2_record(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析Heap2记录
        
        Args:
            record: XLOG记录
            
        Returns:
            解析结果字典
        """
        info = record.get_info()
        
        if info == self.XLOG_HEAP2_MULTI_INSERT:
            return self._parse_multi_insert(record)
        elif info == self.XLOG_HEAP2_FREEZE:
            return self._parse_freeze(record)
        elif info == self.XLOG_HEAP2_CLEAN:
            return self._parse_clean(record)
        elif info == self.XLOG_HEAP2_VISIBLE:
            return self._parse_visible(record)
        else:
            return {'operation': 'unknown_heap2', 'info': info}
    
    def _parse_insert(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析INSERT操作
        
        Args:
            record: INSERT记录
            
        Returns:
            INSERT操作信息
        """
        if not record.main_data:
            return {'operation': 'insert', 'error': 'no_main_data'}
        
        reader = BinaryReader(record.main_data)
        
        # 读取块号
        block_num = reader.read_uint32()
        
        # 读取偏移量
        offset_num = reader.read_uint16()
        
        # 读取元组数据
        tuple_data = self._parse_tuple_data(reader)
        
        # 提取表信息
        table_info = self._extract_table_info(record)
        
        # 提取插入的值
        values = self._extract_tuple_values(tuple_data, table_info)
        
        return {
            'operation': 'insert',
            'table_info': table_info,
            'values': values,
            'block_num': block_num,
            'offset_num': offset_num
        }
    
    def _parse_delete(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析DELETE操作
        
        Args:
            record: DELETE记录
            
        Returns:
            DELETE操作信息
        """
        if not record.main_data:
            return {'operation': 'delete', 'error': 'no_main_data'}
        
        reader = BinaryReader(record.main_data)
        
        # 读取块号
        block_num = reader.read_uint32()
        
        # 读取偏移量
        offset_num = reader.read_uint16()
        
        # 读取最新的xmax
        latest_xmax = reader.read_uint32()
        
        # 提取表信息
        table_info = self._extract_table_info(record)
        
        # 提取WHERE条件
        where_conditions = self._extract_where_conditions(record, offset_num)
        
        return {
            'operation': 'delete',
            'table_info': table_info,
            'where_conditions': where_conditions,
            'block_num': block_num,
            'offset_num': offset_num,
            'latest_xmax': latest_xmax
        }
    
    def _parse_update(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析UPDATE操作
        
        Args:
            record: UPDATE记录
            
        Returns:
            UPDATE操作信息
        """
        if not record.main_data:
            return {'operation': 'update', 'error': 'no_main_data'}
        
        reader = BinaryReader(record.main_data)
        
        # 读取块号
        block_num = reader.read_uint32()
        
        # 读取偏移量
        offset_num = reader.read_uint16()
        
        # 读取最新的xmax
        latest_xmax = reader.read_uint32()
        
        # 读取新元组数据
        new_tuple_data = self._parse_tuple_data(reader)
        
        # 提取表信息
        table_info = self._extract_table_info(record)
        
        # 提取新值
        new_values = self._extract_tuple_values(new_tuple_data, table_info)
        
        # 提取WHERE条件
        where_conditions = self._extract_where_conditions(record, offset_num)
        
        return {
            'operation': 'update',
            'table_info': table_info,
            'new_values': new_values,
            'where_conditions': where_conditions,
            'block_num': block_num,
            'offset_num': offset_num,
            'latest_xmax': latest_xmax,
            'is_hot_update': False
        }
    
    def _parse_hot_update(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析HOT UPDATE操作
        
        Args:
            record: HOT UPDATE记录
            
        Returns:
            HOT UPDATE操作信息
        """
        update_info = self._parse_update(record)
        update_info['is_hot_update'] = True
        return update_info
    
    def _parse_multi_insert(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析多行INSERT操作
        
        Args:
            record: 多行INSERT记录
            
        Returns:
            多行INSERT操作信息
        """
        if not record.main_data:
            return {'operation': 'multi_insert', 'error': 'no_main_data'}
        
        reader = BinaryReader(record.main_data)
        
        # 读取标志
        flags = reader.read_uint8()
        
        # 读取块号
        block_num = reader.read_uint32()
        
        # 读取元组数量
        ntuples = reader.read_uint16()
        
        # 读取偏移量数组
        offsets = []
        for i in range(ntuples):
            offsets.append(reader.read_uint16())
        
        # 读取每个元组的数据
        tuples = []
        for i in range(ntuples):
            tuple_data = self._parse_tuple_data(reader)
            tuples.append(tuple_data)
        
        # 提取表信息
        table_info = self._extract_table_info(record)
        
        # 提取所有行的值
        rows_values = []
        for tuple_data in tuples:
            values = self._extract_tuple_values(tuple_data, table_info)
            rows_values.append(values)
        
        return {
            'operation': 'multi_insert',
            'table_info': table_info,
            'rows_values': rows_values,
            'block_num': block_num,
            'offsets': offsets,
            'flags': flags
        }
    
    def _parse_freeze(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析FREEZE操作
        
        Args:
            record: FREEZE记录
            
        Returns:
            FREEZE操作信息
        """
        return {
            'operation': 'freeze',
            'description': 'VACUUM FREEZE operation'
        }
    
    def _parse_clean(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析CLEAN操作
        
        Args:
            record: CLEAN记录
            
        Returns:
            CLEAN操作信息
        """
        return {
            'operation': 'clean',
            'description': 'VACUUM CLEAN operation'
        }
    
    def _parse_visible(self, record: XLogRecord) -> Dict[str, Any]:
        """
        解析VISIBLE操作
        
        Args:
            record: VISIBLE记录
            
        Returns:
            VISIBLE操作信息
        """
        return {
            'operation': 'visible',
            'description': 'VACUUM visibility marking operation'
        }
    
    def _parse_tuple_data(self, reader: BinaryReader) -> HeapTupleData:
        """
        解析元组数据
        
        Args:
            reader: 二进制数据读取器
            
        Returns:
            元组数据对象
        """
        return HeapTupleData(reader)
    
    def _extract_table_info(self, record: XLogRecord) -> Dict[str, Any]:
        """
        从记录中提取表信息
        
        Args:
            record: XLOG记录
            
        Returns:
            表信息字典
        """
        table_info = {
            'oid': 0,
            'name': 'unknown_table',
            'schema': 'public',
            'is_catalog': False
        }
        
        # 从块信息中提取关系文件节点
        if record.blocks:
            block = record.blocks[0]
            if 'relfilenode' in block:
                relnode = block['relfilenode']
                table_info['relfilenode'] = relnode
                
                # 检查是否是系统表
                if relnode['relNode'] in self.catalog_tables:
                    table_info['oid'] = relnode['relNode']
                    table_info['name'] = self.catalog_tables[relnode['relNode']]
                    table_info['schema'] = 'pg_catalog'
                    table_info['is_catalog'] = True
                else:
                    # 用户表，这里简化处理
                    table_info['name'] = f'user_table_{relnode["relNode"]}'
        
        return table_info
    
    def _extract_tuple_values(self, tuple_data: HeapTupleData, 
                            table_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        从元组数据中提取值
        
        Args:
            tuple_data: 元组数据
            table_info: 表信息
            
        Returns:
            值字典
        """
        values = {}
        
        # 简化实现，实际需要根据表结构解析
        if table_info['is_catalog']:
            # 系统表，根据表类型解析
            if table_info['name'] == 'pg_class':
                values = self._parse_pg_class_tuple(tuple_data)
            elif table_info['name'] == 'pg_attribute':
                values = self._parse_pg_attribute_tuple(tuple_data)
            elif table_info['name'] == 'pg_type':
                values = self._parse_pg_type_tuple(tuple_data)
            else:
                values = {'data': 'binary_data'}
        else:
            # 用户表，简化处理
            values = {
                'id': 1,
                'data': 'sample_data'
            }
        
        return values
    
    def _extract_where_conditions(self, record: XLogRecord, 
                                offset_num: int) -> List[str]:
        """
        提取WHERE条件
        
        Args:
            record: XLOG记录
            offset_num: 偏移量
            
        Returns:
            WHERE条件列表
        """
        # 简化实现，实际需要根据主键或唯一索引生成
        return [f"ctid = '({record.blocks[0]['block_num']},{offset_num})'"]
    
    def _parse_pg_class_tuple(self, tuple_data: HeapTupleData) -> Dict[str, Any]:
        """
        解析pg_class元组
        
        Args:
            tuple_data: 元组数据
            
        Returns:
            pg_class字段值
        """
        # 简化实现，实际需要根据pg_class表结构解析
        return {
            'relname': 'table_name',
            'relnamespace': 2200,
            'reltype': 0,
            'reloftype': 0,
            'relowner': 10,
            'relam': 0,
            'relfilenode': 0,
            'reltablespace': 0,
            'relpages': 0,
            'reltuples': 0,
            'relallvisible': 0,
            'reltoastrelid': 0,
            'relhasindex': False,
            'relisshared': False,
            'relpersistence': 'p',
            'relkind': 'r',
            'relnatts': 0,
            'relchecks': 0,
            'relhasrules': False,
            'relhastriggers': False,
            'relhassubclass': False,
            'relrowsecurity': False,
            'relforcerowsecurity': False,
            'relispopulated': True,
            'relreplident': 'n',
            'relfrozenxid': 0,
            'relminmxid': 0
        }
    
    def _parse_pg_attribute_tuple(self, tuple_data: HeapTupleData) -> Dict[str, Any]:
        """
        解析pg_attribute元组
        
        Args:
            tuple_data: 元组数据
            
        Returns:
            pg_attribute字段值
        """
        return {
            'attrelid': 0,
            'attname': 'column_name',
            'atttypid': 0,
            'attstattarget': 0,
            'attlen': 0,
            'attnum': 0,
            'attndims': 0,
            'attcacheoff': -1,
            'atttypmod': -1,
            'attbyval': False,
            'attstorage': 'p',
            'attalign': 'i',
            'attnotnull': False,
            'atthasdef': False,
            'atthasmissing': False,
            'attidentity': '',
            'attgenerated': '',
            'attisdropped': False,
            'attislocal': True,
            'attinhcount': 0,
            'attcollation': 0
        }
    
    def _parse_pg_type_tuple(self, tuple_data: HeapTupleData) -> Dict[str, Any]:
        """
        解析pg_type元组
        
        Args:
            tuple_data: 元组数据
            
        Returns:
            pg_type字段值
        """
        return {
            'typname': 'type_name',
            'typnamespace': 2200,
            'typowner': 10,
            'typlen': 0,
            'typbyval': False,
            'typtype': 'b',
            'typcategory': 'U',
            'typispreferred': False,
            'typisdefined': True,
            'typdelim': ',',
            'typrelid': 0,
            'typelem': 0,
            'typarray': 0,
            'typinput': 0,
            'typoutput': 0,
            'typreceive': 0,
            'typsend': 0,
            'typmodin': 0,
            'typmodout': 0,
            'typanalyze': 0,
            'typalign': 'i',
            'typstorage': 'p',
            'typnotnull': False,
            'typbasetype': 0,
            'typtypmod': -1,
            'typndims': 0,
            'typcollation': 0
        }