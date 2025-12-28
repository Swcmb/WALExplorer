"""
WAL文本文件解析器
用于解析pg_waldump输出的文本格式WAL记录
"""

import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from core.wal_parser import get_rmgr_name


@dataclass
class WALTextRecord:
    """
    文本格式的WAL记录
    """
    rmgr: str              # 资源管理器名称
    rmgr_id: int           # 资源管理器ID
    length: int            # 记录长度
    total_length: int      # 总长度
    tx_id: int             # 事务ID
    lsn: str               # LSN
    prev_lsn: str          # 前一个LSN
    description: str       # 描述信息
    raw_line: str          # 原始文本行


class WALTextParser:
    """
    WAL文本文件解析器
    解析pg_waldump工具输出的文本格式
    """
    
    def __init__(self):
        """
        初始化文本解析器
        """
        # 资源管理器名称到ID的映射
        self.rmgr_name_to_id = {
            'XLOG': 0,
            'Transaction': 1,
            'Storage': 2,
            'CLOG': 3,
            'Database': 4,
            'Tablespace': 5,
            'MultiXact': 6,
            'RelMap': 7,
            'Standby': 8,
            'Heap2': 9,
            'Heap': 10,
            'Btree': 11,
            'Hash': 12,
            'Gin': 13,
            'Gist': 14,
            'Sequence': 15,
            'SPGist': 16,
            'BRIN': 17,
            'Generic': 18,
            'Logical': 19,
            'Dist': 20,
            'CommitTs': 21,
            'ReplicationOrigin': 22,
            'ReplicationSlot': 23,
            'Heap3': 24
        }
        
        # 正则表达式模式
        self.record_pattern = re.compile(
            r'rmgr:\s+(\w+)\s+len\s\(rec/tot\):\s+(\d+)/\s*(\d+),\s+tx:\s*(\d+),\s+lsn:\s+([^,]+),\s+prev\s+([^,]+),\s+desc:\s+(.+)'
        )
    
    def parse_text_file(self, file_path: str) -> List[WALTextRecord]:
        """
        解析WAL文本文件
        
        Args:
            file_path: 文本文件路径
            
        Returns:
            解析后的WAL记录列表
        """
        records = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    record = self._parse_line(line)
                    if record:
                        records.append(record)
                        
        except FileNotFoundError:
            print(f"错误: 文件不存在: {file_path}")
        except Exception as e:
            print(f"解析文件时出错: {e}")
        
        return records
    
    def _parse_line(self, line: str) -> Optional[WALTextRecord]:
        """
        解析单行WAL记录
        
        Args:
            line: 文本行
            
        Returns:
            解析后的记录或None
        """
        match = self.record_pattern.match(line)
        if not match:
            return None
        
        rmgr_name = match.group(1)
        length = int(match.group(2))
        total_length = int(match.group(3))
        tx_id = int(match.group(4))
        lsn = match.group(5)
        prev_lsn = match.group(6)
        description = match.group(7)
        
        # 获取资源管理器ID
        rmgr_id = self.rmgr_name_to_id.get(rmgr_name, -1)
        
        return WALTextRecord(
            rmgr=rmgr_name,
            rmgr_id=rmgr_id,
            length=length,
            total_length=total_length,
            tx_id=tx_id,
            lsn=lsn,
            prev_lsn=prev_lsn,
            description=description,
            raw_line=line
        )
    
    def filter_by_rmgr(self, records: List[WALTextRecord], rmgr_name: str) -> List[WALTextRecord]:
        """
        按资源管理器名称过滤记录
        
        Args:
            records: 记录列表
            rmgr_name: 资源管理器名称
            
        Returns:
            过滤后的记录列表
        """
        return [r for r in records if r.rmgr == rmgr_name]
    
    def filter_by_rmgr_id(self, records: List[WALTextRecord], rmgr_id: int) -> List[WALTextRecord]:
        """
        按资源管理器ID过滤记录
        
        Args:
            records: 记录列表
            rmgr_id: 资源管理器ID
            
        Returns:
            过滤后的记录列表
        """
        return [r for r in records if r.rmgr_id == rmgr_id]
    
    def filter_by_tx_id(self, records: List[WALTextRecord], tx_id: int) -> List[WALTextRecord]:
        """
        按事务ID过滤记录
        
        Args:
            records: 记录列表
            tx_id: 事务ID
            
        Returns:
            过滤后的记录列表
        """
        return [r for r in records if r.tx_id == tx_id]
    
    def get_statistics(self, records: List[WALTextRecord]) -> Dict[str, Any]:
        """
        获取记录统计信息
        
        Args:
            records: 记录列表
            
        Returns:
            统计信息字典
        """
        if not records:
            return {}
        
        # 按资源管理器统计
        rmgr_stats = {}
        for record in records:
            rmgr_stats[record.rmgr] = rmgr_stats.get(record.rmgr, 0) + 1
        
        # 按事务统计
        tx_stats = {}
        for record in records:
            if record.tx_id != 0:  # 排除系统事务
                tx_stats[record.tx_id] = tx_stats.get(record.tx_id, 0) + 1
        
        return {
            'total_records': len(records),
            'rmgr_statistics': rmgr_stats,
            'transaction_statistics': tx_stats,
            'lsn_range': {
                'start': records[0].lsn if records else None,
                'end': records[-1].lsn if records else None
            }
        }
    
    def group_by_transaction(self, records: List[WALTextRecord]) -> Dict[int, List[WALTextRecord]]:
        """
        按事务分组记录
        
        Args:
            records: 记录列表
            
        Returns:
            按事务ID分组的记录字典
        """
        grouped = {}
        for record in records:
            if record.tx_id not in grouped:
                grouped[record.tx_id] = []
            grouped[record.tx_id].append(record)
        
        return grouped
    
    def find_dml_operations(self, records: List[WALTextRecord]) -> List[WALTextRecord]:
        """
        查找DML操作记录
        
        Args:
            records: 记录列表
            
        Returns:
            DML操作记录列表
        """
        dml_records = []
        
        for record in records:
            if record.rmgr in ['Heap', 'Heap2']:
                # 检查描述中是否包含DML操作
                desc = record.description.lower()
                if any(op in desc for op in ['insert', 'update', 'delete', 'multi_insert']):
                    dml_records.append(record)
        
        return dml_records
    
    def find_ddl_operations(self, records: List[WALTextRecord]) -> List[WALTextRecord]:
        """
        查找DDL操作记录
        
        Args:
            records: 记录列表
            
        Returns:
            DDL操作记录列表
        """
        ddl_records = []
        
        for record in records:
            if record.rmgr in ['Database', 'Tablespace']:
                ddl_records.append(record)
            elif record.rmgr in ['Heap', 'Heap2'] and record.tx_id == 0:
                # 系统事务可能是DDL操作
                desc = record.description.lower()
                if any(op in desc for op in ['create', 'drop', 'alter']):
                    ddl_records.append(record)
        
        return ddl_records