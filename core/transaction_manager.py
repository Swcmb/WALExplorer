"""
事务管理器
负责管理和跟踪PostgreSQL事务的状态和操作
"""

from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
from enum import Enum
from core.wal_parser import XLogRecord, get_rmgr_name


class TransactionState(Enum):
    """
    事务状态枚举
    """
    IN_PROGRESS = "in_progress"
    COMMITTED = "committed"
    ABORTED = "aborted"
    PREPARED = "prepared"


@dataclass
class TransactionInfo:
    """
    事务信息类
    """
    xid: int                                          # 事务ID
    state: TransactionState = TransactionState.IN_PROGRESS  # 事务状态
    start_lsn: Optional[str] = None                   # 开始LSN
    commit_lsn: Optional[str] = None                  # 提交LSN
    records: List[XLogRecord] = field(default_factory=list)  # 事务包含的记录
    savepoints: List[int] = field(default_factory=list)     # 保存点列表
    subtransactions: Set[int] = field(default_factory=set)  # 子事务集合
    parent_xid: Optional[int] = None                  # 父事务ID
    
    def add_record(self, record: XLogRecord):
        """
        添加记录到事务
        
        Args:
            record: XLOG记录
        """
        self.records.append(record)
    
    def add_subtransaction(self, subxid: int):
        """
        添加子事务
        
        Args:
            subxid: 子事务ID
        """
        self.subtransactions.add(subxid)
    
    def is_subtransaction(self) -> bool:
        """
        检查是否是子事务
        
        Returns:
            如果是子事务返回True
        """
        return self.parent_xid is not None
    
    def get_all_xids(self) -> Set[int]:
        """
        获取事务及其所有子事务的ID
        
        Returns:
            事务ID集合
        """
        result = {self.xid}
        result.update(self.subtransactions)
        return result


class TransactionManager:
    """
    事务管理器
    负责跟踪和管理WAL记录中的事务
    """
    
    def __init__(self):
        """
        初始化事务管理器
        """
        self.transactions: Dict[int, TransactionInfo] = {}  # 活跃事务
        self.committed_transactions: Dict[int, TransactionInfo] = {}  # 已提交事务
        self.aborted_transactions: Dict[int, TransactionInfo] = {}  # 已回滚事务
        self.subtransaction_map: Dict[int, int] = {}  # 子事务到父事务的映射
        
        # 统计信息
        self.total_transactions = 0
        self.committed_count = 0
        self.aborted_count = 0
    
    def process_record(self, record: XLogRecord):
        """
        处理XLOG记录，更新事务状态
        
        Args:
            record: XLOG记录
        """
        rmgr_name = get_rmgr_name(record.xl_rmid)
        
        # 处理事务相关的记录
        if rmgr_name == 'Transaction':
            self._process_transaction_record(record)
        elif record.xl_xid != 0:
            # 处理其他事务记录
            self._process_general_record(record)
    
    def _process_transaction_record(self, record: XLogRecord):
        """
        处理事务管理器记录
        
        Args:
            record: 事务记录
        """
        info = record.get_info()
        xid = record.xl_xid
        
        if info == 0x00:  # XLOG_XACT_COMMIT
            self._commit_transaction(xid, record)
        elif info == 0x10:  # XLOG_XACT_ABORT
            self._abort_transaction(xid, record)
        elif info == 0x20:  # XLOG_XACT_PREPARE
            self._prepare_transaction(xid, record)
        elif info == 0x30:  # XLOG_XACT_COMMIT_PREPARED
            self._commit_prepared_transaction(xid, record)
        elif info == 0x40:  # XLOG_XACT_ABORT_PREPARED
            self._abort_prepared_transaction(xid, record)
        elif info == 0x50:  # XLOG_XACT_ASSIGNMENT
            self._process_assignment(record)
        elif info == 0x60:  # XLOG_XACT_INVALID
            self._process_invalid(record)
        else:
            # 其他事务记录类型
            self._process_other_transaction_record(record, info)
    
    def _process_general_record(self, record: XLogRecord):
        """
        处理一般记录
        
        Args:
            record: XLOG记录
        """
        xid = record.xl_xid
        
        # 如果事务不存在，创建新事务
        if xid not in self.transactions:
            self._create_transaction(xid, record)
        
        # 添加记录到事务
        if xid in self.transactions:
            self.transactions[xid].add_record(record)
    
    def _create_transaction(self, xid: int, record: XLogRecord):
        """
        创建新事务
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        transaction = TransactionInfo(
            xid=xid,
            start_lsn=str(record.xl_prev)
        )
        transaction.add_record(record)
        
        self.transactions[xid] = transaction
        self.total_transactions += 1
    
    def _commit_transaction(self, xid: int, record: XLogRecord):
        """
        提交事务
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        if xid in self.transactions:
            transaction = self.transactions[xid]
            transaction.state = TransactionState.COMMITTED
            transaction.commit_lsn = str(record.xl_prev)
            transaction.add_record(record)
            
            # 移动到已提交事务
            self.committed_transactions[xid] = transaction
            del self.transactions[xid]
            self.committed_count += 1
            
            # 处理子事务
            for subxid in transaction.subtransactions:
                if subxid in self.transactions:
                    subtransaction = self.transactions[subxid]
                    subtransaction.state = TransactionState.COMMITTED
                    subtransaction.commit_lsn = str(record.xl_prev)
                    self.committed_transactions[subxid] = subtransaction
                    del self.transactions[subxid]
    
    def _abort_transaction(self, xid: int, record: XLogRecord):
        """
        回滚事务
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        if xid in self.transactions:
            transaction = self.transactions[xid]
            transaction.state = TransactionState.ABORTED
            transaction.commit_lsn = str(record.xl_prev)
            transaction.add_record(record)
            
            # 移动到已回滚事务
            self.aborted_transactions[xid] = transaction
            del self.transactions[xid]
            self.aborted_count += 1
            
            # 处理子事务
            for subxid in transaction.subtransactions:
                if subxid in self.transactions:
                    subtransaction = self.transactions[subxid]
                    subtransaction.state = TransactionState.ABORTED
                    subtransaction.commit_lsn = str(record.xl_prev)
                    self.aborted_transactions[subxid] = subtransaction
                    del self.transactions[subxid]
    
    def _prepare_transaction(self, xid: int, record: XLogRecord):
        """
        准备事务（两阶段提交）
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        if xid in self.transactions:
            transaction = self.transactions[xid]
            transaction.state = TransactionState.PREPARED
            transaction.add_record(record)
    
    def _commit_prepared_transaction(self, xid: int, record: XLogRecord):
        """
        提交已准备的事务
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        if xid in self.transactions:
            self._commit_transaction(xid, record)
    
    def _abort_prepared_transaction(self, xid: int, record: XLogRecord):
        """
        回滚已准备的事务
        
        Args:
            xid: 事务ID
            record: XLOG记录
        """
        if xid in self.transactions:
            self._abort_transaction(xid, record)
    
    def _process_assignment(self, record: XLogRecord):
        """
        处理事务分配记录
        
        Args:
            record: 分配记录
        """
        # 处理子事务分配
        pass
    
    def _process_invalid(self, record: XLogRecord):
        """
        处理无效事务记录
        
        Args:
            record: 无效记录
        """
        pass
    
    def _process_other_transaction_record(self, record: XLogRecord, info: int):
        """
        处理其他事务记录类型
        
        Args:
            record: XLOG记录
            info: 信息标志
        """
        xid = record.xl_xid
        
        if xid not in self.transactions:
            self._create_transaction(xid, record)
        else:
            self.transactions[xid].add_record(record)
    
    def get_transaction(self, xid: int) -> Optional[TransactionInfo]:
        """
        获取事务信息
        
        Args:
            xid: 事务ID
            
        Returns:
            事务信息
        """
        # 在活跃事务中查找
        if xid in self.transactions:
            return self.transactions[xid]
        
        # 在已提交事务中查找
        if xid in self.committed_transactions:
            return self.committed_transactions[xid]
        
        # 在已回滚事务中查找
        if xid in self.aborted_transactions:
            return self.aborted_transactions[xid]
        
        return None
    
    def get_active_transactions(self) -> Dict[int, TransactionInfo]:
        """
        获取所有活跃事务
        
        Returns:
            活跃事务字典
        """
        return self.transactions.copy()
    
    def get_committed_transactions(self) -> Dict[int, TransactionInfo]:
        """
        获取所有已提交事务
        
        Returns:
            已提交事务字典
        """
        return self.committed_transactions.copy()
    
    def get_aborted_transactions(self) -> Dict[int, TransactionInfo]:
        """
        获取所有已回滚事务
        
        Returns:
            已回滚事务字典
        """
        return self.aborted_transactions.copy()
    
    def get_transaction_records(self, xid: int) -> List[XLogRecord]:
        """
        获取事务的所有记录
        
        Args:
            xid: 事务ID
            
        Returns:
            记录列表
        """
        transaction = self.get_transaction(xid)
        if transaction:
            return transaction.records.copy()
        return []
    
    def get_committed_records(self) -> List[XLogRecord]:
        """
        获取所有已提交事务的记录
        
        Returns:
            记录列表
        """
        records = []
        for transaction in self.committed_transactions.values():
            records.extend(transaction.records)
        return records
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        获取事务统计信息
        
        Returns:
            统计信息字典
        """
        return {
            'total_transactions': self.total_transactions,
            'active_count': len(self.transactions),
            'committed_count': self.committed_count,
            'aborted_count': self.aborted_count,
            'committed_transactions': self.committed_count,
            'aborted_transactions': self.aborted_count
        }
    
    def reset(self):
        """
        重置事务管理器状态
        """
        self.transactions.clear()
        self.committed_transactions.clear()
        self.aborted_transactions.clear()
        self.subtransaction_map.clear()
        self.total_transactions = 0
        self.committed_count = 0
        self.aborted_count = 0
    
    def add_subtransaction(self, parent_xid: int, subxid: int):
        """
        添加子事务关系
        
        Args:
            parent_xid: 父事务ID
            subxid: 子事务ID
        """
        self.subtransaction_map[subxid] = parent_xid
        
        # 如果父事务存在，添加子事务
        if parent_xid in self.transactions:
            self.transactions[parent_xid].add_subtransaction(subxid)
        
        # 创建子事务
        if subxid not in self.transactions:
            subtransaction = TransactionInfo(
                xid=subxid,
                parent_xid=parent_xid
            )
            self.transactions[subxid] = subtransaction
    
    def get_parent_transaction(self, subxid: int) -> Optional[int]:
        """
        获取子事务的父事务ID
        
        Args:
            subxid: 子事务ID
            
        Returns:
            父事务ID
        """
        return self.subtransaction_map.get(subxid)
    
    def is_transaction_active(self, xid: int) -> bool:
        """
        检查事务是否活跃
        
        Args:
            xid: 事务ID
            
        Returns:
            如果事务活跃返回True
        """
        return xid in self.transactions
    
    def is_transaction_committed(self, xid: int) -> bool:
        """
        检查事务是否已提交
        
        Args:
            xid: 事务ID
            
        Returns:
            如果事务已提交返回True
        """
        return xid in self.committed_transactions
    
    def is_transaction_aborted(self, xid: int) -> bool:
        """
        检查事务是否已回滚
        
        Args:
            xid: 事务ID
            
        Returns:
            如果事务已回滚返回True
        """
        return xid in self.aborted_transactions