"""
XLOG记录读取器
负责高效读取和解析WAL文件中的XLOG记录
"""

import os
from typing import Iterator, List, Optional, Tuple, Union
from pathlib import Path

from core.wal_parser import XLogRecord, WALPageHeader, WALFile
from utils.binary_reader import BinaryReader
from utils.lsn_utils import LSN


class XLogReader:
    """
    XLOG记录读取器
    提供流式读取WAL记录的功能
    """
    
    def __init__(self, wal_file_path: str):
        """
        初始化XLOG读取器
        
        Args:
            wal_file_path: WAL文件路径
        """
        self.wal_file_path = wal_file_path
        self.file_size = os.path.getsize(wal_file_path)
        self.file_handle = None
        self.current_position = 0
        
        # WAL文件常量
        self.WAL_BLOCK_SIZE = 8192
        self.XLOG_PAGE_MAGIC = 0xD099
        
    def __enter__(self):
        """
        上下文管理器入口
        """
        self.file_handle = open(self.wal_file_path, 'rb')
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        上下文管理器出口
        """
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None
    
    def read_records(self, start_lsn: Optional[LSN] = None, 
                    end_lsn: Optional[LSN] = None) -> Iterator[XLogRecord]:
        """
        读取WAL记录
        
        Args:
            start_lsn: 起始LSN（可选）
            end_lsn: 结束LSN（可选）
            
        Yields:
            XLOG记录
        """
        if not self.file_handle:
            raise RuntimeError("XLogReader未正确初始化，请使用with语句")
        
        # 如果指定了起始LSN，跳转到对应位置
        if start_lsn:
            self._seek_to_lsn(start_lsn)
        
        while self.current_position < self.file_size:
            # 检查是否到达结束LSN
            if end_lsn and self._current_lsn() > end_lsn:
                break
            
            # 读取页面
            page_records = self._read_page_records()
            for record in page_records:
                if end_lsn and self._record_lsn(record) > end_lsn:
                    return
                yield record
    
    def _seek_to_lsn(self, lsn: LSN):
        """
        跳转到指定LSN位置
        
        Args:
            lsn: 目标LSN
        """
        # 计算LSN对应的文件位置
        # LSN的高32位是文件号，低32位是文件内偏移
        file_offset = lsn.file_offset
        
        # 对齐到页边界
        page_aligned_offset = (file_offset // self.WAL_BLOCK_SIZE) * self.WAL_BLOCK_SIZE
        
        # 跳转到计算的位置
        self.file_handle.seek(page_aligned_offset)
        self.current_position = page_aligned_offset
    
    def _read_page_records(self) -> List[XLogRecord]:
        """
        读取当前页面的所有记录
        
        Returns:
            页面中的记录列表
        """
        if self.current_position + self.WAL_BLOCK_SIZE > self.file_size:
            return []
        
        # 读取页面数据
        page_data = self.file_handle.read(self.WAL_BLOCK_SIZE)
        self.current_position += len(page_data)
        
        if len(page_data) < self.WAL_BLOCK_SIZE:
            return []
        
        reader = BinaryReader(page_data)
        
        # 读取页头
        try:
            page_header = WALPageHeader(reader)
        except:
            return []
        
        # 检查魔数
        if page_header.magic != self.XLOG_PAGE_MAGIC:
            return []
        
        # 解析页面中的记录
        records = []
        page_start = self.current_position - self.WAL_BLOCK_SIZE
        page_end = page_start + self.WAL_BLOCK_SIZE
        
        while reader.tell() < len(page_data):
            # 检查是否有足够的数据读取记录头
            if reader.remaining_bytes() < XLogRecord.SIZEOF_XLOG_RECORD:
                break
            
            record_start = reader.tell()
            
            try:
                # 读取记录头
                xl_tot_len = reader.peek_bytes(4)
                if len(xl_tot_len) < 4:
                    break
                
                import struct
                total_len = struct.unpack('<I', xl_tot_len)[0]
                
                # 检查记录是否超出页面范围
                if record_start + total_len > len(page_data):
                    break
                
                # 解析完整记录
                record = XLogRecord(reader)
                records.append(record)
                
                # 移动到下一个记录
                next_record = record_start + total_len
                if next_record >= len(page_data):
                    break
                
                reader.seek(next_record)
                
            except (struct.error, EOFError):
                # 记录损坏，跳过此记录
                break
        
        return records
    
    def _current_lsn(self) -> LSN:
        """
        获取当前位置对应的LSN
        
        Returns:
            当前LSN
        """
        # 这里简化处理，实际需要根据文件名和偏移计算
        return LSN(self.current_position)
    
    def _record_lsn(self, record: XLogRecord) -> LSN:
        """
        获取记录的LSN
        
        Args:
            record: XLOG记录
            
        Returns:
            记录LSN
        """
        # 简化处理，使用记录的prev LSN作为近似
        return record.xl_prev


class XLogSegmentReader:
    """
    XLOG段文件读取器
    用于处理多个WAL段文件的连续读取
    """
    
    def __init__(self, segment_directory: str, timeline_id: int = 1):
        """
        初始化XLOG段文件读取器
        
        Args:
            segment_directory: WAL段文件目录
            timeline_id: 时间线ID
        """
        self.segment_directory = Path(segment_directory)
        self.timeline_id = timeline_id
        self.segment_size = 16 * 1024 * 1024  # 16MB
        
        # 查找所有段文件
        self.segment_files = self._find_segment_files()
        self.segment_files.sort()
        
    def _find_segment_files(self) -> List[str]:
        """
        查找所有段文件
        
        Returns:
            段文件路径列表
        """
        segment_files = []
        
        # WAL段文件命名格式: {timeline_id}{segment_id:08X}
        pattern = f"{self.timeline_id:08X}"
        
        for file_path in self.segment_directory.glob(f"{pattern}*"):
            if file_path.is_file():
                segment_files.append(str(file_path))
        
        return segment_files
    
    def read_records(self, start_lsn: Optional[LSN] = None,
                    end_lsn: Optional[LSN] = None) -> Iterator[XLogRecord]:
        """
        从段文件中读取记录
        
        Args:
            start_lsn: 起始LSN（可选）
            end_lsn: 结束LSN（可选）
            
        Yields:
            XLOG记录
        """
        # 确定要读取的段文件范围
        start_segment = 0
        end_segment = len(self.segment_files) - 1
        
        if start_lsn:
            start_segment = self._lsn_to_segment_index(start_lsn)
        
        if end_lsn:
            end_segment = self._lsn_to_segment_index(end_lsn)
        
        # 读取指定范围的段文件
        for i in range(start_segment, min(end_segment + 1, len(self.segment_files))):
            segment_file = self.segment_files[i]
            
            with XLogReader(segment_file) as reader:
                # 计算当前段的LSN范围
                segment_start_lsn = self._segment_index_to_lsn(i)
                segment_end_lsn = self._segment_index_to_lsn(i + 1) - 1
                
                # 调整LSN范围
                segment_start = max(segment_start_lsn, start_lsn) if start_lsn else segment_start_lsn
                segment_end = min(segment_end_lsn, end_lsn) if end_lsn else segment_end_lsn
                
                # 读取记录
                for record in reader.read_records(segment_start, segment_end):
                    yield record
    
    def _lsn_to_segment_index(self, lsn: LSN) -> int:
        """
        将LSN转换为段文件索引
        
        Args:
            lsn: LSN
            
        Returns:
            段文件索引
        """
        # 简化实现，实际需要根据LSN计算段号
        return lsn.file_id
    
    def _segment_index_to_lsn(self, index: int) -> LSN:
        """
        将段文件索引转换为LSN
        
        Args:
            index: 段文件索引
            
        Returns:
            LSN
        """
        # 简化实现
        return LSN((index << 32) | 0)


class XLogFilteredReader:
    """
    XLOG过滤读取器
    提供按条件过滤XLOG记录的功能
    """
    
    def __init__(self, reader: XLogReader):
        """
        初始化过滤读取器
        
        Args:
            reader: 底层XLOG读取器
        """
        self.reader = reader
    
    def filter_by_rmid(self, rmid: int) -> Iterator[XLogRecord]:
        """
        按资源管理器ID过滤记录
        
        Args:
            rmid: 资源管理器ID
            
        Yields:
            过滤后的XLOG记录
        """
        for record in self.reader.read_records():
            if record.xl_rmid == rmid:
                yield record
    
    def filter_by_xid(self, xid: int) -> Iterator[XLogRecord]:
        """
        按事务ID过滤记录
        
        Args:
            xid: 事务ID
            
        Yields:
            过滤后的XLOG记录
        """
        for record in self.reader.read_records():
            if record.xl_xid == xid:
                yield record
    
    def filter_by_lsn_range(self, start_lsn: LSN, end_lsn: LSN) -> Iterator[XLogRecord]:
        """
        按LSN范围过滤记录
        
        Args:
            start_lsn: 起始LSN
            end_lsn: 结束LSN
            
        Yields:
            过滤后的XLOG记录
        """
        for record in self.reader.read_records(start_lsn, end_lsn):
            yield record
    
    def filter_by_info(self, info_mask: int) -> Iterator[XLogRecord]:
        """
        按信息标志过滤记录
        
        Args:
            info_mask: 信息标志掩码
            
        Yields:
            过滤后的XLOG记录
        """
        for record in self.reader.read_records():
            if record.get_info() & info_mask:
                yield record