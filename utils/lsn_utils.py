"""
LSN（Log Sequence Number）工具函数
用于处理PostgreSQL的LSN值
"""

from typing import Tuple, Union


class LSN:
    """
    PostgreSQL LSN（Log Sequence Number）处理类
    LSN是64位值，高32位是日志文件号，低32位是文件内偏移量
    """
    
    def __init__(self, value: Union[int, str]):
        """
        初始化LSN
        
        Args:
            value: LSN值，可以是整数或字符串格式（如"0/16B37B0"）
        """
        if isinstance(value, str):
            self.value = self._parse_string(value)
        else:
            self.value = value
    
    def _parse_string(self, lsn_str: str) -> int:
        """
        解析字符串格式的LSN
        
        Args:
            lsn_str: 字符串格式的LSN，如"0/16B37B0"
            
        Returns:
            64位整数格式的LSN
        """
        if '/' not in lsn_str:
            raise ValueError(f"无效的LSN格式: {lsn_str}")
        
        high_str, low_str = lsn_str.split('/')
        high = int(high_str, 16) if high_str else 0
        low = int(low_str, 16)
        
        return (high << 32) | low
    
    @property
    def file_id(self) -> int:
        """
        获取日志文件ID（高32位）
        
        Returns:
            日志文件ID
        """
        return self.value >> 32
    
    @property
    def file_offset(self) -> int:
        """
        获取文件内偏移量（低32位）
        
        Returns:
            文件内偏移量
        """
        return self.value & 0xFFFFFFFF
    
    def to_string(self) -> str:
        """
        转换为字符串格式
        
        Returns:
            字符串格式的LSN，如"0/16B37B0"
        """
        return f"{self.file_id:X}/{self.file_offset:X}"
    
    def __str__(self) -> str:
        return self.to_string()
    
    def __repr__(self) -> str:
        return f"LSN('{self.to_string()}')"
    
    def __eq__(self, other) -> bool:
        if isinstance(other, LSN):
            return self.value == other.value
        return False
    
    def __lt__(self, other) -> bool:
        if isinstance(other, LSN):
            return self.value < other.value
        return NotImplemented
    
    def __le__(self, other) -> bool:
        return self == other or self < other
    
    def __gt__(self, other) -> bool:
        if isinstance(other, LSN):
            return self.value > other.value
        return NotImplemented
    
    def __ge__(self, other) -> bool:
        return self == other or self > other
    
    def __hash__(self) -> int:
        return hash(self.value)
    
    def __int__(self) -> int:
        return self.value
    
    def distance(self, other: 'LSN') -> int:
        """
        计算两个LSN之间的距离
        
        Args:
            other: 另一个LSN
            
        Returns:
            两个LSN之间的字节数
        """
        if self.file_id != other.file_id:
            raise ValueError("只能计算同一文件内的LSN距离")
        
        return abs(self.file_offset - other.file_offset)
    
    def next_segment(self, segment_size: int = 16 * 1024 * 1024) -> 'LSN':
        """
        获取下一个段的LSN
        
        Args:
            segment_size: 段大小，默认16MB
            
        Returns:
            下一个段的LSN
        """
        next_offset = (self.file_offset // segment_size + 1) * segment_size
        return LSN((self.file_id << 32) | next_offset)
    
    def is_segment_boundary(self, segment_size: int = 16 * 1024 * 1024) -> bool:
        """
        检查是否为段边界
        
        Args:
            segment_size: 段大小，默认16MB
            
        Returns:
            如果是段边界返回True
        """
        return self.file_offset % segment_size == 0


def parse_lsn_range(range_str: str) -> Tuple[LSN, LSN]:
    """
    解析LSN范围字符串
    
    Args:
        range_str: LSN范围字符串，如"0/16B37B0-0/16B38B0"
        
    Returns:
        起始和结束LSN的元组
    """
    if '-' not in range_str:
        raise ValueError(f"无效的LSN范围格式: {range_str}")
    
    start_str, end_str = range_str.split('-')
    return LSN(start_str.strip()), LSN(end_str.strip())


def format_lsn_range(start: LSN, end: LSN) -> str:
    """
    格式化LSN范围
    
    Args:
        start: 起始LSN
        end: 结束LSN
        
    Returns:
        格式化的LSN范围字符串
    """
    return f"{start}-{end}"