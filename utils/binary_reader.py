"""
二进制数据读取工具
用于读取和解析PostgreSQL WAL文件的二进制数据
"""

import struct
from typing import Union, Tuple, Optional


class BinaryReader:
    """
    二进制数据读取器
    提供读取各种数据类型的方法
    """
    
    def __init__(self, data: bytes):
        """
        初始化二进制读取器
        
        Args:
            data: 二进制数据
        """
        self.data = data
        self.position = 0
        self.length = len(data)
    
    def read_bytes(self, count: int) -> bytes:
        """
        读取指定数量的字节
        
        Args:
            count: 要读取的字节数
            
        Returns:
            读取的字节数据
            
        Raises:
            EOFError: 如果到达文件末尾
        """
        if self.position + count > self.length:
            raise EOFError(f"尝试读取{count}字节，但只剩{self.length - self.position}字节")
        
        result = self.data[self.position:self.position + count]
        self.position += count
        return result
    
    def read_uint8(self) -> int:
        """
        读取无符号8位整数
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(1)
        return struct.unpack('<B', data)[0]
    
    def read_uint16(self) -> int:
        """
        读取无符号16位整数（小端序）
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(2)
        return struct.unpack('<H', data)[0]
    
    def read_uint32(self) -> int:
        """
        读取无符号32位整数（小端序）
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(4)
        return struct.unpack('<I', data)[0]
    
    def read_uint64(self) -> int:
        """
        读取无符号64位整数（小端序）
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(8)
        return struct.unpack('<Q', data)[0]
    
    def read_int32(self) -> int:
        """
        读取有符号32位整数（小端序）
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(4)
        return struct.unpack('<i', data)[0]
    
    def read_int64(self) -> int:
        """
        读取有符号64位整数（小端序）
        
        Returns:
            读取的整数值
        """
        data = self.read_bytes(8)
        return struct.unpack('<q', data)[0]
    
    def read_string(self, length: int, encoding: str = 'utf-8') -> str:
        """
        读取指定长度的字符串
        
        Args:
            length: 字符串长度
            encoding: 字符编码，默认为utf-8
            
        Returns:
            读取的字符串
        """
        data = self.read_bytes(length)
        # 移除末尾的空字符
        data = data.rstrip(b'\x00')
        return data.decode(encoding, errors='ignore')
    
    def read_null_terminated_string(self, encoding: str = 'utf-8') -> str:
        """
        读取以空字符结尾的字符串
        
        Args:
            encoding: 字符编码，默认为utf-8
            
        Returns:
            读取的字符串
        """
        result = b''
        while True:
            if self.position >= self.length:
                break
            byte = self.read_bytes(1)
            if byte == b'\x00':
                break
            result += byte
        
        return result.decode(encoding, errors='ignore')
    
    def peek_bytes(self, count: int) -> bytes:
        """
        查看指定数量的字节，但不移动位置指针
        
        Args:
            count: 要查看的字节数
            
        Returns:
            查看的字节数据
        """
        if self.position + count > self.length:
            return self.data[self.position:]
        return self.data[self.position:self.position + count]
    
    def skip_bytes(self, count: int) -> None:
        """
        跳过指定数量的字节
        
        Args:
            count: 要跳过的字节数
        """
        if self.position + count > self.length:
            raise EOFError(f"尝试跳过{count}字节，但只剩{self.length - self.position}字节")
        self.position += count
    
    def is_eof(self) -> bool:
        """
        检查是否到达文件末尾
        
        Returns:
            如果到达末尾返回True，否则返回False
        """
        return self.position >= self.length
    
    def remaining_bytes(self) -> int:
        """
        获取剩余字节数
        
        Returns:
            剩余字节数
        """
        return max(0, self.length - self.position)
    
    def tell(self) -> int:
        """
        获取当前位置
        
        Returns:
            当前位置
        """
        return self.position
    
    def seek(self, position: int) -> None:
        """
        设置当前位置
        
        Args:
            position: 新位置
            
        Raises:
            ValueError: 如果位置超出范围
        """
        if position < 0 or position > self.length:
            raise ValueError(f"位置{position}超出范围[0, {self.length}]")
        self.position = position