"""
WAL文件解析核心模块
负责解析PostgreSQL WAL文件的格式和结构
"""

import struct
from typing import Dict, Any, Optional, List
from utils.binary_reader import BinaryReader
from utils.lsn_utils import LSN


class XLogRecord:
    """
    XLOG记录结构体
    对应PostgreSQL源码中的XLogRecord结构
    """
    
    # 常量定义
    SIZEOF_XLOG_RECORD = 24  # offsetof(XLogRecord, xl_crc) + sizeof(pg_crc32c)
    
    # XLOG记录标志位
    XLR_INFO_MASK = 0x0F
    XLR_RMGR_INFO_MASK = 0xF0
    XLR_SPECIAL_REL_UPDATE = 0x01
    XLR_CHECK_CONSISTENCY = 0x02
    
    def __init__(self, reader: BinaryReader):
        """
        从二进制数据中解析XLOG记录
        
        Args:
            reader: 二进制数据读取器
        """
        self.xl_tot_len = reader.read_uint32()      # total len of entire record
        self.xl_xid = reader.read_uint32()          # xact id
        xl_prev_value = reader.read_uint64()        # ptr to previous record in log
        self.xl_prev = LSN(xl_prev_value)
        self.xl_info = reader.read_uint8()          # flag bits
        self.xl_rmid = reader.read_uint8()          # resource manager for this record
        reader.skip_bytes(2)                        # 2 bytes of padding
        self.xl_crc = reader.read_uint32()          # CRC for this record
        
        # 解析记录数据
        self.blocks = []  # 块引用列表
        self.main_data = b''  # 主数据
        
        # 解析块引用和主数据
        self._parse_record_data(reader)
    
    def _parse_record_data(self, reader: BinaryReader):
        """
        解析记录的块引用和主数据部分
        """
        # 计算记录数据的起始位置和长度
        data_start = reader.tell()
        data_end = data_start + self.xl_tot_len - self.SIZEOF_XLOG_RECORD
        
        # 解析块引用
        while reader.tell() < data_end:
            # 查看下一个字节的ID
            next_byte = reader.peek_bytes(1)
            if not next_byte:
                break
                
            block_id = next_byte[0]
            
            if block_id == 255:  # XLR_BLOCK_ID_DATA_SHORT
                self._parse_data_short(reader)
                break
            elif block_id == 254:  # XLR_BLOCK_ID_DATA_LONG
                self._parse_data_long(reader)
                break
            elif block_id in [253, 252]:  # XLR_BLOCK_ID_ORIGIN, XLR_BLOCK_ID_TOPLEVEL_XID
                # 跳过这些特殊块
                reader.read_bytes(1)
                if block_id == 253:  # XLR_BLOCK_ID_ORIGIN
                    reader.skip_bytes(2)  # RepOriginId
                else:  # XLR_BLOCK_ID_TOPLEVEL_XID
                    reader.skip_bytes(4)  # TransactionId
            else:
                # 普通块引用
                self._parse_block_reference(reader)
    
    def _parse_block_reference(self, reader: BinaryReader):
        """
        解析块引用
        """
        start_pos = reader.tell()
        
        # 读取块头
        block_id = reader.read_uint8()
        fork_flags = reader.read_uint8()
        data_length = reader.read_uint16()
        
        block_info = {
            'id': block_id,
            'fork_flags': fork_flags,
            'data_length': data_length,
            'has_image': bool(fork_flags & 0x10),
            'has_data': bool(fork_flags & 0x20),
            'will_init': bool(fork_flags & 0x40),
            'same_rel': bool(fork_flags & 0x80),
            'fork_num': fork_flags & 0x0F
        }
        
        # 如果有页面镜像
        if block_info['has_image']:
            self._parse_block_image(reader, block_info)
        
        # 如果不是相同关系，读取关系文件节点
        if not block_info['same_rel']:
            block_info['relfilenode'] = {
                'spcNode': reader.read_uint32(),
                'dbNode': reader.read_uint32(),
                'relNode': reader.read_uint32()
            }
        
        # 读取块号
        block_info['block_num'] = reader.read_uint32()
        
        # 如果有数据，读取数据
        if block_info['has_data']:
            block_info['data'] = reader.read_bytes(data_length)
        
        self.blocks.append(block_info)
    
    def _parse_block_image(self, reader: BinaryReader, block_info: Dict[str, Any]):
        """
        解析块镜像
        """
        length = reader.read_uint16()
        hole_offset = reader.read_uint16()
        bimg_info = reader.read_uint8()
        
        block_info['image'] = {
            'length': length,
            'hole_offset': hole_offset,
            'bimg_info': bimg_info,
            'has_hole': bool(bimg_info & 0x01),
            'should_apply': bool(bimg_info & 0x02)
        }
        
        # 如果有压缩信息
        if (bimg_info & 0x01) and (bimg_info & 0x1C):  # has_hole and compressed
            hole_length = reader.read_uint16()
            block_info['image']['hole_length'] = hole_length
        
        # 读取镜像数据
        block_info['image']['data'] = reader.read_bytes(length)
    
    def _parse_data_short(self, reader: BinaryReader):
        """
        解析短格式主数据
        """
        reader.read_bytes(1)  # id (255)
        data_length = reader.read_uint8()
        self.main_data = reader.read_bytes(data_length)
    
    def _parse_data_long(self, reader: BinaryReader):
        """
        解析长格式主数据
        """
        reader.read_bytes(1)  # id (254)
        data_length = reader.read_uint32()
        self.main_data = reader.read_bytes(data_length)
    
    def get_rmgr_info(self) -> int:
        """
        获取资源管理器信息
        
        Returns:
            资源管理器信息
        """
        return self.xl_info & self.XLR_RMGR_INFO_MASK
    
    def get_info(self) -> int:
        """
        获取记录信息
        
        Returns:
            记录信息
        """
        return self.xl_info & self.XLR_INFO_MASK
    
    def is_special_rel_update(self) -> bool:
        """
        检查是否为特殊关系更新
        
        Returns:
            如果是特殊关系更新返回True
        """
        return bool(self.xl_info & self.XLR_SPECIAL_REL_UPDATE)
    
    def is_consistency_check(self) -> bool:
        """
        检查是否需要一致性检查
        
        Returns:
            如果需要一致性检查返回True
        """
        return bool(self.xl_info & self.XLR_CHECK_CONSISTENCY)


class WALPageHeader:
    """
    WAL页头结构
    """
    
    SIZEOF_WAL_PAGE_HEADER = 24
    
    def __init__(self, reader: BinaryReader):
        """
        从二进制数据中解析WAL页头
        
        Args:
            reader: 二进制数据读取器
        """
        self.magic = reader.read_uint16()        # 魔数
        self.info = reader.read_uint16()         # 标志位
        self.tli = reader.read_uint32()          # 时间线ID
        self.prev_page_lsn = LSN(reader.read_uint64())  # 上一页LSN
        self.page_lsn = LSN(reader.read_uint64())        # 本页LSN
    
    def is_new_page(self) -> bool:
        """检查是否为新页"""
        return bool(self.info & 0x0001)
    
    def is_contained_record(self) -> bool:
        """检查是否包含跨页记录"""
        return bool(self.info & 0x0002)


class WALFile:
    """
    WAL文件解析器
    """
    
    # WAL文件常量
    WAL_SEGMENT_SIZE = 16 * 1024 * 1024  # 16MB
    WAL_BLOCK_SIZE = 8192                # 8KB
    XLOG_PAGE_MAGIC = 0xD099             # WAL页魔数
    
    def __init__(self, file_path: str):
        """
        初始化WAL文件解析器
        
        Args:
            file_path: WAL文件路径
        """
        self.file_path = file_path
        self.file_data = None
        self.records = []
        
    def parse(self):
        """
        解析WAL文件
        """
        with open(self.file_path, 'rb') as f:
            self.file_data = f.read()
        
        reader = BinaryReader(self.file_data)
        
        # 解析WAL文件头
        self._parse_wal_file_header(reader)
        
        # 解析WAL页
        self._parse_wal_pages(reader)
    
    def _parse_wal_file_header(self, reader: BinaryReader):
        """
        解析WAL文件头
        """
        # WAL文件开头的长头信息
        self.system_identifier = reader.read_uint64()
        self.segment_size = reader.read_uint32()
        self.xlog_blcksz = reader.read_uint32()
        self.xlog_seg_size = reader.read_uint32()
        
        # 跳过到第一个WAL页
        reader.seek(self.xlog_blcksz)
    
    def _parse_wal_pages(self, reader: BinaryReader):
        """
        解析WAL页
        """
        while not reader.is_eof():
            # 检查是否到达页边界
            pos = reader.tell()
            if pos % self.WAL_BLOCK_SIZE != 0:
                # 对齐到页边界
                next_page = ((pos // self.WAL_BLOCK_SIZE) + 1) * self.WAL_BLOCK_SIZE
                if next_page >= len(self.file_data):
                    break
                reader.seek(next_page)
                continue
            
            # 读取页头
            if reader.remaining_bytes() < WALPageHeader.SIZEOF_WAL_PAGE_HEADER:
                break
                
            page_header = WALPageHeader(reader)
            
            # 检查魔数
            if page_header.magic != self.XLOG_PAGE_MAGIC:
                continue
            
            # 解析页中的记录
            self._parse_page_records(reader, page_header)
    
    def _parse_page_records(self, reader: BinaryReader, page_header: WALPageHeader):
        """
        解析页中的记录
        """
        page_start = reader.tell() - WALPageHeader.SIZEOF_WAL_PAGE_HEADER
        page_end = page_start + self.WAL_BLOCK_SIZE
        
        while reader.tell() < page_end:
            # 检查是否有足够的数据读取记录头
            if reader.remaining_bytes() < XLogRecord.SIZEOF_XLOG_RECORD:
                break
            
            # 记录开始位置
            record_start = reader.tell()
            
            try:
                # 解析XLOG记录
                record = XLogRecord(reader)
                self.records.append(record)
                
                # 移动到下一个记录
                next_record = record_start + record.xl_tot_len
                if next_record >= page_end:
                    break
                    
                reader.seek(next_record)
                
            except (EOFError, struct.error):
                # 记录损坏或到达文件末尾
                break
    
    def get_records_by_rmid(self, rmid: int) -> List[XLogRecord]:
        """
        根据资源管理器ID获取记录
        
        Args:
            rmid: 资源管理器ID
            
        Returns:
            匹配的记录列表
        """
        return [record for record in self.records if record.xl_rmid == rmid]
    
    def get_records_by_xid(self, xid: int) -> List[XLogRecord]:
        """
        根据事务ID获取记录
        
        Args:
            xid: 事务ID
            
        Returns:
            匹配的记录列表
        """
        return [record for record in self.records if record.xl_xid == xid]


# 资源管理器ID常量（从PostgreSQL源码中提取）
RMGR_IDS = {
    0: 'XLOG',
    1: 'Transaction',
    2: 'Storage',
    3: 'CLOG',
    4: 'Database',
    5: 'Tablespace',
    6: 'MultiXact',
    7: 'RelMap',
    8: 'Standby',
    9: 'Heap2',
    10: 'Heap',
    11: 'Btree',
    12: 'Hash',
    13: 'Gin',
    14: 'Gist',
    15: 'Sequence',
    16: 'SPGist',
    17: 'BRIN',
    18: 'Generic',
    19: 'Logical',
    20: 'Dist',
    21: 'CommitTs',
    22: 'ReplicationOrigin',
    23: 'ReplicationSlot',
    24: 'Heap3'
}


def get_rmgr_name(rmid: int) -> str:
    """
    获取资源管理器名称
    
    Args:
        rmid: 资源管理器ID
        
    Returns:
        资源管理器名称
    """
    return RMGR_IDS.get(rmid, f'Unknown({rmid})')