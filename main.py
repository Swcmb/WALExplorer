#!/usr/bin/env python3
"""
WALExplorer主程序
PostgreSQL WAL文件解析工具的命令行接口
"""

import argparse
import sys
import os
from pathlib import Path

# 添加项目路径到Python路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.wal_parser import WALFile, get_rmgr_name
from output.sql_formatter import SQLFormatter
from utils.wal_text_parser import WALTextParser


def create_parser() -> argparse.ArgumentParser:
    """
    创建命令行参数解析器
    
    Returns:
        配置好的参数解析器
    """
    parser = argparse.ArgumentParser(
        description='WALExplorer - PostgreSQL WAL文件解析工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例用法:
  %(prog)s /path/to/wal/file                    # 解析WAL文件并输出SQL
  %(prog)s /path/to/wal/file -o output.sql      # 输出到指定文件
  %(prog)s /path/to/wal/file --rmgr 10          # 只解析Heap记录
  %(prog)s /path/to/wal/file --xid 1234         # 只解析指定事务的记录
        """
    )
    
    parser.add_argument(
        'wal_file',
        help='要解析的WAL文件路径'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='输出文件路径（默认输出到标准输出）'
    )
    
    parser.add_argument(
        '--rmgr',
        type=int,
        choices=range(25),
        help='只解析指定资源管理器ID的记录 (0-24)'
    )
    
    parser.add_argument(
        '--xid',
        type=int,
        help='只解析指定事务ID的记录'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='显示详细信息'
    )
    
    parser.add_argument(
        '--version',
        action='version',
        version='WALExplorer 1.0.0'
    )
    
    return parser


def validate_wal_file(file_path: str) -> bool:
    """
    验证WAL文件是否有效
    
    Args:
        file_path: WAL文件路径
        
    Returns:
        如果文件有效返回True
    """
    if not os.path.exists(file_path):
        print(f"错误: 文件不存在: {file_path}", file=sys.stderr)
        return False
    
    if not os.path.isfile(file_path):
        print(f"错误: 路径不是文件: {file_path}", file=sys.stderr)
        return False
    
    if os.path.getsize(file_path) == 0:
        print(f"错误: 文件为空: {file_path}", file=sys.stderr)
        return False
    
    return True


def print_statistics(wal_file: WALFile, verbose: bool = False):
    """
    打印解析统计信息
    
    Args:
        wal_file: 解析后的WAL文件对象
        verbose: 是否显示详细信息
    """
    total_records = len(wal_file.records)
    
    print(f"WAL文件解析统计:", file=sys.stderr)
    print(f"  文件路径: {wal_file.file_path}", file=sys.stderr)
    print(f"  总记录数: {total_records}", file=sys.stderr)
    
    if verbose and total_records > 0:
        # 按资源管理器统计
        rmgr_stats = {}
        for record in wal_file.records:
            rmgr_name = get_rmgr_name(record.xl_rmid)
            rmgr_stats[rmgr_name] = rmgr_stats.get(rmgr_name, 0) + 1
        
        print("  资源管理器统计:", file=sys.stderr)
        for rmgr_name, count in sorted(rmgr_stats.items()):
            print(f"    {rmgr_name}: {count}", file=sys.stderr)
        
        # 显示LSN范围
        if wal_file.records:
            first_lsn = LSN(wal_file.records[0].xl_prev.value)
            last_record = wal_file.records[-1]
            last_lsn = LSN(last_record.xl_prev.value + last_record.xl_tot_len)
            print(f"  LSN范围: {first_lsn} - {last_lsn}", file=sys.stderr)


def print_text_statistics(records, text_parser: WALTextParser):
    """
    打印文本格式WAL文件的统计信息
    
    Args:
        records: 解析后的记录列表
        text_parser: 文本解析器
    """
    stats = text_parser.get_statistics(records)
    
    print(f"WAL文本文件解析统计:", file=sys.stderr)
    print(f"  总记录数: {stats.get('total_records', 0)}", file=sys.stderr)
    
    if stats.get('rmgr_statistics'):
        print("  资源管理器统计:", file=sys.stderr)
        for rmgr_name, count in sorted(stats['rmgr_statistics'].items()):
            print(f"    {rmgr_name}: {count}", file=sys.stderr)
    
    if stats.get('transaction_statistics'):
        print(f"  事务数量: {len(stats['transaction_statistics'])}", file=sys.stderr)
    
    if stats.get('lsn_range'):
        lsn_range = stats['lsn_range']
        print(f"  LSN范围: {lsn_range['start']} - {lsn_range['end']}", file=sys.stderr)


def filter_records(wal_file: WALFile, rmgr_id: int = None, xid: int = None):
    """
    根据条件过滤记录
    
    Args:
        wal_file: WAL文件对象
        rmgr_id: 资源管理器ID过滤器
        xid: 事务ID过滤器
        
    Returns:
        过滤后的记录列表
    """
    records = wal_file.records
    
    if rmgr_id is not None:
        records = [r for r in records if r.xl_rmid == rmgr_id]
    
    if xid is not None:
        records = [r for r in records if r.xl_xid == xid]
    
    return records


def main():
    """
    主函数
    """
    parser = create_parser()
    args = parser.parse_args()
    
    # 验证WAL文件
    if not validate_wal_file(args.wal_file):
        sys.exit(1)
    
    try:
        # 判断文件类型
        is_text_file = args.wal_file.lower().endswith('.txt')
        
        if args.verbose:
            file_type = "文本格式" if is_text_file else "二进制格式"
            print(f"正在解析{file_type}WAL文件: {args.wal_file}", file=sys.stderr)
        
        if is_text_file:
            # 解析文本格式WAL文件
            text_parser = WALTextParser()
            text_records = text_parser.parse_text_file(args.wal_file)
            
            # 过滤记录
            if args.rmgr is not None:
                text_records = text_parser.filter_by_rmgr_id(text_records, args.rmgr)
            
            if args.xid is not None:
                text_records = text_parser.filter_by_tx_id(text_records, args.xid)
            
            if args.verbose:
                print_text_statistics(text_records, text_parser)
            
            if not text_records:
                print("警告: 没有找到匹配的记录", file=sys.stderr)
                return
            
            # 生成SQL语句
            formatter = SQLFormatter()
            sql_statements = formatter.format_text_records(text_records)
            
        else:
            # 解析二进制格式WAL文件
            wal_file = WALFile(args.wal_file)
            wal_file.parse()
            
            # 过滤记录
            records = filter_records(wal_file, args.rmgr, args.xid)
            
            if args.verbose:
                print_statistics(wal_file, args.verbose)
            
            if not records:
                print("警告: 没有找到匹配的记录", file=sys.stderr)
                return
            
            # 生成SQL语句
            formatter = SQLFormatter()
            sql_statements = formatter.format_records(records)
        
        # 输出结果
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(sql_statements)
            if args.verbose:
                print(f"SQL语句已写入文件: {args.output}", file=sys.stderr)
        else:
            print(sql_statements)
    
    except Exception as e:
        print(f"错误: 解析WAL文件时发生异常: {e}", file=sys.stderr)
        if args.verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()