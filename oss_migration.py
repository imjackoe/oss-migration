#!/usr/bin/env python
# coding=utf-8
'''
Description  : OSS数据迁移工具（命令行参数版）
'''
import sys
import os
import time
import re
import threading
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import oss2
from dotenv import load_dotenv

# 加载环境变量
load_dotenv('.env')

RETRY_TIMES = 3
THROTTLE_DELAY = 2
STATS_INTERVAL = 5
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '20'))
PAGE_SIZE = int(os.getenv('PAGE_SIZE', '500'))
SKIP_EXISTS = False

# 全局状态（线程安全控制）
counter_lock = threading.Lock()
log_lock = threading.Lock()
copied = 0
skipped = 0
errors = 0

def format_path(path):
    """统一路径格式"""
    return path if path.endswith('/') else path + '/'

def parse_oss_uri(uri):
    """解析OSS URI格式：oss://bucket-name/path/prefix/"""
    pattern = r'^oss://([^/]+)/(.*)$'
    match = re.match(pattern, uri)
    if not match:
        raise ValueError(f"无效的OSS路径格式: {uri}")

    bucket_name = match.group(1)
    prefix = format_path(match.group(2) or '')
    return bucket_name, prefix

def get_total_objects(bucket, prefix):
    """预统计有效对象总数"""
    print("🔄 正在统计迁移对象总数...")
    total = 0
    marker = ''

    while True:
        result = bucket.list_objects(
            prefix=prefix,
            marker=marker,
            max_keys=PAGE_SIZE
        )

        valid_objs = [obj for obj in result.object_list if not obj.key.endswith('/')]
        total += len(valid_objs)

        if result.is_truncated:
            marker = result.next_marker
        else:
            break

    print(f"✅ 待迁移对象总数: {total}")
    return total

def process_object(obj, dest_bucket, src_bucket_name, src_prefix, dest_prefix, s_log, f_log):
    global copied, skipped, errors
    src_key = obj.key

    # 生成目标路径
    dest_key = src_key.replace(src_prefix, dest_prefix, 1)

    # 跳过条件1: 目标已存在相同路径文件
    if SKIP_EXISTS:
        if dest_bucket.object_exists(dest_key):
            with counter_lock:
                skipped += 1
            with log_lock:
                s_log.write(f"[SKIP][{time.ctime()}] {dest_key}\n")
            return

    # 跳过相同路径
    if src_key == dest_key:
        with counter_lock:
            skipped += 1
        with log_lock:
            s_log.write(f"[SKIP][{time.ctime()}] {src_key}\n")
        return

    # 带重试的拷贝操作
    for attempt in range(1, RETRY_TIMES+1):
        try:
            dest_bucket.copy_object(
                src_bucket_name,
                src_key,
                dest_key
            )
            with counter_lock:
                copied += 1
            with log_lock:
                s_log.write(f"[SUCCESS][{time.ctime()}] {src_key} -> {dest_key}\n")
            return
        except oss2.exceptions.OssError as e:
            if e.status == 429:  # 流量控制
                time.sleep(THROTTLE_DELAY * attempt)
                continue
            with counter_lock:
                errors += 1
            with log_lock:
                f_log.write(f"[ATTEMPT {attempt}/{RETRY_TIMES}][{time.ctime()}] {src_key} | {str(e)}\n")
            if attempt == RETRY_TIMES:
                f_log.write(f"[FINAL FAIL][{time.ctime()}] {src_key}\n")

def print_progress(start_time, total_objects):
    """定时输出进度信息"""
    while True:
        processed = copied + skipped + errors
        if total_objects > 0:
            progress = processed / total_objects * 100
            elapsed = time.time() - start_time
            speed = processed / elapsed if elapsed > 0 else 0
            remaining = (total_objects - processed) / speed if speed > 0 else 0

            status = (
                f"🚀 进度: {processed}/{total_objects} ({progress:.1f}%) | "
                f"速度: {speed:.1f} obj/s | "
                f"用时: {elapsed:.0f}s | "
                f"剩余: {remaining:.0f}s"
            )
        else:
            status = f"🔄 已处理: {processed} 对象"

        print(status, end='\r')
        time.sleep(STATS_INTERVAL)

def copy_objects(src_bucket, dest_bucket, src_prefix, dest_prefix, success_log, fail_log):
    global copied, skipped, errors
    copied = skipped = errors = 0
    marker = ''
    start_time = time.time()

    # 获取总数
    total_objects = get_total_objects(src_bucket, src_prefix)

    # 启动进度线程
    stats_thread = threading.Thread(
        target=print_progress,
        args=(start_time, total_objects),
        daemon=True
    )
    stats_thread.start()

    with open(success_log, 'a') as s_log, open(fail_log, 'a') as f_log:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            while True:
                result = src_bucket.list_objects(
                    prefix=src_prefix,
                    marker=marker,
                    max_keys=PAGE_SIZE
                )

                if not result.object_list:
                    break

                # 提交本页任务
                futures = [
                    executor.submit(
                        process_object,
                        obj,
                        dest_bucket,
                        src_bucket.bucket_name,
                        src_prefix,
                        dest_prefix,
                        s_log,
                        f_log
                    )
                    for obj in result.object_list
                    if not obj.key.endswith('/')
                ]

                # 等待本页完成
                for _ in as_completed(futures):
                    pass

                # 更新分页标记
                marker = result.next_marker if result.is_truncated else None
                if not marker:
                    break

    # 输出报告
    print("\n" + "="*50)
    print(f"总处理对象: {copied + skipped + errors}")
    print(f"✅ 成功复制: {copied} (成功率: {copied/(copied+errors or 1):.1%})")
    print(f"⏭️ 自动跳过: {skipped}")
    print(f"❌ 最终失败: {errors}")
    print(f"⏱️ 总耗时: {time.time()-start_time:.1f}秒")
    print(f"📜 成功日志: {success_log}")
    print(f"📄 错误日志: {fail_log}")
    print("="*50)

    return total_objects

if __name__ == '__main__':
    # 配置参数
    parser = argparse.ArgumentParser(description='OSS数据迁移工具')
    parser.add_argument('--src', required=True, help='源路径，格式：oss://bucket/path/')
    parser.add_argument('--dest', required=True, help='目标路径，格式：oss://bucket/path/')
    parser.add_argument('--page-size', type=int, default=500, help='分页大小（默认：500）')
    parser.add_argument('--workers', type=int, default=20, help='并发线程数（默认：20）')
    parser.add_argument('--skip-exists', type=bool, default=False, help='目标存在时忽略（默认：False）')
    args = parser.parse_args()

    # 解析配置
    if args.page_size:
        PAGE_SIZE = int(args.page_size)
    if args.workers:
        MAX_WORKERS = int(args.workers)
    if (args.skip_exists):
        SKIP_EXISTS = True

    # 解析OSS路径
    try:
        src_bucket_name, src_prefix = parse_oss_uri(args.src)
        dest_bucket_name, dest_prefix = parse_oss_uri(args.dest)
    except ValueError as e:
        print(f"错误: {str(e)}")
        sys.exit(1)

    # 初始化OSS客户端
    auth = oss2.Auth(
        os.getenv('OSS_ACCESS_KEY_ID'),
        os.getenv('OSS_ACCESS_KEY_SECRET')
    )
    endpoint = os.getenv('OSS_ENDPOINT', 'https://oss-cn-hangzhou.aliyuncs.com')

    src_bucket = oss2.Bucket(auth, endpoint, src_bucket_name)
    dest_bucket = oss2.Bucket(auth, endpoint, dest_bucket_name)

    # 生成日志文件名
    log_prefix = f"{src_bucket_name}#{src_prefix.replace('/', '_')}to_{dest_bucket_name}#{dest_prefix.replace('/', '_')}"
    success_log = f"logs/{log_prefix}success.log"
    fail_log = f"logs/{log_prefix}fail.log"

    # 打印配置
    print("\n🔧 迁移配置:")
    print(f"源路径: oss://{src_bucket_name}/{src_prefix}")
    print(f"目标路径: oss://{dest_bucket_name}/{dest_prefix}")
    print(f"分页大小: {PAGE_SIZE}")
    print(f"并发线程: {MAX_WORKERS}")
    print(f"日志文件: {success_log} 和 {fail_log}\n")

    total_objects = 0

    # 执行迁移
    try:
        total_objects = copy_objects(
            src_bucket=src_bucket,
            dest_bucket=dest_bucket,
            src_prefix=src_prefix,
            dest_prefix=dest_prefix,
            success_log=success_log,
            fail_log=fail_log
        )
    except KeyboardInterrupt:
        print("\n⚠️ 操作已中断！正在保存状态...")
        processed = copied + skipped + errors
        if total_objects > 0:
            print(f"当前进度: {processed}/{total_objects} ({processed/total_objects*100:.1f}%)")
    finally:
        print("迁移进程结束")
