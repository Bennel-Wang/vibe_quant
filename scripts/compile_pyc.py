"""
编译Python文件为.pyc字节码
保留原始.py文件，同时生成.pyc文件以提升加载效率
"""

import compileall
import os
import sys


def compile_project(root_dir=None, force=False):
    """
    编译项目中的所有Python文件为.pyc

    Args:
        root_dir: 项目根目录，默认为当前目录
        force: 是否强制重新编译
    """
    if root_dir is None:
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    directories = [
        os.path.join(root_dir, 'quant_system'),
        os.path.join(root_dir, 'easyquotation'),
        os.path.join(root_dir, 'scripts'),
    ]

    # 也编译根目录下的 .py 文件
    root_py_files = [
        os.path.join(root_dir, 'main.py'),
        os.path.join(root_dir, 'run_scheduler.py'),
    ]

    print("=" * 50)
    print("开始编译Python文件为.pyc字节码...")
    print(f"Python版本: {sys.version}")
    print("=" * 50)

    success_count = 0
    fail_count = 0

    # 编译目录
    for d in directories:
        if os.path.exists(d):
            print(f"\n编译目录: {d}")
            result = compileall.compile_dir(
                d,
                force=force,
                quiet=0,
                optimize=2,
            )
            if result:
                success_count += 1
            else:
                fail_count += 1

    # 编译单独文件
    for f in root_py_files:
        if os.path.exists(f):
            print(f"编译文件: {f}")
            result = compileall.compile_file(f, force=force, quiet=0, optimize=2)
            if result:
                success_count += 1
            else:
                fail_count += 1

    print("\n" + "=" * 50)
    print(f"编译完成! 成功: {success_count}, 失败: {fail_count}")
    print(f".pyc文件保存在各目录的 __pycache__/ 文件夹中")
    print("=" * 50)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='编译Python文件为.pyc字节码')
    parser.add_argument('--force', action='store_true', help='强制重新编译所有文件')
    parser.add_argument('--dir', type=str, default=None, help='项目根目录')
    args = parser.parse_args()

    compile_project(root_dir=args.dir, force=args.force)
