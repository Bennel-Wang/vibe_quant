#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Tushare Token 可用性检测脚本

用法:
    python scripts/check_tushare_token.py              # 检测配置中生效的 token 及各来源
    python scripts/check_tushare_token.py --token XXX  # 只检测指定 token

检测内容:
    1. token 来源: 环境变量 TUSHARE_TOKEN 与 config.yaml, 以及按
       data_sourcing/config.py 优先级(环境变量 > config.yaml)的最终生效值
    2. 格式体检: 首尾空白、引号残留、不可见字符、非十六进制、长度
    3. 真实性验证: 调用 Tushare Pro 最轻量接口 trade_cal 确认 token 可用
"""
import argparse
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def mask(token: str) -> str:
    if not token:
        return "<空>"
    if len(token) <= 12:
        return f"{token[:2]}*** (len={len(token)})"
    return f"{token[:8]}...{token[-4:]} (len={len(token)})"


def load_yaml_token() -> tuple:
    """返回 (token, 错误信息)"""
    path = REPO_ROOT / "config.yaml"
    if not path.exists():
        return "", f"config.yaml 不存在: {path}"
    try:
        import yaml
    except ImportError:
        return "", "未安装 pyyaml, 无法读取 config.yaml (pip install pyyaml)"
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        tok = (data.get("tokens") or {}).get("tushare_token", "") or ""
        return str(tok), ""
    except Exception as e:
        return "", f"解析 config.yaml 失败: {e}"


def format_check(name: str, token: str) -> bool:
    """格式体检, 返回 True 表示未发现格式问题"""
    if not token:
        print(f"  [格式] {name}: <空>")
        return False
    issues = []
    if token != token.strip():
        issues.append("首尾含空白字符")
    if any(c in token for c in "'\""):
        issues.append("含引号残留(复制时带入)")
    if any(ord(c) < 33 or ord(c) > 126 for c in token):
        issues.append("含不可见/非ASCII字符")
    if not re.fullmatch(r"[0-9a-fA-F]+", token):
        issues.append("含非十六进制字符")
    if len(token) != 40:
        issues.append(f"长度{len(token)}位(常见为40位)")
    if issues:
        print(f"  [格式] {name}: 注意 -> {'; '.join(issues)}")
        return False
    print(f"  [格式] {name}: OK")
    return True


def test_token(label: str, token: str) -> bool:
    """调用 Tushare Pro 验证 token 是否被接受, 返回 True/False"""
    print(f"\n>>> 验证 {label}: {mask(token)}")
    format_check(label, token)
    try:
        import tushare as ts
    except ImportError:
        print("  [API ] 未安装 tushare (pip install tushare), 无法联网验证")
        return False
    try:
        pro = ts.pro_api(token)
        df = pro.trade_cal(start_date="20240101", end_date="20240110")
        rows = 0 if df is None else len(df)
        print(f"  [API ] 通过! trade_cal 返回 {rows} 行, token 可用")
        return True
    except Exception as e:
        msg = str(e).strip().splitlines()[0] if str(e).strip() else repr(e)
        print(f"  [API ] 失败: {msg}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser(description="检测 Tushare token 是否可用")
    ap.add_argument("--token", help="直接检测指定 token(不读取任何配置)")
    args = ap.parse_args()

    print("=" * 60)
    print("Tushare Token 检测")
    print("=" * 60)

    if args.token:
        ok = test_token("命令行指定", args.token.strip())
        return 0 if ok else 1

    env_tok = os.environ.get("TUSHARE_TOKEN", "") or ""
    yaml_tok, yaml_err = load_yaml_token()
    # 与 data_sourcing/config.py:29-32 相同的优先级: 环境变量 > config.yaml
    effective = env_tok or yaml_tok
    effective_src = "环境变量 TUSHARE_TOKEN" if env_tok else ("config.yaml" if yaml_tok else "<无>")

    print("\n[1] token 来源")
    print(f"  环境变量 TUSHARE_TOKEN : {mask(env_tok)}")
    if yaml_err:
        print(f"  config.yaml           : {yaml_err}")
    else:
        print(f"  config.yaml           : {mask(yaml_tok)}")
    print(f"  最终生效(优先级: 环境变量 > config.yaml): {mask(effective)}  <- 来源: {effective_src}")

    print("\n[2] 真实性验证(调用 tushare pro.trade_cal)")
    results = {}
    if env_tok:
        results["环境变量"] = test_token("环境变量 TUSHARE_TOKEN", env_tok)
    if yaml_tok and yaml_tok != env_tok:
        results["config.yaml"] = test_token("config.yaml tushare_token", yaml_tok)
    if not results:
        print("  没有任何可测的 token, 请先在 config.yaml 的 tokens.tushare_token 中配置")

    print("\n" + "=" * 60)
    print("结论")
    print("=" * 60)
    ok_effective = results.get("环境变量" if env_tok else "config.yaml", False)
    if ok_effective:
        print(f"  当前生效的 token(来自 {effective_src})验证通过, 程序应能正常取数。")
    else:
        print(f"  当前生效的 token(来自 {effective_src})不可用!")
    if env_tok and yaml_tok and env_tok != yaml_tok:
        if results.get("环境变量") is False and results.get("config.yaml") is True:
            print("  -> 环境变量 TUSHARE_TOKEN 是旧/错的, 且它优先于 config.yaml, 这就是报")
            print("     '您的token不对' 的原因。修复: 删除该环境变量(Windows):")
            print("       [Environment]::SetEnvironmentVariable('TUSHARE_TOKEN', $null, 'User')")
            print("     然后重开终端再运行程序。")
        elif results.get("环境变量") is True and results.get("config.yaml") is False:
            print("  -> 环境变量可用但 config.yaml 里的不对, 建议把 config.yaml 同步为环境变量的值。")
        elif not any(results.values()):
            print("  -> 两个 token 都不可用, 请登录 https://tushare.pro 个人主页重新复制 token,")
            print("     写入 config.yaml 的 tokens.tushare_token。")
    elif not any(results.values()):
        print("  -> 请登录 https://tushare.pro 个人主页重新复制 token,")
        print("     写入 config.yaml 的 tokens.tushare_token。")
    return 0 if ok_effective else 1


if __name__ == "__main__":
    sys.exit(main())
