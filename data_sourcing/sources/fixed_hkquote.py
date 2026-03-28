"""
修复easyquotation港股指数bug
原因: HKQuote.format_response_data 正则只匹配数字代码(\d+), 不支持HSI等字母代码
修复: 将 \d+ 改为 .+ 以支持字母和数字代码
参考: https://blog.csdn.net/zhangkexin_z/article/details/140048461
"""
import re
from easyquotation.hkquote import HKQuote


class FixedHKQuote(HKQuote):
    """修复港股指数代码匹配的HKQuote子类"""

    def format_response_data(self, rep_data, **kwargs):
        stocks_detail = "".join(rep_data)

        stock_dict = {}
        # 原正则: r'v_r_hk\d+=".*?"' 只匹配数字代码
        # 修复为: r'v_r_hk.+=".*?"' 支持字母代码(如HSI)
        for raw_quotation in re.findall(r'v_r_hk.+?=".*?"', stocks_detail):
            try:
                quotation = re.search('"(.*?)"', raw_quotation).group(1).split("~")
                if len(quotation) < 35:
                    continue
                stock_dict[quotation[2]] = dict(
                    stock_code=quotation[2],
                    lotSize=quotation[0],
                    name=quotation[1],
                    price=float(quotation[3]) if quotation[3] else 0,
                    lastPrice=float(quotation[4]) if quotation[4] else 0,
                    openPrice=float(quotation[5]) if quotation[5] else 0,
                    amount=float(quotation[6]) if quotation[6] else 0,
                    time=quotation[30] if len(quotation) > 30 else "",
                    dtd=float(quotation[32]) if len(quotation) > 32 and quotation[32] else 0,
                    high=float(quotation[33]) if len(quotation) > 33 and quotation[33] else 0,
                    low=float(quotation[34]) if len(quotation) > 34 and quotation[34] else 0,
                )
            except (ValueError, IndexError, AttributeError) as e:
                continue
        return stock_dict
