from datetime import datetime
import pytz

BEIJING_TZ = pytz.timezone('Asia/Shanghai')


def beijing_now() -> datetime:
    return datetime.now(BEIJING_TZ)
