from utils import logger, HiveQuery


query_all_user_data = HiveQuery(
    """
    select zjhm, sqrq, target from work_zxysbl.xxx
    """,
    result="df",
)


query_xxx = HiveQuery(
    """
    select zjhm, xxx from work_zxysbl.xxx
    """,
    result="df",
)
