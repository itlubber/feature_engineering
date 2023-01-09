from utils import logger
from feature_calculator.sample import *
from feature_calculator.impala_query import query_all_user_data


# 获取建模的所有用户ID、时间、好坏标签、对应征信报告(可选)
all_user_data = query_all_user_data.query()

# 计算指标
feature_xxx = deal_xxx()

# 合并指标
all_user_data = all_user_data.merge(feature_xxx, how="left", on="zjhm")

# 保存指标
save_pickle(all_user_data, "outputs/all_user_data.pkl")
