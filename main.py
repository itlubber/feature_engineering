import argparse
from utils import logger, seed_everything
from feature_calculator.sample import *
from feature_calculator.impala_query import query_all_user_data


# 配置相关参数或环境
# 命令行传参
# parser = argparse.ArgumentParser(description="特征工程相关参数配置")
# parser.add_argument('--save', action='store_true', default=False, help="布尔型参数使用")
# parser.add_argument('--seed', type=int, default=3407, help="随机种子, 保证结果可复现时设置")
# parser.add_argument('--outputs', type=str, default="outputs/all_user_data.pkl", help="最终结果保存的路径")
# args = parser.parse_args()


# 脚本内部传参
class args:
    save = False
    seed = 3407
    outputs = "outputs/all_user_data.pkl"


seed_everything(args.seed)


# 获取建模的所有用户ID、时间、好坏标签、对应征信报告(可选)
all_user_data = query_all_user_data.query()


# 计算指标
feature_xxx = deal_xxx()


# 合并指标
all_user_data = all_user_data.merge(feature_xxx, how="left", on="zjhm")


# 保存指标
save_pickle(all_user_data, args.outputs)
