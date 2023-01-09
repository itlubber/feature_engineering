import os
import numpy as np
import pandas as pd

from utils import *
from feature_calculator.impala_query import *


def deal_xxx():
    # 数据读取
    data = query_xxx.query()

    # 指标加工
    features = data.groupby("zjhm")["xxx"].agg(["len"]).reset_index()

    # 返回加工的指标，需至少包含两列：["用户唯一ID", "特征1", ["特征2", "特征3", ......]]
    return features
