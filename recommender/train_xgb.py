import xgboost as xgb
import pandas as pd
import numpy as np
from xgboost.sklearn import XGBClassifier
from sklearn import metrics
from sklearn.model_selection import GridSearchCV, cross_val_score
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from db import MyMongoDb
import time, datetime

DAO = MyMongoDb("dzdp")
no_need_columns = {
    "review": ["_id", "id", "comment", "heart-num", "got", "recommend", "score"],
    "shop": ["_id", "item2vec", "coordinate"],
    "member": ["_id", "item2vec", "tags"]}
score_columns = ["flavor", "env", "service"]
coordinate_columns = ["latitude", "longitude"]


def split_list(lst, index):
    if not lst:
        return {}

    res = {}
    for i in range(len(index)):
        res[index[i]] = lst[i]
    return res


def update_dict(s, u):
    s.update(u)
    return s


def get_raw_data():
    review_data = pd.DataFrame([update_dict(s, split_list(s.get("score", None), score_columns))
                                for s in DAO.get_all("review")])
    shop_data = pd.DataFrame([update_dict(s, split_list(s.get("coordinate", None), coordinate_columns))
                              for s in DAO.get_all("shop")])
    member_data = pd.DataFrame(list(DAO.get_all("member")))

    return review_data, shop_data, member_data


def pre_process(review_data, shop_data, member_data):
    # 1. delete no needed columns
    review_data.drop(columns=no_need_columns["review"], inplace=True)
    shop_data.drop(columns=no_need_columns["shop"], inplace=True)
    member_data.drop(columns=no_need_columns["member"], inplace=True)

    # 2. change some column names
    shop_data.rename(columns={"id": "shop-id", "name": "shop-name"}, inplace=True)
    member_data.rename(columns={"id": "member-id"}, inplace=True)

    # 3. merge to one DataFrame
    data = pd.merge(review_data, shop_data, on="shop-id")
    data = pd.merge(data, member_data, on="member-id")

    # 4. change some columns format
    data["contri-value"] = data["contri-value"].astype(int)
    data["is-vip"] = data["is-vip"].astype(bool)

    def format_zero_float(x):
        if x:
            return x
        else:
            return np.nan
    now = datetime.datetime.now()
    data["register-date"] = pd.to_datetime(data["register-date"], format="%Y-%m-%d")
    data["create-time"] = pd.to_datetime(data["create-time"], format="%Y-%m-%d")
    data["update-time"] = pd.to_datetime(data["update-time"], format="%Y-%m-%d")
    data[score_columns] = data[score_columns].applymap(format_zero_float)
    data = data.assign(has_branch=data["shop-name"] != data["full-name"],
                       member_score=data[["star"] + score_columns].mean(axis=1),
                       register_days=np.int16((now-data["register-date"]) / np.timedelta64(1, 'D') + 1))
    data.drop(columns=["full-name", "register-date"], inplace=True)
    data["label"] = np.where(data["member_score"]>=3, 1, 0)

    # 5. add averages
    avg = data.groupby('shop-id')[['pay', 'star']+score_columns].mean()
    data.index = data['shop-id'].tolist()
    data[['avg_pay', 'avg_star', 'avg_flavor', 'avg_env', 'avg_service']] = avg
    data.index = range(data.shape[0])
    data.drop(columns=['pay', 'star']+score_columns, inplace=True)

    # 6. transform string feature to one-hot vectors
    data["category"] = LabelEncoder().fit_transform(data["category"])
    data["district"] = LabelEncoder().fit_transform(data["district"])
    one_hot = pd.DataFrame(OneHotEncoder().fit_transform(data[["gender", "category", "district"]]).toarray(),
                           columns=["gender_%d" % i for i in range(3)] +
                                   ["category_%d" % i for i in range(37)] +
                                   ["district_%d" % i for i in range(25)],
                           dtype=np.int8)
    data = pd.concat([data, one_hot], axis=1)
    data.drop(columns=["gender", "category", "district"], inplace=True)
    return data


def data_reader():
    return pre_process(*get_raw_data())


def fit_model(alg, dtrain, predictors, target, use_train_cv=True, cv_folds=5, early_stop_round=50):
    if use_train_cv:
        xgb_param = alg.get_xgb_params()
        xg_train = xgb.DMatrix(dtrain[predictors].values, label=dtrain[target].values)
        cv_result = xgb.cv(xgb_param, xg_train, num_boost_round=xgb_param['n_estimators'], nfold=cv_folds,
                           early_stopping_rounds=early_stop_round, metrics="auc", verbose_eval=2)
        alg.set_params(n_estimators=cv_result.shape[0])

    alg.fit(dtrain[predictors], dtrain[target], verbose=2)
    prediction = alg.predict(dtrain[predictors])
    print prediction


def train(data):
    target = "label"
    predictors = [x for x in data.columns if x not in
                  [target, "shop-id", "member-id", "shop-name", "name", "member_score"]]

    xgb1 = XGBClassifier(max_depth=5,
                         n_estimators=1000,
                         subsample=0.8,
                         colsample_bytree=0.8,
                         random_state=27)
    fit_model(xgb1, data, predictors, target, False)


if __name__ == "__main__":
    start_time = time.time()
    data = data_reader()
    print "Read and pre-processed data : %0.2f seconds." % (time.time() - start_time)
    train(data)
