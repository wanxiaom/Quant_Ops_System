import rqdatac
import os
import pickle


# 函数：加载或生成 etf_list
def get_etf_list(date):
    etf_list_dir = 'F:/DolphinDB/instruments/etf'
    os.makedirs(etf_list_dir, exist_ok=True)
    etf_list_file = os.path.join(etf_list_dir, f"etf_list_{date}.pkl")

    if os.path.exists(etf_list_file):
        with open(etf_list_file, 'rb') as f:
            return pickle.load(f)
    else:
        df = rqdatac.all_instruments(type='ETF', market='cn', date=date)
        etf_list = df['order_book_id'].tolist()
        with open(etf_list_file, 'wb') as f:
            pickle.dump(etf_list, f)
        return etf_list


# 函数：加载或生成 etf_dict
def get_etf_dict(date):
    etf_dict_dir = 'F:/DolphinDB/instruments/etf_dict'
    os.makedirs(etf_dict_dir, exist_ok=True)
    etf_dict_file = os.path.join(etf_dict_dir, f"etf_dict_{date}.pkl")

    if os.path.exists(etf_dict_file):
        with open(etf_dict_file, 'rb') as f:
            return pickle.load(f)
    else:
        etf_dict = {}
        etf_list = get_etf_list(date)
        for etf in etf_list:
            normal_etf = rqdatac.id_convert(etf, to='normal')
            etf_dict[etf] = normal_etf
        with open(etf_dict_file, 'wb') as f:
            pickle.dump(etf_dict, f)
        return etf_dict
