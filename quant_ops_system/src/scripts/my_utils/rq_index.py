import rqdatac
import os
import pickle


# 函数：加载或生成 index_list
def get_index_list(date):
    index_list_dir = 'F:/DolphinDB/instruments/index'
    os.makedirs(index_list_dir, exist_ok=True)
    index_list_file = os.path.join(index_list_dir, f"index_list_{date}.pkl")

    if os.path.exists(index_list_file):
        with open(index_list_file, 'rb') as f:
            return pickle.load(f)
    else:
        df = rqdatac.all_instruments(type='INDX', market='cn', date=date)
        index_list = df['order_book_id'].tolist()
        with open(index_list_file, 'wb') as f:
            pickle.dump(index_list, f)
        return index_list


# 函数：加载或生成 index_dict
def get_index_dict(date):
    index_dict_dir = 'F:/DolphinDB/instruments/index_dict'
    os.makedirs(index_dict_dir, exist_ok=True)
    index_dict_file = os.path.join(index_dict_dir, f"index_dict_{date}.pkl")

    if os.path.exists(index_dict_file):
        with open(index_dict_file, 'rb') as f:
            return pickle.load(f)
    else:
        index_dict = {}
        index_list = get_index_list(date)
        for index in index_list:
            normal_index = rqdatac.id_convert(index, to='normal')
            index_dict[index] = normal_index
        with open(index_dict_file, 'wb') as f:
            pickle.dump(index_dict, f)
        return index_dict
