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
