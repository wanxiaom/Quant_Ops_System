import rqdatac
import os
import pickle


# 函数：加载或生成 code_list
def get_code_list(date):
    code_list_dir = 'F:/DolphinDB/instruments/code'
    os.makedirs(code_list_dir, exist_ok=True)
    code_list_file = os.path.join(code_list_dir, f"code_list_{date}.pkl")

    if os.path.exists(code_list_file):
        with open(code_list_file, 'rb') as f:
            return pickle.load(f)
    else:
        df = rqdatac.all_instruments(type='CS', market='cn', date=date)
        code_list = df['order_book_id'].tolist()
        with open(code_list_file, 'wb') as f:
            pickle.dump(code_list, f)
        return code_list


# 函数：加载或生成 code_dict
def get_code_dict(date):
    code_dict_dir = 'F:/DolphinDB/instruments/code_dict'
    os.makedirs(code_dict_dir, exist_ok=True)
    code_dict_file = os.path.join(code_dict_dir, f"code_dict_{date}.pkl")

    if os.path.exists(code_dict_file):
        with open(code_dict_file, 'rb') as f:
            return pickle.load(f)
    else:
        code_dict = {}
        code_list = get_code_list(date)
        for code in code_list:
            normal_code = rqdatac.id_convert(code, to='normal')
            code_dict[code] = normal_code
        with open(code_dict_file, 'wb') as f:
            pickle.dump(code_dict, f)
        return code_dict
