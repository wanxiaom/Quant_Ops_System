import os


# 函数：获取指定目录下所有CSV文件的文件名（去掉路径和 .csv 后缀）
def get_filenames_without_extension(directory):
    if not os.path.exists(directory):
        print(f"目录 {directory} 不存在")
        return []

    files = [f for f in os.listdir(directory) if f.endswith('.csv')]
    filenames = [os.path.splitext(f)[0] for f in files]
    return filenames
