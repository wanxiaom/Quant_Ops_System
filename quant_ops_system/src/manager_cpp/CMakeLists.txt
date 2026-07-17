cmake_minimum_required(VERSION 3.14)
project(quant_manager_cpp CXX)

# 强制使用 C++20 标准 (最新版 ormpp 及其依赖 iguana 需要)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# 启用 ORMPP 的 MySQL 支持宏
add_compile_definitions(ORMPP_ENABLE_MYSQL)

# 添加本地用户目录和 Conda 目录到搜索路径 (适配无 root 权限环境)
list(APPEND CMAKE_PREFIX_PATH "$ENV{HOME}/.local" "$ENV{CONDA_PREFIX}")
include_directories("$ENV{HOME}/.local/include/hv" "$ENV{CONDA_PREFIX}/include")

# 查找系统中的外部库依赖
find_library(HV_LIBRARY hv REQUIRED)
find_library(MYSQL_LIBRARY NAMES mysqlclient mysqlclient_r REQUIRED)

include_directories($ENV{HOME}/.local/include)
link_directories($ENV{HOME}/.local/lib)

# 引入 ormpp 和其底层依赖 iguana 的头文件 (直接指向克隆下来的仓库根目录)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../ormpp)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../../iguana)

# 编译核心可执行文件
add_executable(manager_cpp main.cpp)

# 链接线程库与第三方动态库
target_link_libraries(manager_cpp pthread ${HV_LIBRARY} ${MYSQL_LIBRARY} redis++ hiredis)
