#pragma once
#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <chrono>
#include <mutex>
#include <future>
#include <condition_variable>
#include <atomic>
#include <Eigen/Dense>

namespace PortfolioBacktest {

// DolphinDB连接配置
struct DolphinDBConfig {
    std::string host = "localhost";
    int port = 8848;
    std::string username = ${DB_USERNAME};
    std::string password = ${DB_PASSWORD};
    int connection_pool_size = 10;
    int timeout_ms = 10000;
};

// 简单查询结果行
struct DolphinDBRow {
    std::vector<std::string> values;
    std::vector<std::string> types;  // 数据类型
};

// 查询结果表
struct DolphinDBTable {
    bool success = false;
    int row_count = 0;
    int column_count = 0;
    std::vector<std::string> column_names;
    std::vector<std::vector<std::string>> string_data;    // 字符串列数据
    std::vector<std::vector<double>> numeric_data;        // 数值列数据
    std::vector<std::vector<std::chrono::system_clock::time_point>> date_data;  // 日期列数据
    
    // 获取行数据
    DolphinDBRow getRow(int row_idx) const {
        DolphinDBRow row;
        for (int col = 0; col < column_count; ++col) {
            if (!string_data[col].empty() && row_idx < static_cast<int>(string_data[col].size())) {
                row.values.push_back(string_data[col][row_idx]);
                row.types.push_back("string");
            } else if (!numeric_data[col].empty() && row_idx < static_cast<int>(numeric_data[col].size())) {
                row.values.push_back(std::to_string(numeric_data[col][row_idx]));
                row.types.push_back("numeric");
            } else if (!date_data[col].empty() && row_idx < static_cast<int>(date_data[col].size())) {
                auto time_t = std::chrono::system_clock::to_time_t(date_data[col][row_idx]);
                char buffer[80];
                std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", std::localtime(&time_t));
                row.values.push_back(buffer);
                row.types.push_back("date");
            } else {
                row.values.push_back("");
                row.types.push_back("unknown");
            }
        }
        return row;
    }
    
    // 获取所有行
    std::vector<DolphinDBRow> getAllRows() const {
        std::vector<DolphinDBRow> rows;
        rows.reserve(row_count);
        for (int i = 0; i < row_count; ++i) {
            rows.push_back(getRow(i));
        }
        return rows;
    }
};

// 策略持仓结构
struct PortfolioHoldings {
    std::string strategy_name;
    std::chrono::system_clock::time_point trade_date;
    std::vector<std::string> symbols;
    Eigen::VectorXd weights;
    
    // 打印持仓信息
    void print() const {
        auto time_t = std::chrono::system_clock::to_time_t(trade_date);
        char buffer[80];
        std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", std::localtime(&time_t));
        
        std::cout << "Strategy: " << strategy_name << std::endl;
        std::cout << "Trade Date: " << buffer << std::endl;
        std::cout << "Number of holdings: " << symbols.size() << std::endl;
        
        double total_weight = weights.sum();
        std::cout << "Total weight: " << total_weight << std::endl;
        
        for (size_t i = 0; i < symbols.size(); ++i) {
            std::cout << "  " << symbols[i] << ": " << weights[i] 
                     << " (" << (weights[i] / total_weight * 100) << "%)" << std::endl;
        }
    }
};

// DolphinDB连接器类
class DolphinDBConnector {
public:
    explicit DolphinDBConnector(const DolphinDBConfig& config);
    ~DolphinDBConnector();
    
    // 禁用拷贝
    DolphinDBConnector(const DolphinDBConnector&) = delete;
    DolphinDBConnector& operator=(const DolphinDBConnector&) = delete;
    
    // 移动构造和赋值
    DolphinDBConnector(DolphinDBConnector&&) noexcept = default;
    DolphinDBConnector& operator=(DolphinDBConnector&&) noexcept = default;
    
    // 连接管理
    bool test_connection();
    bool reconnect();
    void disconnect();  // 添加断开连接的方法
    
    // 查询接口
    DolphinDBTable execute_query(const std::string& query);
    
    // 批量查询
    std::vector<DolphinDBTable> execute_batch_queries(
        const std::vector<std::string>& queries);
    
    // 异步查询（Future模式）
    std::future<DolphinDBTable> execute_query_async(const std::string& query);
    
    // 统计信息
    struct ConnectionStats {
        int total_connections = 0;
        int active_connections = 0;
        int failed_queries = 0;
        int successful_queries = 0;
        double avg_query_time_ms = 0.0;
    };
    
    ConnectionStats get_stats() const;
    
private:
    // 连接状态检查
    bool is_connected() const;  // 添加连接状态检查方法
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace PortfolioBacktest
