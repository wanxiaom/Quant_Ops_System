#include <iostream>
#include <iomanip>
#include <sstream>
#include <deque>
#include <queue>
#include <future>
#include <algorithm>
#include <chrono>
#include <ctime>
#include <cstring>

#include <spdlog/spdlog.h>

#include "dolphindb.h"
#include "dolphin_db_connector.hpp"

namespace PortfolioBacktest {

// 实现类
class DolphinDBConnector::Impl {
private:
    DolphinDBConfig config_;
    std::deque<std::shared_ptr<dolphindb::DBConnection>> connection_pool_;
    std::mutex pool_mutex_;
    std::condition_variable pool_cv_;
    std::atomic<bool> running_{true};
    
    // 统计信息
    struct Stats {
        std::atomic<int> total_connections{0};
        std::atomic<int> active_connections{0};
        std::atomic<int> failed_queries{0};
        std::atomic<int> successful_queries{0};
        std::atomic<double> total_query_time_ms{0.0};
    };
    
    Stats stats_;
    std::atomic<bool> connected_{false};
    
public:
    explicit Impl(const DolphinDBConfig& config) : config_(config) {
        connected_ = false;
        initialize_connection_pool();
        connected_ = !connection_pool_.empty();
    }
    
    ~Impl() {
        shutdown();
    }
    
    void shutdown() {
        running_ = false;
        connected_ = false;
        pool_cv_.notify_all();
        
        std::lock_guard<std::mutex> lock(pool_mutex_);
        for (auto& conn : connection_pool_) {
            if (conn) {
                try {
                    conn->close();
                } catch (...) {
                    // 忽略关闭时的异常
                }
            }
        }
        connection_pool_.clear();
    }
    
    void disconnect() {
        spdlog::info("Disconnecting from DolphinDB...");
        shutdown();
        spdlog::info("Disconnected from DolphinDB");
    }

    bool test_connection() {
        auto conn = get_connection();
        if (!conn) {
            spdlog::error("No connection available from pool");
            stats_.failed_queries++;
            return false;
        }
    
        try {
            auto result = conn->run("1+1");
            // 检查result是否有效且不为空
            if (!result->isNull()) {
                return_connection(conn);
                return true;
            }
        } catch (const std::exception& e) {
            spdlog::error("Connection test failed: {}", e.what());
        }
        return false;
    }

    bool is_connected() const {
        return connected_.load();
    }  
    
    bool reconnect() {
        spdlog::info("Reconnecting to DolphinDB...");
        shutdown();
        running_ = true;
        connection_pool_.clear();
        initialize_connection_pool();
        
        bool success = !connection_pool_.empty();
        connected_ = success;
        
        if (success) {
            spdlog::info("Reconnected to DolphinDB");
        } else {
            spdlog::error("Failed to reconnect to DolphinDB");
        }
        
        return success;
    }
    
private:
    void initialize_connection_pool() {
        spdlog::info("Initializing DolphinDB connection pool with {} connections", 
                     config_.connection_pool_size);
        
        int successful_connections = 0;
        
        for (int i = 0; i < config_.connection_pool_size; ++i) {
            auto conn = std::make_shared<dolphindb::DBConnection>();
            try {
                bool success = conn->connect(
                    config_.host.c_str(),
                    config_.port,
                    config_.username.c_str(),
                    config_.password.c_str()
                );
                
                if (success) {
                    std::lock_guard<std::mutex> lock(pool_mutex_);
                    connection_pool_.push_back(conn);
                    successful_connections++;
                    stats_.total_connections++;
                    
                    spdlog::info("Connection {} established to {}:{}", 
                                 i, config_.host, config_.port);
                } else {
                    spdlog::error("Failed to connect to {}:{}", 
                                  config_.host, config_.port);
                }
            } catch (const std::exception& e) {
                spdlog::error("Connection error: {}", e.what());
            }
        }
        
        if (successful_connections == 0) {
            connected_ = false;
            std::stringstream ss;
            ss << "No valid connections established to DolphinDB at " 
               << config_.host << ":" << config_.port;
            throw std::runtime_error(ss.str());
        }
        
        connected_ = true;
        spdlog::info("Successfully established {}/{} connections", 
                     successful_connections, config_.connection_pool_size);
    }
    
    std::shared_ptr<dolphindb::DBConnection> get_connection() {
        if (!connected_) {
            spdlog::error("Cannot get connection: not connected to DolphinDB");
            return nullptr;
        }
        
        std::unique_lock<std::mutex> lock(pool_mutex_);
        
        auto timeout = std::chrono::milliseconds(config_.timeout_ms);
        bool got_connection = pool_cv_.wait_for(lock, timeout, [this]() {
            return !connection_pool_.empty() || !running_;
        });
        
        if (!got_connection) {
            spdlog::warn("Timeout waiting for connection from pool");
            return nullptr;
        }
        
        if (!running_) {
            return nullptr;
        }
        
        if (connection_pool_.empty()) {
            spdlog::warn("Connection pool is empty");
            return nullptr;
        }
 
        auto conn = connection_pool_.front();
        connection_pool_.pop_front();
        stats_.active_connections++;
        
        return conn;
    }
    
    void return_connection(std::shared_ptr<dolphindb::DBConnection> conn) {
        if (!conn) return;
        
        {
            std::lock_guard<std::mutex> lock(pool_mutex_);
            connection_pool_.push_back(conn);
            stats_.active_connections--;
        }
        pool_cv_.notify_one();
    }
    
    static bool is_valid_and_not_null(const dolphindb::ConstantSP& sp) {
        try {
            return !sp->isNull();
        } catch (...) {
            return false;
        }
    }
    
    DolphinDBTable process_query_result(const dolphindb::ConstantSP& result) {
        DolphinDBTable table;

        try {
            if (result->isNull()) {
                spdlog::warn("Query returned null value");
                return table;
            }
            
            if (!result->isTable()) {
                spdlog::warn("Query result is not a table");
                return table;
            }
            
            dolphindb::TableSP table_sp = result;
            int cols = table_sp->columns();
            int rows = table_sp->rows();
            
            if (rows == 0 || cols == 0) {
                spdlog::info("Query returned empty result");
                table.success = true;
                table.row_count = 0;
                table.column_count = 0;
                return table;
            }
            
            table.column_names.reserve(cols);
            for (int i = 0; i < cols; ++i) {
                table.column_names.push_back(table_sp->getColumnName(i));
            }
            
            table.string_data.resize(cols);
            table.numeric_data.resize(cols);
            table.date_data.resize(cols);
            
            for (int col = 0; col < cols; ++col) {
                dolphindb::VectorSP col_vec = table_sp->getColumn(col);
                if (col_vec->isNull()) {
                    spdlog::warn("Column {} is null", col);
                    continue;
                }
                
                int col_type = col_vec->getType();
                
                if (col_type == 17 || col_type == 18) {
                    table.string_data[col].resize(rows);
                    for (int row = 0; row < rows; ++row) {
                        table.string_data[col][row] = col_vec->getString(row);
                    }
                } 
                else if (col_type >= 1 && col_type <= 5 || col_type == 15 || col_type == 16) {
                    table.numeric_data[col].resize(rows);
                    for (int row = 0; row < rows; ++row) {
                        table.numeric_data[col][row] = col_vec->getDouble(row);
                    }
                }
                else if (col_type == 6 || col_type == 11 || col_type == 12) {
                    table.date_data[col].resize(rows);
                    for (int row = 0; row < rows; ++row) {
                        long long timestamp = 0;
                        if (col_type == 6) {
                            int days = col_vec->getInt(row);
                            timestamp = static_cast<long long>(days) * 24 * 60 * 60 * 1000000000LL;
                        } else if (col_type == 11 || col_type == 12) {
                            timestamp = col_vec->getLong(row) * 1000000LL;
                        }
                        
                        auto tp = std::chrono::system_clock::time_point(
                            std::chrono::nanoseconds(timestamp));
                        table.date_data[col][row] = tp;
                    }
                }
                else {
                    table.string_data[col].resize(rows);
                    for (int row = 0; row < rows; ++row) {
                        table.string_data[col][row] = col_vec->getString(row);
                    }
                    spdlog::info("Column '{}' has type {}, treated as string", 
                               table.column_names[col], col_type);
                }
            }
            
            table.success = true;
            table.row_count = rows;
            table.column_count = cols;
            
        } catch (const std::exception& e) {
            spdlog::error("Error processing query result: {}", e.what());
        }
        
        return table;
    }
    
public:
    DolphinDBTable execute_query(const std::string& query) {
        auto start_time = std::chrono::high_resolution_clock::now();
        
        DolphinDBTable result;
        
        auto conn = get_connection();
        
        if (!conn) {
            spdlog::error("No connection available from pool");
            stats_.failed_queries++;
            return result;
        }
        
        try {
            auto query_result = conn->run(query);
            result = process_query_result(query_result);
            
            if (result.success) {
                stats_.successful_queries++;
                spdlog::info("Query successful: {} rows, {} columns", 
                           result.row_count, result.column_count);
            } else {
                stats_.failed_queries++;
                spdlog::warn("Query returned no data");
            }
            
        } catch (const std::exception& e) {
            spdlog::error("Query execution failed: {}", e.what());
            stats_.failed_queries++;
            result.success = false;
        }
        
        return_connection(conn);
        
        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time).count();
        
        return result;
    }
    
    std::future<DolphinDBTable> execute_query_async(const std::string& query) {
        return std::async(std::launch::async, [this, query]() {
            return execute_query(query);
        });
    }
    
    ConnectionStats get_stats() const {
        ConnectionStats stats;
        stats.total_connections = stats_.total_connections;
        stats.active_connections = stats_.active_connections;
        stats.failed_queries = stats_.failed_queries;
        stats.successful_queries = stats_.successful_queries;
        
        int total_queries = stats.failed_queries + stats.successful_queries;
        if (total_queries > 0) {
            stats.avg_query_time_ms = stats_.total_query_time_ms / total_queries;
        }
        
        return stats;
    }
};

// =================== 公共接口实现 ===================

DolphinDBConnector::DolphinDBConnector(const DolphinDBConfig& config)
    : impl_(std::make_unique<Impl>(config)) {}

DolphinDBConnector::~DolphinDBConnector() = default;

bool DolphinDBConnector::test_connection() {
    return impl_->test_connection();
}

bool DolphinDBConnector::reconnect() {
    return impl_->reconnect();
}

void DolphinDBConnector::disconnect() {
    return impl_->disconnect();
}

bool DolphinDBConnector::is_connected() const {
    return impl_->is_connected();
}

DolphinDBTable DolphinDBConnector::execute_query(const std::string& query) {
    return impl_->execute_query(query);
}

std::future<DolphinDBTable> DolphinDBConnector::execute_query_async(const std::string& query) {
    return impl_->execute_query_async(query);
}

DolphinDBConnector::ConnectionStats DolphinDBConnector::get_stats() const {
    return impl_->get_stats();
}

std::vector<DolphinDBTable> 
DolphinDBConnector::execute_batch_queries(const std::vector<std::string>& queries) {
    std::vector<DolphinDBTable> results;
    results.reserve(queries.size());
    
    std::vector<std::future<DolphinDBTable>> futures;
    futures.reserve(queries.size());
    
    for (const auto& query : queries) {
        futures.push_back(execute_query_async(query));
    }
    
    for (auto& future : futures) {
        results.push_back(future.get());
    }
    
    return results;
}

} // namespace PortfolioBacktest