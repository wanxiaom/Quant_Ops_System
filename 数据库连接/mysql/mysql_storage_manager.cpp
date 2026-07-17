#include <sstream>
#include <iomanip>

#include "utils.hpp"

#include "backtest_manager/mysql/mysql_storage_manager.h"


MySQLStorageManager::MySQLStorageManager(connection_pool<dbng<mysql>>& pool) 
    : pool_(pool) {
}

// 回测任务管理
int64_t MySQLStorageManager::create_backtest_task(const BacktestTask& task) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return -1;
    }
    
    try {
        BacktestTask task_to_insert = task;
        task_to_insert.created_at = get_current_time();
        task_to_insert.updated_at = get_current_time();
        
        if (conn->insert(task_to_insert) > 0) {
            // 获取插入的ID（假设task_id是自增主键）
            auto result = conn->query<std::tuple<int64_t>>(
                "SELECT LAST_INSERT_ID()"
            );
            if (!result.empty()) {
                pool_.return_back(conn);
                return std::get<0>(result[0]);
            }
        }
        pool_.return_back(conn);
        return -1;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return -1;
    }
}

bool MySQLStorageManager::update_backtest_task(const BacktestTask& task) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        BacktestTask task_to_update = task;
        task_to_update.updated_at = get_current_time();
        
        auto condition = "task_id=" + std::to_string(task.task_id);
        bool result = conn->update(task_to_update, condition) > 0;
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return false;
    }
}

bool MySQLStorageManager::update_task_status(int64_t task_id, 
                                           const std::string& status) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }

    try {
        std::string sql = "UPDATE backtest_tasks SET status='" + status + "'";
        sql += ", updated_at='" + get_current_time() + "'";
        sql += " WHERE task_id=" + std::to_string(task_id);
        
        bool result = conn->execute(sql) > 0;
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return false;
    }
}

BacktestTask MySQLStorageManager::get_backtest_task(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "task_id=" + std::to_string(task_id);
        auto tasks = conn->query<BacktestTask>(condition);
        pool_.return_back(conn);
        return tasks.empty() ? BacktestTask{} : tasks[0];
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

std::vector<BacktestTask> MySQLStorageManager::get_backtest_tasks(int limit, int offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "1=1 ORDER BY created_at DESC";
        if (limit > 0) {
            condition += " LIMIT " + std::to_string(limit);
            if (offset > 0) {
                condition += " OFFSET " + std::to_string(offset);
            }
        }
        auto result = conn->query<BacktestTask>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

// 组合信息管理
int64_t MySQLStorageManager::create_portfolio(const Portfolio& portfolio) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return -1;
    }
    
    try {
        Portfolio portfolio_to_insert = portfolio;
        portfolio_to_insert.created_at = get_current_time();
        
        if (conn->insert(portfolio_to_insert) > 0) {
            auto result = conn->query<std::tuple<int64_t>>(
                "SELECT LAST_INSERT_ID()"
            );
            if (!result.empty()) {
                pool_.return_back(conn);
                return std::get<0>(result[0]);
            }
        }
        pool_.return_back(conn);
        return -1;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return -1;
    }
}

// 根据ID获取策略
std::optional<StrategyConfig> MySQLStorageManager::getStrategyById(int id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return std::nullopt;
    }

    try {
        auto strategies = conn->template query_s<StrategyConfig>("id=?", id);
        pool_.return_back(conn);
        if (strategies.empty()) {
            return std::nullopt;
        }
        return strategies[0];
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        throw std::runtime_error(std::string("Failed to get strategy by ID: ") + e.what());
    }
}

Portfolio MySQLStorageManager::get_portfolio(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id);
        auto portfolios = conn->query<Portfolio>(condition);
        pool_.return_back(conn);
        return portfolios.empty() ? Portfolio{} : portfolios[0];
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

std::vector<Portfolio> MySQLStorageManager::get_portfolios_by_task(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "task_id=" + std::to_string(task_id);
        auto result = conn->query<Portfolio>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

int64_t MySQLStorageManager::store_portfolio(const Portfolio& portfolio) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        std::cerr << "storing portfolio: pool_.get failed." << std::endl;
        return 0;
    }
    
    try {
        // 验证输入数据
        if (!portfolio.validate()) {
            std::cerr << "Portfolio validation failed" << std::endl;
            pool_.return_back(conn);
            return 0;
        }
        
        Portfolio portfolio_to_insert = portfolio;
        std::string current_time = get_current_time();
        
        // 设置时间戳
        portfolio_to_insert.created_at = current_time;
        portfolio_to_insert.updated_at = current_time;
        
        // 检查 display_name 是否为空，如果为空则使用 portfolio_name
        if (portfolio_to_insert.display_name.empty()) {
            portfolio_to_insert.display_name = portfolio_to_insert.portfolio_name;
        }
        
        // 如果 parent_portfolio_id 为 0，设置为 nullopt
        if (portfolio_to_insert.parent_portfolio_id.has_value() && 
            portfolio_to_insert.parent_portfolio_id.value() == 0) {
            portfolio_to_insert.parent_portfolio_id = std::nullopt;
        }
        
        // 插入数据库
        int ret = conn->insert(portfolio_to_insert);
        if (ret > 0) {
            auto result = conn->query<std::tuple<int64_t>>("SELECT LAST_INSERT_ID()");
            if (!result.empty()) {
                int64_t new_id = std::get<0>(result[0]);
                std::cout << "storing portfolio: portfolio_id " << new_id << std::endl;
                pool_.return_back(conn);
                return new_id;
            }
        }
        
        pool_.return_back(conn);
        return 0;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error storing portfolio: " << e.what() << std::endl;
        return 0;
    }
}

// 收益率数据管理
bool MySQLStorageManager::store_daily_returns(const std::vector<DailyReturn>& returns) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        std::vector<DailyReturn> returns_to_insert;
        std::string current_time = get_current_time();
        
        for (const auto& ret : returns) {
            DailyReturn ret_to_insert = ret;
            ret_to_insert.return_id = 0;
            ret_to_insert.created_at = current_time;
            returns_to_insert.push_back(ret_to_insert);
        }
        
        bool result = conn->insert(returns_to_insert) > 0;
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return false;
    }
}

std::vector<DailyReturn> MySQLStorageManager::get_daily_returns(int64_t portfolio_id,
                                                              const std::optional<std::string>& start_date,
                                                              const std::optional<std::string>& end_date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "portfolio_id=" + std::to_string(portfolio_id);
        if (start_date.has_value()) {
            condition += " AND date>='" + start_date.value() + "'";
        }
        if (end_date.has_value()) {
            condition += " AND date<='" + end_date.value() + "'";
        }
        condition += " ORDER BY date";
        
        auto result = conn->query<DailyReturn>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

// 其他存储方法的实现类似，这里只展示关键部分
bool MySQLStorageManager::store_period_returns(const std::vector<PeriodReturn>& returns) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(returns);
}

std::vector<PeriodReturn> MySQLStorageManager::get_period_returns(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " ORDER BY rebalance_date";
        auto result = conn->query<PeriodReturn>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_performance_summary(const PerformanceSummary& summary) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }

    try {
        bool result = conn->insert(summary) > 0;
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error store_performance_summary: " << e.what() << std::endl;
        return false;
    }
}

bool MySQLStorageManager::store_performance_summaries(const std::vector<PerformanceSummary>& summaries) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(summaries);
}

// 获取特定组合的绩效汇总数据（按开始日期排序）
std::vector<PerformanceSummary> MySQLStorageManager::get_performance_summaries(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " ORDER BY start_date";
        auto result = conn->query<PerformanceSummary>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error getting performance summaries: " << e.what() << std::endl;
        return {};
    }
}

// 获取特定组合和日期范围的绩效汇总数据
std::vector<PerformanceSummary> MySQLStorageManager::get_performance_summaries_by_date_range(
    int64_t portfolio_id, 
    const std::string& start_date, 
    const std::string& end_date) {
    
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " AND start_date >= '" + start_date + "'" +
                        " AND end_date <= '" + end_date + "'" +
                        " ORDER BY start_date";
        auto result = conn->query<PerformanceSummary>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error getting performance summaries by date range: " << e.what() << std::endl;
        return {};
    }
}

// 获取特定组合的最新绩效汇总数据
std::optional<PerformanceSummary> MySQLStorageManager::get_latest_performance_summary(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return std::nullopt;
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " ORDER BY end_date DESC LIMIT 1";
        auto results = conn->query<PerformanceSummary>(condition);
        pool_.return_back(conn);
        
        if (!results.empty()) {
            return results[0];
        }
        return std::nullopt;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error getting latest performance summary: " << e.what() << std::endl;
        return std::nullopt;
    }
}

// 更新绩效汇总数据（基于metric_id）
bool MySQLStorageManager::update_performance_summary(const PerformanceSummary& summary) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        bool result = conn->update(summary);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error updating performance summary: " << e.what() << std::endl;
        return false;
    }
}

// 删除特定组合的所有绩效汇总数据
bool MySQLStorageManager::delete_performance_summaries(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id);
        bool result = conn->delete_records<PerformanceSummary>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Error deleting performance summaries: " << e.what() << std::endl;
        return false;
    }
}

bool MySQLStorageManager::store_yearly_statistics(const std::vector<YearlyStatistic>& stats) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(stats);
}

std::vector<YearlyStatistic> MySQLStorageManager::get_yearly_statistics(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " ORDER BY year";
        auto result = conn->query<YearlyStatistic>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_monthly_returns(const std::vector<MonthlyReturn>& returns) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(returns);
}

std::vector<MonthlyReturn> MySQLStorageManager::get_monthly_returns(int64_t portfolio_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "portfolio_id=" + std::to_string(portfolio_id) + 
                        " ORDER BY year_month";
        auto result = conn->query<MonthlyReturn>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_correlation_matrix(const std::vector<CorrelationMatrix>& correlations) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(correlations);
}

std::vector<CorrelationMatrix> MySQLStorageManager::get_correlation_matrix(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        auto condition = "task_id=" + std::to_string(task_id);
        auto result = conn->query<CorrelationMatrix>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

int64_t MySQLStorageManager::create_custom_index(const CustomIndex& index) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return -1;
    }
    
    try {
        CustomIndex index_to_insert = index;
        index_to_insert.created_at = get_current_time();
        
        if (conn->insert(index_to_insert) > 0) {
            auto result = conn->query<std::tuple<int64_t>>(
                "SELECT LAST_INSERT_ID()"
            );
            if (!result.empty()) {
                pool_.return_back(conn);
                return std::get<0>(result[0]);
            }
        }
        pool_.return_back(conn);
        return -1;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return -1;
    }
}

bool MySQLStorageManager::store_custom_index_returns(const std::vector<CustomIndexReturn>& returns) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(returns);
}

std::vector<CustomIndexReturn> MySQLStorageManager::get_custom_index_returns(int64_t index_id,
                                                                           const std::optional<std::string>& start_date,
                                                                           const std::optional<std::string>& end_date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "index_id=" + std::to_string(index_id);
        if (start_date.has_value()) {
            condition += " AND date>='" + start_date.value() + "'";
        }
        if (end_date.has_value()) {
            condition += " AND date<='" + end_date.value() + "'";
        }
        condition += " ORDER BY date";
        
        auto result = conn->query<CustomIndexReturn>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_portfolio_weights(const std::vector<PortfolioWeight>& weights) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(weights);
}

std::vector<PortfolioWeight> MySQLStorageManager::get_portfolio_weights(int64_t portfolio_id,
                                                                      const std::optional<std::string>& rebalance_date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "portfolio_id=" + std::to_string(portfolio_id);
        if (rebalance_date.has_value()) {
            condition += " AND rebalance_date='" + rebalance_date.value() + "'";
        }
        condition += " ORDER BY rebalance_date, stock_code";
        
        auto result = conn->query<PortfolioWeight>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_composite_weights(const std::vector<CompositeWeight>& weights) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(weights);
}

std::vector<CompositeWeight> MySQLStorageManager::get_composite_weights(int64_t task_id,
                                                                      const std::optional<std::string>& rebalance_date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "task_id=" + std::to_string(task_id);
        if (rebalance_date.has_value()) {
            condition += " AND rebalance_date='" + rebalance_date.value() + "'";
        }
        condition += " ORDER BY rebalance_date, parent_portfolio_id";
        
        auto result = conn->query<CompositeWeight>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_trade_details(const std::vector<TradeDetail>& trades) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(trades);
}

std::vector<TradeDetail> MySQLStorageManager::get_trade_details(int64_t portfolio_id,
                                                              const std::optional<std::string>& rebalance_date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "portfolio_id=" + std::to_string(portfolio_id);
        if (rebalance_date.has_value()) {
            condition += " AND rebalance_date='" + rebalance_date.value() + "'";
        }
        condition += " ORDER BY trade_date, stock_code";
        
        auto result = conn->query<TradeDetail>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

bool MySQLStorageManager::store_holding_details(const std::vector<HoldingDetail>& holdings) {
	std::lock_guard<std::mutex> lock(mutex_);

    return batch_insert(holdings);
}

std::vector<HoldingDetail> MySQLStorageManager::get_holding_details(int64_t portfolio_id,
                                                                  const std::optional<std::string>& date) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return {};
    }
    
    try {
        std::string condition = "portfolio_id=" + std::to_string(portfolio_id);
        if (date.has_value()) {
            condition += " AND date='" + date.value() + "'";
        }
        condition += " ORDER BY date, stock_code";
        
        auto result = conn->query<HoldingDetail>(condition);
        pool_.return_back(conn);
        return result;
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        // 记录日志
        return {};
    }
}

// 获取任务所有结果（按组合名称分类）
std::map<std::string, BacktestResult> MySQLStorageManager::get_task_results(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    std::map<std::string, BacktestResult> results;
    
    if (!conn) {
        spdlog::error("Failed to get database connection");
        return results;
    }
    
    try {
        // 1. 首先获取任务的所有组合
        std::string portfolio_sql = 
            "SELECT portfolio_id, portfolio_name, display_name, strategy_type "
            "FROM portfolios WHERE task_id = ?";
        
        auto portfolios = conn->query_s<Portfolio>(portfolio_sql, task_id);
        
        if (portfolios.empty()) {
            spdlog::warn("No portfolios found for task_id: {}", task_id);
            pool_.return_back(conn);
            return results;
        }
        
        // 2. 对每个组合获取完整的结果
        for (const auto& portfolio : portfolios) {
            if (portfolio.portfolio_id == 0) {
                continue;
            }
            
            BacktestResult result;
            result.portfolio_name = portfolio.portfolio_name;
            result.display_name = portfolio.display_name;
            result.strategy_type = portfolio.strategy_type;
            
            try {
                // 3. 获取调仓日序列和绩效汇总
                std::string period_sql = 
                    "SELECT rebalance_date, period_start, period_end "
                    "FROM period_returns "
                    "WHERE portfolio_id = ? "
                    "ORDER BY rebalance_date";
                
                auto period_records = conn->query_s<PeriodReturn>(period_sql, portfolio.portfolio_id);
                
                if (period_records.empty()) {
                    spdlog::warn("No period returns found for portfolio_id: {}", portfolio.portfolio_id);
                    continue;
                }
                
                // 填充交易日序列
                result.trade_dates.reserve(period_records.size());
                for (const auto& record : period_records) {
                    result.trade_dates.push_back(string_to_time_point(record.rebalance_date));
                }
                
                // 4. 获取最新的绩效汇总
                std::string summary_sql = 
                    "SELECT * FROM performance_summaries "
                    "WHERE portfolio_id = ? "
                    "ORDER BY updated_at DESC "
                    "LIMIT 1";
                
                auto summaries = conn->query_s<PerformanceSummary>(summary_sql, portfolio.portfolio_id);
                
                if (!summaries.empty()) {
                    result.summary = summaries[0];
                }
                
                // 5. 获取调仓期收益数据
                size_t n = period_records.size();
                result.period_returns.resize(n);
                result.period_returns_bc.resize(n);
                result.benchmark_returns.resize(n);
                result.active_returns.resize(n);
                result.turnover_rates.resize(n);
                
                for (size_t i = 0; i < n; ++i) {
                    const auto& record = period_records[i];
                    result.period_returns[i] = record.return_rate;
                    result.period_returns_bc[i] = record.return_rate_bc;
                    result.benchmark_returns[i] = record.benchmark_return;
                    result.active_returns[i] = record.active_return;
                    result.turnover_rates[i] = record.turnover_rate;
                }
                
                // 6. 获取日频收益数据（如果存在）
                try {
                    std::string daily_sql = 
                        "SELECT date, return_rate, active_return "
                        "FROM daily_returns "
                        "WHERE portfolio_id = ? AND date BETWEEN ? AND ? "
                        "ORDER BY date";
                    
                    auto start_date = period_records.front().period_start;
                    auto end_date = period_records.back().period_end;
                    
                    auto daily_records = conn->query_s<DailyReturn>(daily_sql, portfolio.portfolio_id, start_date, end_date);
                    
                    if (!daily_records.empty()) {
                        size_t daily_n = daily_records.size();
                        result.daily_returns.resize(daily_n);
                        result.benchmark_daily_returns.resize(daily_n);
                        
                        for (size_t i = 0; i < daily_n; ++i) {
                            const auto& record = daily_records[i];
                            result.daily_returns[i] = record.return_rate;
                            // 基准收益率 = 组合收益率 - 主动收益率
                            result.benchmark_daily_returns[i] = record.return_rate - record.active_return;
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to fetch daily returns for portfolio {}: {}",
                                 portfolio.portfolio_id, e.what());
                }
                
                // 7. 获取持仓历史（权重）
                try {
                    std::string weights_sql = 
                        "SELECT weight_id, portfolio_id, rebalance_date, stock_code, stock_name, weight, created_at "
                        "FROM portfolio_weights "
                        "WHERE portfolio_id = ? "
                        "ORDER BY rebalance_date, stock_code";
                    
                    auto weight_records = conn->query_s<PortfolioWeight>(weights_sql, portfolio.portfolio_id);
                    
                    if (!weight_records.empty()) {
                        // 按调仓日期分组构建权重历史
                        std::map<std::string, std::map<std::string, double>> grouped_weights;
                        
                        for (const auto& weight : weight_records) {
                            grouped_weights[weight.rebalance_date][weight.stock_code] = weight.weight;
                        }
                        
                        // 按照调仓日期顺序填充
                        for (const auto& period : period_records) {
                            auto it = grouped_weights.find(period.rebalance_date);
                            if (it != grouped_weights.end()) {
                                result.weights_history.push_back(it->second);
                            } else {
                                result.weights_history.push_back({}); // 空持仓
                            }
                        }
                    }
                } catch (const std::exception& e) {
                    spdlog::warn("Failed to fetch portfolio weights for portfolio {}: {}",
                                 portfolio.portfolio_id, e.what());
                }
                
                // 8. 获取年度统计
                std::string yearly_sql = 
                    "SELECT * FROM yearly_statistics "
                    "WHERE portfolio_id = ? "
                    "ORDER BY year";
                
                auto yearly_stats = conn->query_s<YearlyStatistic>(yearly_sql, portfolio.portfolio_id);
                result.yearly_stats = yearly_stats;
                
                // 9. 获取月度统计
                std::string monthly_sql = 
                    "SELECT * FROM monthly_returns "
                    "WHERE portfolio_id = ? "
                    "ORDER BY year_month";
                
                auto monthly_stats = conn->query_s<MonthlyReturn>(monthly_sql, portfolio.portfolio_id);
                result.monthly_stats = monthly_stats;
                
                // 10. 使用组合名称作为key（如果有重复，添加ID后缀）
                std::string key = portfolio.portfolio_name;
                if (key.empty()) {
                    key = "portfolio_" + std::to_string(portfolio.portfolio_id);
                }
                
                // 检查是否重名，如果重名则添加ID后缀
                if (results.find(key) != results.end()) {
                    key = key + "_" + std::to_string(portfolio.portfolio_id);
                }
                
                results[key] = std::move(result);
                
                spdlog::info("Successfully loaded portfolio {} (ID: {}) with {} periods",
                            portfolio.portfolio_name, portfolio.portfolio_id, n);
                
            } catch (const std::exception& e) {
                spdlog::error("Error loading portfolio {} (ID: {}): {}",
                            portfolio.portfolio_name, portfolio.portfolio_id, e.what());
            }
        }
        
        spdlog::info("Loaded {} portfolios for task_id: {}", results.size(), task_id);
        pool_.return_back(conn);
        return results;
        
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        spdlog::error("Get task results error for task_id {}: {}", task_id, e.what());
        return results;
    }
}

// 获取任务所有结果（详细版本，包含所有组合的绩效数据）
std::map<std::string, std::vector<PerformanceSummary>> MySQLStorageManager::get_task_detailed_results(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    std::map<std::string, std::vector<PerformanceSummary>> results;
    
    if (!conn) {
        return results;
    }
    
    try {
        // 获取任务的所有组合
        auto portfolios = get_portfolios_by_task(task_id);
        
        for (const auto& portfolio : portfolios) {
            if (portfolio.portfolio_id == 0) continue;
            
            std::string key = "portfolio_" + std::to_string(portfolio.portfolio_id);
            if (!portfolio.portfolio_name.empty()) {
                key = portfolio.portfolio_name;
            }
            
            // 获取该组合的所有绩效汇总（按时间倒序）
            std::string condition = "portfolio_id=" + std::to_string(portfolio.portfolio_id) +
                                   " ORDER BY updated_at DESC";
            auto summaries = conn->query<PerformanceSummary>(condition);
            
            if (!summaries.empty()) {
                results[key] = summaries;
            }
        }
        
        pool_.return_back(conn);
        return results;
        
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Get task detailed results error: " << e.what() << std::endl;
        return results;
    }
}

// 取消回测任务
bool MySQLStorageManager::cancel_backtest_task(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        // 首先检查任务是否存在且状态可取消
        auto task = get_backtest_task(task_id);
        if (task.task_id == 0) {
            pool_.return_back(conn);
            return false; // 任务不存在
        }
        
        // 只有处于pending或running状态的任务可以取消
        if (task.status != "pending" && task.status != "running") {
            pool_.return_back(conn);
            return false;
        }
        
        // 更新任务状态为cancelled
        std::string sql = 
            "UPDATE backtest_tasks SET "
            "status = 'cancelled', "
            "updated_at = '" + get_current_time() + "' "
            "WHERE task_id = " + std::to_string(task_id);
        
        bool success = conn->execute(sql) > 0;
        
        // 如果成功取消，还可以添加取消记录到日志表（如果有）
        if (success) {
            // 可选：记录取消操作到任务历史或日志表
            std::string log_sql = 
                "INSERT INTO task_cancellation_logs (task_id, cancelled_at, reason) "
                "VALUES (" + std::to_string(task_id) + ", '" + 
                get_current_time() + "', 'User cancelled')";
            conn->execute(log_sql);
        }
        
        pool_.return_back(conn);
        return success;
        
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        std::cerr << "Cancel backtest task error: " << e.what() << std::endl;
        return false;
    }
}

// 删除回测任务及相关数据（级联删除）
bool MySQLStorageManager::delete_backtest_task(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    if (!conn) {
        return false;
    }
    
    try {
        // 开始事务，确保数据一致性
        conn->execute("START TRANSACTION");
        
        // 1. 获取任务的所有组合
        auto portfolios = get_portfolios_by_task(task_id);
        
        for (const auto& portfolio : portfolios) {
            if (portfolio.portfolio_id == 0) continue;
            
            // 2. 删除组合相关的所有数据
            // 2.1 删除日收益率数据
            conn->execute("DELETE FROM daily_returns WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.2 删除周期收益率数据
            conn->execute("DELETE FROM period_returns WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.3 删除绩效指标数据
            conn->execute("DELETE FROM performance_metrics WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.4 删除年度统计数据
            conn->execute("DELETE FROM yearly_statistics WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.5 删除月度收益率数据
            conn->execute("DELETE FROM monthly_returns WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.6 删除绩效汇总数据
            conn->execute("DELETE FROM performance_summaries WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.7 删除持仓权重数据
            conn->execute("DELETE FROM portfolio_weights WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.8 删除交易详情数据
            conn->execute("DELETE FROM trade_details WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 2.9 删除持仓详情数据
            conn->execute("DELETE FROM holding_details WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
            
            // 3. 删除组合本身
            conn->execute("DELETE FROM portfolios WHERE portfolio_id = " + 
                       std::to_string(portfolio.portfolio_id));
        }
        
        // 4. 删除任务相关的其他数据
        // 4.1 删除相关性矩阵数据
        conn->execute("DELETE FROM correlation_matrix WHERE task_id = " + 
                   std::to_string(task_id));
        
        // 4.2 删除复合权重数据
        conn->execute("DELETE FROM composite_weights WHERE task_id = " + 
                   std::to_string(task_id));
        
        // 4.3 删除自定义指数数据（如果有的话）
        // 首先获取任务相关的自定义指数
        std::string index_sql = 
            "SELECT index_id FROM custom_indexes WHERE task_id = " + 
            std::to_string(task_id);
        auto index_results = conn->query<std::tuple<int64_t>>(index_sql);
        
        for (const auto& index_tuple : index_results) {
            int64_t index_id = std::get<0>(index_tuple);
            // 删除自定义指数收益率数据
            conn->execute("DELETE FROM custom_index_returns WHERE index_id = " + 
                       std::to_string(index_id));
            // 删除自定义指数
            conn->execute("DELETE FROM custom_indexes WHERE index_id = " + 
                       std::to_string(index_id));
        }
        
        // 4.4 删除取消记录（如果有）
        conn->execute("DELETE FROM task_cancellation_logs WHERE task_id = " + 
                   std::to_string(task_id));
        
        // 5. 最后删除任务本身
        int deleted = conn->execute("DELETE FROM backtest_tasks WHERE task_id = " + 
                                std::to_string(task_id));
        
        if (deleted > 0) {
            conn->execute("COMMIT");
            pool_.return_back(conn);
            return true;
        } else {
            conn->execute("ROLLBACK");
            pool_.return_back(conn);
            return false;
        }
        
    } catch (const std::exception& e) {
        conn->execute("ROLLBACK");
        pool_.return_back(conn);
        std::cerr << "Delete backtest task error: " << e.what() << std::endl;
        return false;
    }
}

// 获取任务统计信息
BacktestTaskStats MySQLStorageManager::get_task_statistics(int64_t task_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto conn = pool_.get();
    BacktestTaskStats stats = {};
    stats.task_id = task_id;
    
    if (!conn) {
        spdlog::error("Failed to get database connection for task_id: {}", task_id);
        return stats;
    }
    
    try {
        // 获取任务基本信息
        auto task = get_backtest_task(task_id);
        if (task.task_id == 0) {
            spdlog::warn("Task not found for task_id: {}", task_id);
            pool_.return_back(conn);
            return stats;
        }
        
        stats.task_name = task.task_name;
        stats.status = task.status;
        stats.created_at = task.created_at;
        
        // 统计组合数量
        std::string portfolio_sql = 
            "SELECT COUNT(*) FROM portfolios WHERE task_id = ?";
        auto portfolio_count = conn->query<std::tuple<int64_t>>(portfolio_sql, task_id);
        if (!portfolio_count.empty()) {
            stats.portfolio_count = std::get<0>(portfolio_count[0]);
        }
        
        // 统计调仓期收益数据量
        if (stats.portfolio_count > 0) {
            std::string period_sql = 
                "SELECT COUNT(*) FROM period_returns pr "
                "JOIN portfolios p ON pr.portfolio_id = p.portfolio_id "
                "WHERE p.task_id = ?";
            auto period_count = conn->query<std::tuple<int64_t>>(period_sql, task_id);
            if (!period_count.empty()) {
                stats.period_returns_count = std::get<0>(period_count[0]);
            }
            
            // 统计日收益率数据量
            std::string daily_sql = 
                "SELECT COUNT(*) FROM daily_returns dr "
                "JOIN portfolios p ON dr.portfolio_id = p.portfolio_id "
                "WHERE p.task_id = ?";
            auto daily_count = conn->query<std::tuple<int64_t>>(daily_sql, task_id);
            if (!daily_count.empty()) {
                stats.daily_returns_count = std::get<0>(daily_count[0]);
            }
            
            // 统计持仓权重数据量
            std::string weights_sql = 
                "SELECT COUNT(*) FROM portfolio_weights pw "
                "JOIN portfolios p ON pw.portfolio_id = p.portfolio_id "
                "WHERE p.task_id = ?";
            auto weights_count = conn->query<std::tuple<int64_t>>(weights_sql, task_id);
            if (!weights_count.empty()) {
                stats.weights_count = std::get<0>(weights_count[0]);
            }
            
            // 统计绩效汇总数据量
            std::string summary_sql = 
                "SELECT COUNT(*) FROM performance_summaries ps "
                "JOIN portfolios p ON ps.portfolio_id = p.portfolio_id "
                "WHERE p.task_id = ?";
            auto summary_count = conn->query<std::tuple<int64_t>>(summary_sql, task_id);
            if (!summary_count.empty()) {
                stats.summary_count = std::get<0>(summary_count[0]);
            }
        }
        
        // 获取任务结果统计
        auto results = get_task_results(task_id);
        stats.result_count = results.size();
        
        // 如果有结果，计算平均绩效指标
        if (!results.empty()) {
            double total_sharpe = 0.0;
            double total_sharpe_bc = 0.0;  // 费前夏普
            double total_max_dd = 0.0;
            double total_annualized_return = 0.0;
            double total_annualized_vol = 0.0;
            double total_information_ratio = 0.0;
            double total_alpha = 0.0;
            double total_beta = 0.0;
            double total_win_rate = 0.0;
            double total_turnover = 0.0;
            
            int valid_sharpe_count = 0;
            int valid_max_dd_count = 0;
            int valid_annualized_count = 0;
            int valid_vol_count = 0;
            int valid_ir_count = 0;
            int valid_alpha_count = 0;
            int valid_beta_count = 0;
            int valid_win_rate_count = 0;
            int valid_turnover_count = 0;
            
            for (const auto& [key, result] : results) {
                const auto& summary = result.summary;
                
                // 夏普比率
                if (!std::isnan(summary.portfolio_sharpe_ratio) && 
                    !std::isinf(summary.portfolio_sharpe_ratio)) {
                    total_sharpe += summary.portfolio_sharpe_ratio;
                    valid_sharpe_count++;
                }
                
                // 最大回撤
                if (!std::isnan(summary.portfolio_max_drawdown) && 
                    !std::isinf(summary.portfolio_max_drawdown)) {
                    total_max_dd += summary.portfolio_max_drawdown;
                    valid_max_dd_count++;
                }
                
                // 年化收益率
                if (!std::isnan(summary.portfolio_annualized_return) && 
                    !std::isinf(summary.portfolio_annualized_return)) {
                    total_annualized_return += summary.portfolio_annualized_return;
                    valid_annualized_count++;
                }
                
                // 年化波动率
                if (!std::isnan(summary.portfolio_annualized_vol) && 
                    !std::isinf(summary.portfolio_annualized_vol)) {
                    total_annualized_vol += summary.portfolio_annualized_vol;
                    valid_vol_count++;
                }
                
                // 信息比率
                if (!std::isnan(summary.active_information_ratio) && 
                    !std::isinf(summary.active_information_ratio)) {
                    total_information_ratio += summary.active_information_ratio;
                    valid_ir_count++;
                }
                
                // Alpha
                if (!std::isnan(summary.alpha) && 
                    !std::isinf(summary.alpha)) {
                    total_alpha += summary.alpha;
                    valid_alpha_count++;
                }
                
                // Beta
                if (!std::isnan(summary.beta) && 
                    !std::isinf(summary.beta)) {
                    total_beta += summary.beta;
                    valid_beta_count++;
                }
                
                // 胜率
                if (!std::isnan(summary.portfolio_win_rate) && 
                    !std::isinf(summary.portfolio_win_rate)) {
                    total_win_rate += summary.portfolio_win_rate;
                    valid_win_rate_count++;
                }
                
                // 换手率
                if (!std::isnan(summary.annualized_turnover) && 
                    !std::isinf(summary.annualized_turnover)) {
                    total_turnover += summary.annualized_turnover;
                    valid_turnover_count++;
                }
            }
            
            // 计算平均值
            if (valid_sharpe_count > 0) {
                stats.avg_sharpe_ratio = total_sharpe / valid_sharpe_count;
            }
            
            if (valid_max_dd_count > 0) {
                stats.avg_max_drawdown = total_max_dd / valid_max_dd_count;
            }
            
            if (valid_annualized_count > 0) {
                stats.avg_annualized_return = total_annualized_return / valid_annualized_count;
            }
            
            if (valid_vol_count > 0) {
                stats.avg_annualized_vol = total_annualized_vol / valid_vol_count;
            }
            
            if (valid_ir_count > 0) {
                stats.avg_information_ratio = total_information_ratio / valid_ir_count;
            }
            
            if (valid_alpha_count > 0) {
                stats.avg_alpha = total_alpha / valid_alpha_count;
            }
            
            if (valid_beta_count > 0) {
                stats.avg_beta = total_beta / valid_beta_count;
            }
            
            if (valid_win_rate_count > 0) {
                stats.avg_win_rate = total_win_rate / valid_win_rate_count;
            }
            
            if (valid_turnover_count > 0) {
                stats.avg_turnover = total_turnover / valid_turnover_count;
            }
            
            // 计算最佳和最差组合
            if (!results.empty()) {
                std::vector<std::pair<std::string, double>> sharpe_values;
                std::vector<std::pair<std::string, double>> return_values;
                std::vector<std::pair<std::string, double>> max_dd_values;
                
                for (const auto& [key, result] : results) {
                    const auto& summary = result.summary;
                    
                    if (!std::isnan(summary.portfolio_sharpe_ratio)) {
                        sharpe_values.emplace_back(key, summary.portfolio_sharpe_ratio);
                    }
                    
                    if (!std::isnan(summary.portfolio_annualized_return)) {
                        return_values.emplace_back(key, summary.portfolio_annualized_return);
                    }
                    
                    if (!std::isnan(summary.portfolio_max_drawdown)) {
                        max_dd_values.emplace_back(key, summary.portfolio_max_drawdown);
                    }
                }
                
                // 找到夏普最高的组合
                if (!sharpe_values.empty()) {
                    auto best_sharpe = std::max_element(
                        sharpe_values.begin(), sharpe_values.end(),
                        [](const auto& a, const auto& b) { return a.second < b.second; });
                    stats.best_sharpe_portfolio = best_sharpe->first;
                    stats.best_sharpe_ratio = best_sharpe->second;
                }
                
                // 找到收益最高的组合
                if (!return_values.empty()) {
                    auto best_return = std::max_element(
                        return_values.begin(), return_values.end(),
                        [](const auto& a, const auto& b) { return a.second < b.second; });
                    stats.best_return_portfolio = best_return->first;
                    stats.best_annualized_return = best_return->second;
                }
                
                // 找到回撤最小的组合
                if (!max_dd_values.empty()) {
                    auto best_drawdown = std::min_element(
                        max_dd_values.begin(), max_dd_values.end(),
                        [](const auto& a, const auto& b) { return a.second < b.second; });
                    stats.best_drawdown_portfolio = best_drawdown->first;
                    stats.best_max_drawdown = best_drawdown->second;
                }
            }
        }
        
        stats.updated_at = get_current_time();
        spdlog::info("Task statistics loaded for task_id: {}, {} portfolios, {} results",
                    task_id, stats.portfolio_count, stats.result_count);
        
        pool_.return_back(conn);
        return stats;
        
    } catch (const std::exception& e) {
        pool_.return_back(conn);
        spdlog::error("Get task statistics error for task_id {}: {}", task_id, e.what());
        return stats;
    }
}

