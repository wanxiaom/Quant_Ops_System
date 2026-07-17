// mysql_storage_manager.h
#pragma once
#include <memory>
#include <vector>
#include <map>
#include <string>
#include <chrono>
#include <mutex>
#include <optional>
#include <functional>
#include <Eigen/Dense>

#include "mysql.hpp"
#include "connection_pool.hpp"
#include "dbng.hpp"
#include "model/backtest_models.hpp"
#include <spdlog/spdlog.h>


using TimePoint = std::chrono::system_clock::time_point;


struct PeriodReturnDetail {
    double nav_end;                          // 调仓期结束时净资产
    double gross_return;                     // 区间累计毛收益
    double net_return;                       // 区间累计净收益
    Eigen::VectorXd gross_daily_returns;     // 每日毛收益
    Eigen::VectorXd net_daily_returns;       // 每日净收益
    double turnover_rate;                     // 单边换手率
    double cost_ratio;                        // 费用比率（相对于期初净值）
    std::vector<TimePoint> dates;             // 对应的日期序列
    std::map<std::string, double> final_weights; // 调仓期结束时的实际权重
};

struct HoldingPeriodResult {
    std::vector<double> gross_daily;
    std::vector<double> net_daily;  // 等于 gross_daily（无交易）
    std::map<std::string, double> final_weights;
};



// 回测结果
struct BacktestResult {
    int64_t portfolio_id;                              // 组合ID

    std::string portfolio_name;                // 组合名称
    std::string display_name;                  // 显示名称
    std::string strategy_type;                 // 策略类型
	std::vector<std::chrono::system_clock::time_point> alltrading_dates;   // 所有交易日序列
    std::vector<std::chrono::system_clock::time_point> trade_dates;        // 调仓日序列
    
    // 收益数据,存储在period_returns表中
    Eigen::VectorXd period_returns;            // 调仓期收益（考虑交易成本）
    Eigen::VectorXd period_returns_bc;         // 调仓期收益（不考虑交易成本）
	
    Eigen::VectorXd benchmark_returns;         // 基准调仓期收益
    Eigen::VectorXd active_returns;            // 调仓期主动收益
    Eigen::VectorXd turnover_rates;            // 调仓期日频换手率

    Eigen::VectorXd daily_returns;             // 日频收益
	Eigen::VectorXd daily_active_returns;      // 日频超额收益
    Eigen::VectorXd benchmark_daily_returns;   // 基准日频收益

    
    // 持仓历史
    std::vector<std::map<std::string, double>> weights_history;
    
    // 绩效指标
    PerformanceSummary summary;                // 绩效汇总
    std::vector<YearlyStatistic> yearly_stats; // 年度统计
    std::vector<MonthlyReturn> monthly_stats;  // 月度收益
};



// 回测任务统计信息结构体
struct BacktestTaskStats {
    int64_t task_id = 0;                      // 任务ID
    std::string task_name;                    // 任务名称
    std::string status;                       // 任务状态
    std::string created_at;                   // 创建时间
    std::string updated_at;                   // 更新时间
    
    // 数量统计
    int64_t portfolio_count = 0;              // 组合数量
    int64_t result_count = 0;                 // 结果数量
    int64_t daily_returns_count = 0;          // 日收益率数据量
    int64_t period_returns_count = 0;         // 调仓期收益数据量
    int64_t weights_count = 0;                // 持仓权重数据量
    int64_t summary_count = 0;                // 绩效汇总数据量
    int64_t running_days = 0;                 // 运行天数
    
    // 平均绩效指标
    double avg_sharpe_ratio = 0.0;            // 平均夏普比率
    double avg_max_drawdown = 0.0;            // 平均最大回撤
    double avg_annualized_return = 0.0;       // 平均年化收益率
    double avg_annualized_vol = 0.0;          // 平均年化波动率
    double avg_information_ratio = 0.0;       // 平均信息比率
    double avg_alpha = 0.0;                   // 平均Alpha
    double avg_beta = 0.0;                    // 平均Beta
    double avg_win_rate = 0.0;                // 平均胜率
    double avg_turnover = 0.0;                // 平均换手率
    
    // 最佳组合
    std::string best_sharpe_portfolio;        // 夏普最高组合
    double best_sharpe_ratio = 0.0;           // 最高夏普比率
    std::string best_return_portfolio;        // 收益最高组合
    double best_annualized_return = 0.0;      // 最高年化收益
    std::string best_drawdown_portfolio;      // 回撤最小组合
    double best_max_drawdown = 0.0;           // 最小最大回撤
    
    // 转换为JSON
    json to_json() const {
        json j;
        j["task_id"] = task_id;
        j["task_name"] = task_name;
        j["status"] = status;
        j["created_at"] = created_at;
        j["updated_at"] = updated_at;
        
        // 数量统计
        j["portfolio_count"] = portfolio_count;
        j["result_count"] = result_count;
        j["daily_returns_count"] = daily_returns_count;
        j["period_returns_count"] = period_returns_count;
        j["weights_count"] = weights_count;
        j["summary_count"] = summary_count;
        j["running_days"] = running_days;
        
        // 平均绩效指标
        j["avg_sharpe_ratio"] = avg_sharpe_ratio;
        j["avg_max_drawdown"] = avg_max_drawdown;
        j["avg_annualized_return"] = avg_annualized_return;
        j["avg_annualized_vol"] = avg_annualized_vol;
        j["avg_information_ratio"] = avg_information_ratio;
        j["avg_alpha"] = avg_alpha;
        j["avg_beta"] = avg_beta;
        j["avg_win_rate"] = avg_win_rate;
        j["avg_turnover"] = avg_turnover;
        
        // 最佳组合
        j["best_sharpe_portfolio"] = best_sharpe_portfolio;
        j["best_sharpe_ratio"] = best_sharpe_ratio;
        j["best_return_portfolio"] = best_return_portfolio;
        j["best_annualized_return"] = best_annualized_return;
        j["best_drawdown_portfolio"] = best_drawdown_portfolio;
        j["best_max_drawdown"] = best_max_drawdown;
        
        return j;
    }
};



class MySQLStorageManager {
public:
    MySQLStorageManager(connection_pool<dbng<mysql>>& pool);
    ~MySQLStorageManager(){}

	 connection_pool<dbng<mysql>>& getConnectionPool() { return pool_; }
	
    // 回测任务管理
    int64_t create_backtest_task(const BacktestTask& task);
    bool update_backtest_task(const BacktestTask& task);
    bool update_task_status(int64_t task_id, const std::string& status);
    BacktestTask get_backtest_task(int64_t task_id);
    std::vector<BacktestTask> get_backtest_tasks(int limit = 100, 
                                                int offset = 0);

	// 根据ID获取策略
	std::optional<StrategyConfig> getStrategyById(int id);

												
    // 组合信息管理
    int64_t create_portfolio(const Portfolio& portfolio);
    Portfolio get_portfolio(int64_t portfolio_id);
    std::vector<Portfolio> get_portfolios_by_task(int64_t task_id);

	int64_t store_portfolio(const Portfolio& portfolio);
    
    // 收益率数据管理
    bool store_daily_returns(const std::vector<DailyReturn>& returns);
    std::vector<DailyReturn> get_daily_returns(int64_t portfolio_id,
                                             const std::optional<std::string>& start_date = std::nullopt,
                                             const std::optional<std::string>& end_date = std::nullopt);
    
    // 调仓期收益率管理
    bool store_period_returns(const std::vector<PeriodReturn>& returns);
    std::vector<PeriodReturn> get_period_returns(int64_t portfolio_id);
    
    // 绩效指标管理
    bool store_performance_summary(const PerformanceSummary& summary);
	bool store_performance_summaries(const std::vector<PerformanceSummary>& summaries);
	
	// 获取特定组合的绩效汇总数据（按开始日期排序）
	std::vector<PerformanceSummary> get_performance_summaries(int64_t portfolio_id);
	
	// 获取特定组合和日期范围的绩效汇总数据
	std::vector<PerformanceSummary> get_performance_summaries_by_date_range(
		int64_t portfolio_id, 
		const std::string& start_date, 
		const std::string& end_date);
	
	// 获取特定组合的最新绩效汇总数据
	std::optional<PerformanceSummary> get_latest_performance_summary(int64_t portfolio_id);
	
	// 更新绩效汇总数据（基于metric_id）
	bool update_performance_summary(const PerformanceSummary& summary);
	
	// 删除特定组合的所有绩效汇总数据
	bool delete_performance_summaries(int64_t portfolio_id);

    
    // 分年统计数据管理
    bool store_yearly_statistics(const std::vector<YearlyStatistic>& stats);
    std::vector<YearlyStatistic> get_yearly_statistics(int64_t portfolio_id);
    
    // 月度收益率管理
    bool store_monthly_returns(const std::vector<MonthlyReturn>& returns);
    std::vector<MonthlyReturn> get_monthly_returns(int64_t portfolio_id);
    
    // 相关性矩阵管理
    bool store_correlation_matrix(const std::vector<CorrelationMatrix>& correlations);
    std::vector<CorrelationMatrix> get_correlation_matrix(int64_t task_id);
    
    // 自定义指数管理
    int64_t create_custom_index(const CustomIndex& index);
    bool store_custom_index_returns(const std::vector<CustomIndexReturn>& returns);
    std::vector<CustomIndexReturn> get_custom_index_returns(int64_t index_id,
                                                          const std::optional<std::string>& start_date = std::nullopt,
                                                          const std::optional<std::string>& end_date = std::nullopt);
    
    // 持仓权重管理
    bool store_portfolio_weights(const std::vector<PortfolioWeight>& weights);
    std::vector<PortfolioWeight> get_portfolio_weights(int64_t portfolio_id,
                                                     const std::optional<std::string>& rebalance_date = std::nullopt);
    
    // 合成权重管理
    bool store_composite_weights(const std::vector<CompositeWeight>& weights);
    std::vector<CompositeWeight> get_composite_weights(int64_t task_id,
                                                     const std::optional<std::string>& rebalance_date = std::nullopt);
    
    // 交易明细管理
    bool store_trade_details(const std::vector<TradeDetail>& trades);
    std::vector<TradeDetail> get_trade_details(int64_t portfolio_id,
                                             const std::optional<std::string>& rebalance_date = std::nullopt);
    
    // 持仓明细管理
    bool store_holding_details(const std::vector<HoldingDetail>& holdings);
    std::vector<HoldingDetail> get_holding_details(int64_t portfolio_id,
                                                 const std::optional<std::string>& date = std::nullopt);
	
	// 获取任务所有结果（按组合名称分类）
	std::map<std::string, BacktestResult> get_task_results(int64_t task_id);
	
	// 获取任务所有结果（详细版本，包含所有组合的绩效数据）
	std::map<std::string, std::vector<PerformanceSummary>> get_task_detailed_results(int64_t task_id);
	
	// 取消回测任务
	bool cancel_backtest_task(int64_t task_id);
	
	// 删除回测任务及相关数据（级联删除）
	bool delete_backtest_task(int64_t task_id);
	
	// 获取任务统计信息
	BacktestTaskStats get_task_statistics(int64_t task_id);

	template<typename T>
	bool batch_insert(const std::vector<T>& records) {
	    auto db = pool_.get();
	    if (!db)
		{
			spdlog::warn("batch_insert Failed to get database connection");
			return false;
		}
	    try {
	        bool result = db->insert(records) > 0;
	        pool_.return_back(db);   // 正常归还
	        return result;
	    } catch (const std::exception& e) {
	        pool_.return_back(db);   // 异常也要归还
	        spdlog::error("batch_insert Error: {}", e.what());
	        return false;
	    }
	}

	template<typename T>
	std::vector<T> query(const std::string& condition = "") {
	    std::lock_guard<std::mutex> lock(mutex_);
	    auto db = pool_.get();
	    if (!db)
		{
			spdlog::warn("query Failed to get database connection");
            return {};
        }
	    try {
	        auto result = db->query<T>(condition);
	        pool_.return_back(db);
	        return result;
	    } catch (const std::exception& e) {
	        pool_.return_back(db);
	        spdlog::error("query Error: {}", e.what());
	        return {};
	    }
	}
	
    
private:
    connection_pool<dbng<mysql>>& pool_;
    std::mutex mutex_;
    
    // 初始化数据库表
    bool create_tables();

};

