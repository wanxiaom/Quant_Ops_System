#pragma once
#include <Eigen/Dense>

#include "backtest_manager/dolphin/dolphin_db_connector.hpp"



namespace PortfolioBacktest {

struct BatchStockData {
    std::vector<std::string> symbols;
    std::vector<std::chrono::system_clock::time_point> dates;
    Eigen::MatrixXd close_prices;        // 股票×日期
    Eigen::MatrixXd turnover_amounts;    // 股票×日期
    Eigen::MatrixXd turnover_rates;      // 股票×日期
};


using TimePoint = std::chrono::system_clock::time_point;

struct PriceRecord {
    double prev_close = std::numeric_limits<double>::quiet_NaN();
    double close = std::numeric_limits<double>::quiet_NaN();
    double open = std::numeric_limits<double>::quiet_NaN();
    double vwap = std::numeric_limits<double>::quiet_NaN();
};


struct TimePointHash {
    std::size_t operator()(const TimePoint& tp) const {
        return std::hash<long long>()(tp.time_since_epoch().count());
    }
};

using TimePoint = std::chrono::system_clock::time_point;

// DolphinDB数据查询管理器
class DolphinDBQueryManager {
public:
    DolphinDBQueryManager(std::shared_ptr<DolphinDBConnector> connector);
    ~DolphinDBQueryManager() = default;

    BatchStockData query_batch_stock_data(
    const std::vector<std::string>& symbols,
    const std::chrono::system_clock::time_point& start_date,
    const std::chrono::system_clock::time_point& end_date);


	// 批量查询多只股票、多个交易日、多种价格类型
	std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>>
	query_batch_price_data(
	    const std::vector<std::string>& symbols,
	    const std::vector<TimePoint>& dates);

	// 批量查询多只股票、多个交易日、多种价格类型
	std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>>
	query_batch_index_price_data(
	    const std::vector<std::string>& symbols,
	    const std::vector<TimePoint>& dates);

	// 获取指定交易日的前一个交易日
	TimePoint get_previous_trading_date(const TimePoint& date);


	TimePoint get_next_trading_date(const TimePoint& date);
	
    // 查询交易日历
    std::vector<TimePoint> query_trading_dates(
        const TimePoint& start_date,
        const TimePoint& end_date);
    
    // 查询基准收益率
    Eigen::VectorXd query_benchmark_returns(
        const std::string& benchmark_code,
        const std::vector<TimePoint>& trade_dates,
        const std::string& price_type = "close");
    
    // 查询基准日频收益
	std::pair<Eigen::VectorXd, std::vector<TimePoint>> query_benchmark_daily_returns(
	const std::string& symbol,
	const TimePoint& start_date,
	const TimePoint& end_date,
	const std::string& price_type = "close");
		

	std::pair<std::map<std::string, size_t>, std::map<TimePoint, size_t>> 
	create_matrix_index(
	    const std::vector<std::string>& symbols,
	    const std::vector<TimePoint>& dates);
	
    // 查询股票池
    std::vector<std::string> query_universe_stocks(
        const std::string& universe_type,
        const TimePoint& date);
    
    // 查询价格数据（批量）
    Eigen::MatrixXd query_price_data(
        const std::vector<std::string>& symbols,
        const std::vector<TimePoint>& dates,
        const std::string& price_type = "close");

    Eigen::MatrixXd query_index_price_data(
        const std::vector<std::string>& symbols,
        const std::vector<TimePoint>& dates,
        const std::string& price_type = "close");
		
    
    // 查询成交额数据（批量）
    Eigen::MatrixXd query_turnover_amount_data(
        const std::vector<std::string>& symbols,
        const std::vector<TimePoint>& dates);
    
    // 查询换手率数据（批量）
    Eigen::MatrixXd query_turnover_rate_data(
        const std::vector<std::string>& symbols,
        const std::vector<TimePoint>& dates);
    
    // 查询股票因子暴露（行业等）
    std::map<std::string, std::map<std::string, double>> query_factor_exposure(
        const std::vector<std::string>& symbols,
        const TimePoint& date);
    
    // 查询股票基本信息
    std::map<std::string, std::string> query_stock_info(
        const std::vector<std::string>& symbols,
        const TimePoint& date);


    std::shared_ptr<DolphinDBConnector> getDolphinConnector(){return connector_;}

	// 获取基准指数成分股权重（假设表 index_weights）
    std::map<std::string, double> get_benchmark_weights(
        const std::string& benchmark_code,
        const TimePoint& date,
        double benchmark_position = 1.0);

    // 获取因子暴露矩阵：风格因子 + 行业哑变量
    // 返回 pair<矩阵, 因子名称列表>，矩阵行数=股票数，列数=因子数（风格因子数+行业数）
    std::pair<Eigen::MatrixXd, std::vector<std::string>> get_factor_exposures_matrix(
        const std::vector<std::string>& symbols,
        const TimePoint& date);

    // 获取行业分类（基于 industry 表）
    std::map<std::string, std::string> get_industry_classification(
        const std::vector<std::string>& symbols,
        const TimePoint& date);

    // 获取指定区间个股总收益率
    Eigen::VectorXd get_stock_returns(
        const std::vector<std::string>& symbols,
        const TimePoint& start_date,
        const TimePoint& end_date,
        bool is_last_period,
        const std::string& price_type = "close");

    // 获取所有可用的行业列表（从 industry 表或配置）
    std::vector<std::string> get_all_industries();

private:
    std::shared_ptr<DolphinDBConnector> connector_;
    
    // 日期转换
    std::string timepoint_to_string(const TimePoint& tp);
    TimePoint string_to_timepoint(const std::string& str);
    
    // 构建符号列表字符串
    std::string build_symbol_list(const std::vector<std::string>& symbols);
    
    // 构建日期范围字符串
    std::string build_date_range(const TimePoint& start, const TimePoint& end);
    
    // 解析查询结果
    Eigen::MatrixXd parse_matrix_result(	const std::vector<std::string>& symbols, const DolphinDBTable& result,
                                       int symbol_col, int date_col, int value_col);


	Eigen::MatrixXd parse_matrix_result_ordered(
		const std::vector<std::string>& symbols,
		const std::vector<TimePoint>& dates,
		const DolphinDBTable& result,
		int symbol_col, int date_col, int value_col);



    // 辅助：将股票列表的行业映射转换为行业哑变量矩阵
    Eigen::MatrixXd industry_dummy_matrix(
        const std::vector<std::string>& symbols,
        const std::map<std::string, std::string>& industry_map,
        const std::vector<std::string>& all_industries);

};

} // namespace PortfolioBacktest