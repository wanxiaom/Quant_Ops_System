//dolphin_query_manager.cpp
#include <spdlog/spdlog.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include "utils.hpp"
#include "dolphin_query_manager.h"

namespace PortfolioBacktest {

DolphinDBQueryManager::DolphinDBQueryManager(
    std::shared_ptr<DolphinDBConnector> connector)
    : connector_(connector) {}


// 获取指定交易日的前一个交易日
TimePoint DolphinDBQueryManager::get_previous_trading_date(const TimePoint& date) {
    try {
        std::string date_str = timepoint_to_string(date);
        
        // 查询小于给定日期的最大交易日
        std::string query = "select max(date) as prev_date from loadTable(\"dfs://basic_info\", \"trade_date\") "
                           "where date < " + date_str;
        
        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            spdlog::warn("No previous trading date found for {}", date_str);
            return {}; // 返回空时间点或默认值
        }
        
        return result.date_data[0][0];
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying previous trading date: {}", e.what());
        return {};
    }
}

TimePoint DolphinDBQueryManager::get_next_trading_date(const TimePoint& date) {
    try {
        std::string date_str = timepoint_to_string(date);
        
        // 查询大于给定日期的最小交易日
        std::string query = "select min(date) as next_date from loadTable(\"dfs://basic_info\", \"trade_date\") "
                           "where date > " + date_str;
        
        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            spdlog::warn("No next trading date found for {}", date_str);
            return {}; // 返回空时间点
        }
        
        return result.date_data[0][0];
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying next trading date: {}", e.what());
        return {};
    }
}

std::vector<TimePoint> DolphinDBQueryManager::query_trading_dates(
    const TimePoint& start_date,
    const TimePoint& end_date) {
    
    std::vector<TimePoint> dates;
    
    try {
        std::string start_str = timepoint_to_string(start_date);
        std::string end_str = timepoint_to_string(end_date);

        std::string query = "select date from loadTable(\"dfs://basic_info\", \"trade_date\") "
                           "where date >= " + start_str + " and date <= " + end_str + 
                           " order by date";

        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            spdlog::warn("No trading dates found between {} and {}", 
                        start_str, end_str);
            return dates;
        }
        
        for (int i = 0; i < result.row_count; ++i) {
            dates.push_back(result.date_data[0][i]);
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying trading dates: {}", e.what());
    }
    
    return dates;
}

std::vector<std::string> DolphinDBQueryManager::query_universe_stocks(
    const std::string& universe_type,
    const TimePoint& date) {
    
    std::vector<std::string> stocks;
	std::string date_str = timepoint_to_string(date);
    
    try {
        // 将TimePoint转换为字符串格式，假设格式为"yyyy.MM.dd"
        std::string datestr = timepoint_to_string(date);
        
        // 构建查询语句
        // 假设status表有symbol字段和status_type字段
        std::string query = "select code from loadTable(\"dfs://stock_info\", \"tradable\") "
            "where date = " + datestr + " order by code";
			
        spdlog::debug("Querying universe stocks: {}", query);
        
        auto result = connector_->execute_query(query);
        if (!result.success) {
            spdlog::warn("Failed to query universe stocks for type '{}' on date {}", 
                        universe_type, datestr);
            return stocks;
        }
        
        if (result.row_count == 0) {
            spdlog::warn("No stocks found for universe type '{}' on date {}", 
                         universe_type, datestr);
            return stocks;
        }
        
        // 提取股票代码
        // 假设symbol列是字符串类型，存储在string_data中
        if (!result.string_data.empty() && !result.string_data[0].empty()) {
            stocks = result.string_data[0];  // 第一列的所有行
            spdlog::debug("Found {} stocks for universe type '{}' on date {}", 
                         stocks.size(), universe_type, datestr);
        } else {
            spdlog::warn("No string data found in query result for universe stocks");
        }
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying universe stocks: {}", e.what());
    }
    
    return stocks;
}
#if 0
Eigen::MatrixXd DolphinDBQueryManager::query_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates,
    const std::string& price_type) {
    
    if (symbols.empty() || dates.empty()) {
        return Eigen::MatrixXd();
    }
    
    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);
        
        std::string query = "select code, date," + price_type + " as price "
            "from loadTable(\"dfs://instrument_info\", \"ex_quote\") "
            "where code in" + symbol_list + " and date between " + start_str + " and " + end_str + " order by code, date";
        
        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            return Eigen::MatrixXd();
        }
        
        return parse_matrix_result(symbols, result, 0, 1, 2);
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying price data: {}", e.what());
        return Eigen::MatrixXd();
    }
}

Eigen::MatrixXd DolphinDBQueryManager::query_index_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates,
    const std::string& price_type) {
    
    if (symbols.empty() || dates.empty()) {
        return Eigen::MatrixXd();
    }
    
    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);
        
        std::string query = "select code, date," + price_type + " as price "
            "from loadTable(\"dfs://market_data_1d\", \"index\") "
            "where code in" + symbol_list + " and date between " + start_str + " and " + end_str + " order by code, date";
        
        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            return Eigen::MatrixXd();
        }

        return parse_matrix_result(symbols, result, 0, 1, 2);
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying price data: {}", e.what());
        return Eigen::MatrixXd();
    }
}
#endif

Eigen::MatrixXd DolphinDBQueryManager::parse_matrix_result_ordered(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates,
    const DolphinDBTable& result,
    int symbol_col, int date_col, int value_col)
{
    // 符号索引：按传入 symbols 顺序固定
    std::map<std::string, size_t> symbol_index;
    for (size_t i = 0; i < symbols.size(); ++i)
        symbol_index[symbols[i]] = i;

    // 日期索引：按传入 dates 顺序固定
    std::map<TimePoint, size_t> date_index;
    for (size_t i = 0; i < dates.size(); ++i)
        date_index[dates[i]] = i;

    Eigen::MatrixXd matrix(symbols.size(), dates.size());
    matrix.setConstant(std::numeric_limits<double>::quiet_NaN());

    for (int i = 0; i < result.row_count; ++i) {
        const std::string& sym = result.string_data[symbol_col][i];
        const TimePoint& dt = result.date_data[date_col][i];
        double val = result.numeric_data[value_col][i];

        auto it_sym = symbol_index.find(sym);
        auto it_dt = date_index.find(dt);
        if (it_sym != symbol_index.end() && it_dt != date_index.end()) {
            matrix(it_sym->second, it_dt->second) = val;
        }
    }
    return matrix;
}

Eigen::MatrixXd DolphinDBQueryManager::query_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates,
    const std::string& price_type)
{
    if (symbols.empty() || dates.empty())
        return Eigen::MatrixXd();

    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);

        std::string query = "select code, date, " + price_type + " as price "
            "from loadTable(\"dfs://instrument_info\", \"ex_quote\") "
            "where code in " + symbol_list + " and date between " + start_str + " and " + end_str + " "
            "order by code, date";

        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0)
            return Eigen::MatrixXd();

        return parse_matrix_result_ordered(symbols, dates, result, 0, 1, 2);
    } catch (const std::exception& e) {
        spdlog::error("Error querying price data: {}", e.what());
        return Eigen::MatrixXd();
    }
}

Eigen::MatrixXd DolphinDBQueryManager::query_index_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates,
    const std::string& price_type)
{
    if (symbols.empty() || dates.empty())
        return Eigen::MatrixXd();

    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);

        // 根据 price_type 构造查询的列表达式
        std::string price_expr;
        if (price_type == "vwap") {
            // 使用 open, high, low, close 的平均值作为 vwap
            price_expr = "(open + high + low + close) / 4.0";
        } else {
            // 否则直接使用传入的列名（如 "close"）
            price_expr = price_type;
        }

        std::string query = "select code, date, " + price_expr + " as price "
            "from loadTable(\"dfs://market_data_1d\", \"index\") "
            "where code in " + symbol_list + " and date between " + start_str + " and " + end_str + " "
            "order by code, date";

        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0)
            return Eigen::MatrixXd();

        // 返回的矩阵中第 3 列（索引 2）对应 price
        return parse_matrix_result_ordered(symbols, dates, result, 0, 1, 2);
    } catch (const std::exception& e) {
        spdlog::error("Error querying index price data: {}", e.what());
        return Eigen::MatrixXd();
    }
}
	

std::pair<Eigen::VectorXd, std::vector<TimePoint>> DolphinDBQueryManager::query_benchmark_daily_returns(
    const std::string& symbol,
    const TimePoint& start_date,
    const TimePoint& end_date,
    const std::string& price_type) {

    // 返回空向量对
    std::pair<Eigen::VectorXd, std::vector<TimePoint>> empty_result = {
        Eigen::VectorXd(), 
        std::vector<TimePoint>()
    };

    if (symbol.empty()) {
        spdlog::warn("Empty symbol provided to query_benchmark_daily_returns");
        return empty_result;
    }

    try {
        // 查询所有交易日期
        auto trading_dates = query_trading_dates(start_date, end_date);
        if (trading_dates.empty()) {
            spdlog::warn("No trading dates found between {} and {} for symbol {}", 
                        timepoint_to_string(start_date), 
                        timepoint_to_string(end_date),
                        symbol);
            return empty_result;
        }
        
        // 构建包含基准代码的向量
        std::vector<std::string> symbols = {symbol};
        
        // 查询基准价格数据
        auto price_matrix = query_index_price_data(symbols, trading_dates, price_type);
        
        // 检查价格数据是否有效
        if (price_matrix.rows() == 0 || price_matrix.cols() < 1) {
            spdlog::warn("No price data found for symbol {} between {} and {}", 
                        symbol,
                        timepoint_to_string(start_date), 
                        timepoint_to_string(end_date));
            return empty_result;
        }
        
        // 提取价格数据（假设每行对应一个交易日）
        Eigen::VectorXd prices = price_matrix.row(0);
        
        // 检查价格数据行数是否与交易日数量一致
        if (prices.size() != static_cast<int>(trading_dates.size())) {
            spdlog::warn("Price data count ({}) doesn't match trading dates count ({}) for symbol {}", 
                        prices.size(), trading_dates.size(), symbol);
            return empty_result;
        }
        
        // 计算日收益率
        // 收益率数量 = 价格数量 - 1
        int n_returns = prices.size() - 1;
        if (n_returns <= 0) {
            spdlog::warn("Insufficient price data (only {} point) to calculate returns for symbol {}", 
                        prices.size(), symbol);
            return empty_result;
        }
        
        Eigen::VectorXd returns(n_returns);
        std::vector<TimePoint> return_dates;
        return_dates.reserve(n_returns);
        
        for (int i = 1; i < prices.size(); ++i) {
            // 检查价格是否有效（避免除零或负值）
            if (prices[i-1] <= 0) {
                spdlog::warn("Invalid price {} at index {} for symbol {}", 
                            prices[i-1], i-1, symbol);
                returns[i-1] = 0.0;  // 或者使用其他默认值
            } else {
                returns[i-1] = (prices[i] - prices[i-1]) / prices[i-1];
            }
            return_dates.push_back(trading_dates[i]);
        }
        
        return {returns, return_dates};
        
    } catch (const std::exception& e) {
        spdlog::error("Error querying benchmark daily returns for symbol {}: {}", 
                     symbol, e.what());
        return empty_result;
    }
}


std::pair<std::map<std::string, size_t>, std::map<TimePoint, size_t>> 
DolphinDBQueryManager::create_matrix_index(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates) {
	
    // 收集唯一的符号和日期
    std::map<std::string, size_t> symbol_index;
    std::map<TimePoint, size_t> date_index;
    
    for (size_t i = 0; i < symbols.size(); ++i) {
        const std::string& symbol = symbols[i];
        if (symbol_index.find(symbol) == symbol_index.end()) {
            symbol_index[symbol] = symbol_index.size();
        }
    }

    for (size_t i = 0; i < dates.size(); ++i) {
        const TimePoint& date = dates[i];
        if (date_index.find(date) == date_index.end()) {
            date_index[date] = date_index.size();
        }
    }
    
    return {symbol_index, date_index};
}

// 解决查询symbol列表和实际结果数量不一致的问题，使用原始symbol列表构造索引
Eigen::MatrixXd DolphinDBQueryManager::parse_matrix_result(
	const std::vector<std::string>& symbols,
    const DolphinDBTable& result,
    int symbol_col, int date_col, int value_col) {
    

    auto [symbol_index, date_index] = create_matrix_index(
        symbols, 
        result.date_data[date_col]);
    
    // 创建矩阵
    Eigen::MatrixXd matrix(symbol_index.size(), date_index.size());
    matrix.setConstant(std::numeric_limits<double>::quiet_NaN());
    
    // 填充数据
    for (int i = 0; i < result.row_count; ++i) {
        const std::string& symbol = result.string_data[symbol_col][i];
        const TimePoint& date = result.date_data[date_col][i];
        double value = result.numeric_data[value_col][i];

        size_t row = symbol_index.at(symbol);
        size_t col = date_index.at(date);

        matrix(row, col) = value;
		
    }

    return matrix;
}

std::string DolphinDBQueryManager::timepoint_to_string(const TimePoint& tp) {
    auto tt = std::chrono::system_clock::to_time_t(tp);
    std::tm tm = *std::localtime(&tt);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y.%m.%d");
    return oss.str();
}

std::string DolphinDBQueryManager::build_symbol_list(
    const std::vector<std::string>& symbols) {
    
    if (symbols.empty()) return "[]";
    
    std::string result = "[";
    
    for (size_t i = 0; i < symbols.size(); ++i) {
        result += "'" + symbols[i] + "'";
        if (i < symbols.size() - 1) {
            result += ",";
        }
    }
    
    result += "]";
    return result;
}

BatchStockData DolphinDBQueryManager::query_batch_stock_data(
    const std::vector<std::string>& symbols,
    const std::chrono::system_clock::time_point& start_date,
    const std::chrono::system_clock::time_point& end_date) {
    
    BatchStockData batch_data;
    if (symbols.empty()) return batch_data;
    
    std::string start_str = date_to_string(start_date);
    std::string end_str = date_to_string(end_date);
    
    // 构建DolphinDB查询
    std::vector<std::string> quoted_symbols;
    for (const auto& symbol : symbols) {
        quoted_symbols.push_back("'" + symbol + "'");
    }

	std::string symbol_list = "[";
	for (size_t i = 0; i < quoted_symbols.size(); ++i) {
	    symbol_list += quoted_symbols[i];
	    if (i != quoted_symbols.size() - 1) {
	        symbol_list += ",";
	    }
	}
	symbol_list += "]";

	std::string query = "select s.code as symbol, s.date, s.close, s.total_turnover, t.turnover"
						" from loadTable(\"dfs://market_data_1d\", \"stock\") as s"
						" left join loadTable(\"dfs://stock_info\", \"turnover\") as t"
						" on s.code = t.code and s.date = t.date"
						" where s.code in " + symbol_list +
						" and s.date between " + start_str +" and " + end_str + 
						" order by s.code, s.date";
						
    auto table = connector_->execute_query(query);
	//std::cout<<"query_batch_stock_data query:"<<query<<std::endl;
    
    if (!table.success || table.row_count == 0) {
        spdlog::warn("No data returned for batch stock query");
        return batch_data;
    }
    
    // 组织数据
    std::map<std::string, std::vector<double>> close_map;
    std::map<std::string, std::vector<double>> amount_map;
    std::map<std::string, std::vector<double>> rate_map;
    std::map<std::chrono::system_clock::time_point, size_t> date_index_map;
    
    // 第一遍：收集唯一日期
    for (int i = 0; i < table.row_count; ++i) {
        auto date = table.date_data[1][i];
        date_index_map[date] = 0; // 占位，稍后排序
    }
    
    // 排序日期
    std::vector<std::chrono::system_clock::time_point> unique_dates;
    for (const auto& entry : date_index_map) {
        unique_dates.push_back(entry.first);
    }
    std::sort(unique_dates.begin(), unique_dates.end());
    
    // 更新日期索引映射
    for (size_t i = 0; i < unique_dates.size(); ++i) {
        date_index_map[unique_dates[i]] = i;
    }
    
    batch_data.dates = unique_dates;
    batch_data.symbols = symbols;
    
    // 初始化矩阵
    int n_symbols = symbols.size();
    int n_dates = unique_dates.size();
    
    batch_data.close_prices = Eigen::MatrixXd::Constant(n_symbols, n_dates, std::numeric_limits<double>::quiet_NaN());
    batch_data.turnover_amounts = Eigen::MatrixXd::Constant(n_symbols, n_dates, std::numeric_limits<double>::quiet_NaN());
    batch_data.turnover_rates = Eigen::MatrixXd::Constant(n_symbols, n_dates, std::numeric_limits<double>::quiet_NaN());
    
    // 第二遍：填充数据
    std::map<std::string, size_t> symbol_index_map;
    for (size_t i = 0; i < symbols.size(); ++i) {
        symbol_index_map[symbols[i]] = i;
    }
    
    for (int i = 0; i < table.row_count; ++i) {
        std::string symbol = table.string_data[0][i];
        auto date = table.date_data[1][i];
        double close = table.numeric_data[2][i];
        double amount = table.numeric_data[3][i];
        double rate = table.numeric_data[4][i];
        
        auto sym_it = symbol_index_map.find(symbol);
        if (sym_it == symbol_index_map.end()) continue;
        
        auto date_it = date_index_map.find(date);
        if (date_it == date_index_map.end()) continue;
        
        size_t sym_idx = sym_it->second;
        size_t date_idx = date_it->second;
        
        batch_data.close_prices(sym_idx, date_idx) = close;
        batch_data.turnover_amounts(sym_idx, date_idx) = amount;
        batch_data.turnover_rates(sym_idx, date_idx) = rate;
    }
    
    return batch_data;
}

// 批量查询多只股票、多个交易日、多种价格类型
std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>>
DolphinDBQueryManager::query_batch_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates)
{
    std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>> result;
    if (symbols.empty() || dates.empty())
        return result;

    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);

        // 一次性查询 close, open, vwap
        std::string query = "select code, date, pre_close, close, open, vwap "
            "from loadTable(\"dfs://instrument_info\", \"ex_quote\") "
            "where code in " + symbol_list + " and date between " + start_str + " and " + end_str + " "
            "order by code, date";

        auto db_result = connector_->execute_query(query);
        if (!db_result.success || db_result.row_count == 0)
            return result;

        // 解析结果，填充到 map 结构
        for (int i = 0; i < db_result.row_count; ++i) {
            const std::string& symbol = db_result.string_data[0][i]; // code
            const TimePoint& date = db_result.date_data[1][i];       // date
            double prev_close = db_result.numeric_data[2][i];        // prev_close
            double close = db_result.numeric_data[3][i];            // close
            double open = db_result.numeric_data[4][i];             // open
            double vwap = db_result.numeric_data[5][i];             // vwap

            result[symbol][date] = {prev_close, close, open, vwap};
        }
    } catch (const std::exception& e) {
        spdlog::error("Error in query_batch_price_data: {}", e.what());
    }
    return result;
}

std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>>
DolphinDBQueryManager::query_batch_index_price_data(
    const std::vector<std::string>& symbols,
    const std::vector<TimePoint>& dates)
{
    std::unordered_map<std::string, std::unordered_map<TimePoint, PriceRecord, TimePointHash>> result;
    if (symbols.empty() || dates.empty())
        return result;

    try {
        std::string start_str = timepoint_to_string(dates.front());
        std::string end_str = timepoint_to_string(dates.back());
        std::string symbol_list = build_symbol_list(symbols);

        // 修改查询：增加 high, low 列，移除 volume, total_turnover（不再需要）
        std::string query = "select code, date, prev_close, close, open, high, low "
            "from loadTable(\"dfs://market_data_1d\", \"index\") "
            "where code in " + symbol_list + " and date between " + start_str + " and " + end_str + " "
            "order by code, date";

        auto db_result = connector_->execute_query(query);
        if (!db_result.success || db_result.row_count == 0)
            return result;

        // 解析结果，填充到 map 结构
        for (int i = 0; i < db_result.row_count; ++i) {
            const std::string& symbol = db_result.string_data[0][i]; // code
            const TimePoint& date = db_result.date_data[1][i];       // date
            double prev_close = db_result.numeric_data[2][i];        // prev_close
            double close = db_result.numeric_data[3][i];            // close
            double open  = db_result.numeric_data[4][i];            // open
            double high  = db_result.numeric_data[5][i];            // high
            double low   = db_result.numeric_data[6][i];            // low

            // 计算 vwap 为四个价格的算术平均值
            double vwap = (open + high + low + close) / 4.0;
            result[symbol][date] = {prev_close, close, open, vwap};
        }
    } catch (const std::exception& e) {
        spdlog::error("Error in query_batch_index_price_data: {}", e.what());
    }
    return result;
}

std::map<std::string, double> DolphinDBQueryManager::get_benchmark_weights(
    const std::string& benchmark_code,
    const TimePoint& date,
    double benchmark_position)
{
    std::map<std::string, double> weights;
    if (benchmark_code.empty()) return weights;

    try {
        std::string date_str = timepoint_to_string(date);
        std::string query = "select stock_code, weight from loadTable(\"dfs://basic_info\", \"index_weights\") "
                            "where code = '" + benchmark_code + "' and date = " + date_str +
                            " order by stock_code";
        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            spdlog::warn("No benchmark weights found for {} at {}", benchmark_code, date_str);
            return weights;
        }
        for (int i = 0; i < result.row_count; ++i) {
            std::string stock = result.string_data[0][i];
            double w = result.numeric_data[1][i];
            weights[stock] = w * benchmark_position;
        }
    } catch (const std::exception& e) {
        spdlog::error("Error in get_benchmark_weights: {}", e.what());
    }
    return weights;
}

std::map<std::string, std::string> DolphinDBQueryManager::get_industry_classification(
    const std::vector<std::string>& symbols,
    const TimePoint& date)
{
    std::map<std::string, std::string> result;
    if (symbols.empty()) return result;

    try {
        std::string date_str = timepoint_to_string(date);
        std::string symbol_list = build_symbol_list(symbols);
        std::string query = "select code, industry from loadTable(\"dfs://stock_info\", \"industry\") "
                            "where code in " + symbol_list + " and date = " + date_str;
        auto db_result = connector_->execute_query(query);
        if (!db_result.success || db_result.row_count == 0) {
            spdlog::warn("No industry data found for date {}", date_str);
            return result;
        }
        for (int i = 0; i < db_result.row_count; ++i) {
            std::string sym = db_result.string_data[0][i];
            std::string ind = db_result.string_data[1][i];
            result[sym] = ind;
        }
        // 对于未查到行业的股票，设为"未知行业"
        for (const auto& sym : symbols) {
            if (result.find(sym) == result.end()) result[sym] = "未知行业";
        }
    } catch (const std::exception& e) {
        spdlog::error("Error in get_industry_classification: {}", e.what());
    }
    return result;
}

std::vector<std::string> DolphinDBQueryManager::get_all_industries()
{
    std::vector<std::string> industries;
    try {
        // 查询 industry 表中所有不重复的行业值
        std::string query = "select distinct industry from loadTable(\"dfs://stock_info\", \"industry\")";
        auto result = connector_->execute_query(query);
        if (result.success && result.row_count > 0) {
            for (int i = 0; i < result.row_count; ++i) {
                industries.push_back(result.string_data[0][i]);
            }
        }
    } catch (const std::exception& e) {
        spdlog::error("Error in get_all_industries: {}", e.what());
    }
    // 若查询失败，返回预定义行业列表（根据实际业务调整）
    if (industries.empty()) {
        industries = {"银行", "非银金融", "房地产", "医药生物", "食品饮料", "电子", "计算机", 
                      "机械设备", "化工", "汽车", "有色金属", "交通运输", "公用事业", "建筑装饰",
                      "采掘", "钢铁", "家用电器", "纺织服装", "轻工制造", "商业贸易", "休闲服务",
                      "通信", "国防军工", "农林牧渔", "建筑材料", "电气设备", "传媒"};
    }
    return industries;
}


Eigen::MatrixXd DolphinDBQueryManager::industry_dummy_matrix(
    const std::vector<std::string>& symbols,
    const std::map<std::string, std::string>& industry_map,
    const std::vector<std::string>& all_industries)
{
    int n_sym = symbols.size();
    int n_ind = all_industries.size();
    Eigen::MatrixXd dummy = Eigen::MatrixXd::Zero(n_sym, n_ind);
    for (int i = 0; i < n_sym; ++i) {
        const std::string& sym = symbols[i];
        auto it = industry_map.find(sym);
        if (it != industry_map.end()) {
            std::string ind = it->second;
            auto ind_it = std::find(all_industries.begin(), all_industries.end(), ind);
            if (ind_it != all_industries.end()) {
                int col = std::distance(all_industries.begin(), ind_it);
                dummy(i, col) = 1.0;
            }
        }
    }
    return dummy;
}

std::pair<Eigen::MatrixXd, std::vector<std::string>> 
DolphinDBQueryManager::get_factor_exposures_matrix(
    const std::vector<std::string>& symbols,
    const TimePoint& date)
{
    std::pair<Eigen::MatrixXd, std::vector<std::string>> empty_result = {Eigen::MatrixXd(), {}};
    if (symbols.empty()) return empty_result;

    try {
        std::string date_str = timepoint_to_string(date);
        std::string symbol_list = build_symbol_list(symbols);

        // 风格因子列名（根据实际表字段，省略了可能不存在的列）
        std::vector<std::string> style_factors = {"momentum", "beta", "book_to_price", "earnings_yield",
                                                   "liquidity", "size", "residual_volatility", "non_linear_size"};
        // 构建查询，选择风格因子列
        std::string select_clause = "code";
        for (const auto& f : style_factors) select_clause += ", " + f;
        std::string query = "select " + select_clause + " from loadTable(\"dfs://stock_info\", \"factor_exposure\") "
                            "where code in " + symbol_list + " and date = " + date_str;

        auto result = connector_->execute_query(query);
        if (!result.success || result.row_count == 0) {
            spdlog::warn("No factor exposure data for date {}", date_str);
            return empty_result;
        }

        // 构建符号索引
        std::unordered_map<std::string, int> sym_to_idx;
        for (size_t i = 0; i < symbols.size(); ++i) sym_to_idx[symbols[i]] = i;

        // 填充风格因子矩阵
        int n_stocks = symbols.size();
        int n_style = style_factors.size();
        Eigen::MatrixXd style_mat = Eigen::MatrixXd::Constant(n_stocks, n_style, std::numeric_limits<double>::quiet_NaN());

        for (int i = 0; i < result.row_count; ++i) {
            std::string sym = result.string_data[0][i];
            auto it = sym_to_idx.find(sym);
            if (it == sym_to_idx.end()) continue;
            int row = it->second;
            for (int f = 0; f < n_style; ++f) {
                double val = result.numeric_data[1 + f][i]; // 第0列是code，第1列开始是因子
                style_mat(row, f) = val;
            }
        }

        // 获取行业分类和所有行业列表
        auto industry_map = get_industry_classification(symbols, date);
        auto all_industries = get_all_industries();
        Eigen::MatrixXd industry_mat = industry_dummy_matrix(symbols, industry_map, all_industries);

        // 合并风格因子和行业哑变量
        int n_total = n_style + all_industries.size();
        Eigen::MatrixXd full_mat(n_stocks, n_total);
        full_mat << style_mat, industry_mat;

        // 构建因子名称列表
        std::vector<std::string> all_names = style_factors;
        all_names.insert(all_names.end(), all_industries.begin(), all_industries.end());

        return {full_mat, all_names};

    } catch (const std::exception& e) {
        spdlog::error("Error in get_factor_exposures_matrix: {}", e.what());
        return empty_result;
    }
}

Eigen::VectorXd DolphinDBQueryManager::get_stock_returns(
    const std::vector<std::string>& symbols,
    const TimePoint& start_date,
    const TimePoint& end_date,
    bool is_last_period,
    const std::string& price_type)
{
    Eigen::VectorXd returns = Eigen::VectorXd::Zero(symbols.size());
    if (symbols.empty()) return returns;

    std::string end_pt = is_last_period ? "close" : price_type;
    std::string start_pt = price_type;

    auto start_price_mat = query_price_data(symbols, {start_date}, start_pt);
    auto end_price_mat = query_price_data(symbols, {end_date}, end_pt);

    if (start_price_mat.rows() == 0 || end_price_mat.rows() == 0) {
        spdlog::warn("Missing price data in period {} to {}", 
                     timepoint_to_string(start_date), timepoint_to_string(end_date));
        return returns;
    }

    Eigen::VectorXd start_prices = start_price_mat.col(0);
    Eigen::VectorXd end_prices = end_price_mat.col(0);

    for (int i = 0; i < symbols.size(); ++i) {
        double s = start_prices(i);
        double e = end_prices(i);
        if (s > 0 && e > 0) returns(i) = e / s - 1.0;
        else returns(i) = 0.0;
    }
    return returns;
}

} // namespace PortfolioBacktest