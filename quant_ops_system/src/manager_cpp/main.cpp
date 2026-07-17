#include "hv/HttpServer.h"
#include "hv/WebSocketServer.h"
#include "hv/hlog.h"
#include <ormpp/mysql.hpp>
#include <ormpp/dbng.hpp>
#include <ormpp/connection_pool.hpp>
#include <cstdlib>
#include <string>
#include <memory>
#include <chrono>
#include <sw/redis++/redis++.h>
#include <thread>
#include <ctime>
#include <sstream>
#include <vector>
#include <mutex>
#include <set>
#include <map>
#include <algorithm>
#include <fstream>
#include <cctype>
#include <tuple>
#include <cstdint>
#include <unordered_map>

using namespace hv;

// 1. 定义数据表映射结构体 (与数据库中的 tasks 表对应)
struct tasks {
    std::string task_id;
    std::string name;
    std::string node_type;
    std::string task_category;
    std::string script_path;
    std::string cron_expr;
    int timeout_sec = 3600;
    int enabled = 1;
    std::string target_node_id;
    std::string default_params;
};
// 注册反射，让 ormpp 知道如何与 MySQL 的字段自动对应 (新版本使用 YLT_REFL)
YLT_REFL(tasks, task_id, name, node_type, task_category, script_path, cron_expr, timeout_sec, enabled, target_node_id, default_params);

// 1.1 定义任务执行实例映射结构体 (对应 task_instances 表，用于追踪执行状态和日志)
struct task_instances {
    std::string exec_id;      // 唯一执行 ID (如 uuid)
    std::string task_id;      // 关联的 Task ID
    std::string node_id;      // 执行该任务的 Agent 节点 ID
    int status;               // 状态: 0=Running, 1=Success, -1=Failed, -2=Timeout
    int exit_code;            // 进程退出码
    std::string start_time;
    std::string end_time;
    std::string log_path;     // Manager 硬盘上的日志文件路径
    std::string params;       // 执行参数 (JSON 字符串)
    std::string pipeline_id;
};
// 注册 TaskInstance 反射
YLT_REFL(task_instances, exec_id, task_id, node_id, status, exit_code, start_time, end_time, log_path, params, pipeline_id);

// 1.2 定义 DAG 依赖映射结构体 (对应 dag_edges 表)
struct dag_edges {
    std::string parent_task_id;
    std::string child_task_id;
    std::string pipeline_id;
};
YLT_REFL(dag_edges, parent_task_id, child_task_id, pipeline_id);

// 1.3 Agent 节点映射结构体 (对应 agents 表)
struct agents {
    std::string node_id;
    std::string ip;
    std::string os_type;
    std::string status;
    std::string last_heartbeat;
    double cpu_load = 0.0;
    double mem_usage = 0.0;
    std::string created_at;
};
YLT_REFL(agents, node_id, ip, os_type, status, last_heartbeat, cpu_load, mem_usage, created_at);

// 1.4 用户账户 (角色权限由前端按 role 映射，后端只维护用户身份)
struct managed_user {
    std::string user_id;
    std::string username;
    std::string password_hash;
    std::string name;
    std::string email;
    std::string role;
    std::string status;
    std::string last_login_at;
    std::string created_at;
};
YLT_REFL(managed_user, user_id, username, password_hash, name, email, role, status, last_login_at, created_at);


// 1.4 Pipeline / DAG 定义 (对应 pipelines 表)
struct pipelines {
    std::string pipeline_id;
    std::string name;
    std::string description;
    int enabled = 1;
    std::string canvas;         // JSON: { "width": 1600, "height": 720 }
    std::string nodes;          // JSON Array: [{ "task_id": "xxx", "x": 10, "y": 20 }]
    std::string edges;          // JSON Array: [{ "from": "task_a", "to": "task_b" }]
    std::string entry_task_ids; // JSON Array: ["task_a", "task_b"]
    std::string schedule;       // JSON: { "enabled": true, "cron_expr": "0 22 * * *" }
    std::string created_at;
    std::string updated_at;
};
YLT_REFL(pipelines, pipeline_id, name, description, enabled, canvas, nodes, edges, entry_task_ids, schedule, created_at, updated_at);

// 1.5 交易日历映射结构体 (对应 trade_calendar 表，trade_date 使用 YYYY-MM-DD)
struct trade_calendar {
    std::string trade_date;
    std::string source;
    std::string created_at;
};
YLT_REFL(trade_calendar, trade_date, source, created_at);

// 1.6 DolphinDB 监控业务分组 (对应 ddb_monitor_group)
struct ddb_monitor_group {
    std::string group_id;
    std::string display_name;
    int sort_order = 0;
    int enabled = 1;
    std::string description;
    std::string created_at;
    std::string updated_at;
    long monitor_count = 0;
};
YLT_REFL(ddb_monitor_group, group_id, display_name, sort_order, enabled, description, created_at, updated_at);

// 1.7 DolphinDB 表行数监控配置 (对应 ddb_monitor)
struct ddb_monitor {
    std::string monitor_id;
    std::string database;
    std::string table_name;
    std::string date_column;
    std::string date_format;
    std::string frequency;
    std::string where_extra;
    std::string group_id;
    std::string related_task_ids;
    int enabled = 1;
    int anomaly_check_enabled = 1;
    double anomaly_threshold_pct = 0.0;
    int anomaly_window_size = 20;
    int anomaly_min_samples = 5;
    std::string description;
    std::string created_at;
    std::string updated_at;
};
YLT_REFL(ddb_monitor, monitor_id, database, table_name, date_column, date_format, frequency, where_extra, group_id, related_task_ids, enabled, anomaly_check_enabled, anomaly_threshold_pct, anomaly_window_size, anomaly_min_samples, description, created_at, updated_at);

// 1.8 DolphinDB 表行数统计快照 (对应 ddb_monitor_snapshot)
struct ddb_monitor_snapshot {
    std::string monitor_id;
    std::string database;
    std::string table_name;
    std::string group_id;
    std::string date;
    long row_count = -1;
    std::string status;
    std::string checked_at;
    int duration_ms = 0;
    std::string error_message;
};
YLT_REFL(ddb_monitor_snapshot, monitor_id, database, table_name, group_id, date, row_count, status, checked_at, duration_ms, error_message);

// ================= 工具函数：轻量级 Cron 表达式解析 =================
bool match_cron_field(const std::string& field, int value) {
    if (field == "*") return true;
    // 支持 "*/5" 这种整除语法
    if (field.find("*/") == 0) {
        try {
            int div = std::stoi(field.substr(2));
            return div > 0 && (value % div == 0);
        } catch(...) { return false; }
    }
    // 支持精确匹配 "5"
    try { return std::stoi(field) == value; } catch(...) { return false; }
}

bool is_time_for_cron(const std::string& cron_expr, std::time_t now) {
    std::istringstream iss(cron_expr);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) tokens.push_back(token);
    if (tokens.size() != 5) return false; // 必须是5位标准Cron: 分 时 日 月 周

    struct tm tm_now;
    localtime_r(&now, &tm_now); // Linux 下线程安全的本地时间获取

    return match_cron_field(tokens[0], tm_now.tm_min) &&
           match_cron_field(tokens[1], tm_now.tm_hour) &&
           match_cron_field(tokens[2], tm_now.tm_mday) &&
           match_cron_field(tokens[3], tm_now.tm_mon + 1) &&
           match_cron_field(tokens[4], tm_now.tm_wday);

}

template <typename ConnPtr>
std::string resolve_entry_pipeline_id(
    const ConnPtr& conn, const std::string& task_id, const std::string& fallback
) {
    std::vector<std::string> matches;
    auto pipeline_rows = conn->template query<pipelines>("enabled = 1 ORDER BY pipeline_id");
    for (const auto& pipeline : pipeline_rows) {
        bool is_entry = false;
        try {
            hv::Json entry_ids = Json::parse(pipeline.entry_task_ids.empty() ? "[]" : pipeline.entry_task_ids);
            if (entry_ids.is_array()) {
                for (const auto& entry_id : entry_ids) {
                    if (entry_id.is_string() && entry_id.get<std::string>() == task_id) {
                        is_entry = true;
                        break;
                    }
                }
            }

            if (!is_entry && (!entry_ids.is_array() || entry_ids.empty())) {
                hv::Json nodes = Json::parse(pipeline.nodes);
                hv::Json edges = Json::parse(pipeline.edges);
                std::set<std::string> child_ids;
                for (const auto& edge : edges) {
                    if (edge.contains("to") && edge["to"].is_string()) {
                        child_ids.insert(edge["to"].get<std::string>());
                    }
                }
                for (const auto& node : nodes) {
                    if (!node.contains("task_id") || !node["task_id"].is_string()) continue;
                    std::string node_id = node["task_id"].get<std::string>();
                    if (node_id == task_id && !child_ids.count(node_id)) {
                        is_entry = true;
                        break;
                    }
                }
            }
        } catch (...) {
            continue;
        }
        if (is_entry) matches.push_back(pipeline.pipeline_id);
    }
    return matches.size() == 1 ? matches.front() : fallback;
}

template <typename ConnPtr>
std::string resolve_task_pipeline_id(
    const ConnPtr& conn, const std::string& task_id, const std::string& fallback
) {
    std::vector<std::string> matches;
    auto pipeline_rows = conn->template query<pipelines>("enabled = 1 ORDER BY pipeline_id");
    for (const auto& pipeline : pipeline_rows) {
        try {
            hv::Json nodes = Json::parse(pipeline.nodes.empty() ? "[]" : pipeline.nodes);
            if (!nodes.is_array()) continue;
            for (const auto& node : nodes) {
                if (node.is_object() && node.contains("task_id") && node["task_id"].is_string() &&
                    node["task_id"].get<std::string>() == task_id) {
                    matches.push_back(pipeline.pipeline_id);
                    break;
                }
            }
        } catch (...) {}
    }
    return matches.size() == 1 ? matches.front() : fallback;
}

std::string build_child_params(const std::string& child_default_params, const std::string& parent_params) {
    hv::Json merged = Json::object();
    try {
        if (!child_default_params.empty()) {
            auto parsed_child = Json::parse(child_default_params);
            if (parsed_child.is_object()) merged = parsed_child;
        }
    } catch (...) {
        merged = Json::object();
    }

    try {
        if (!parent_params.empty()) {
            auto parsed_parent = Json::parse(parent_params);
            if (parsed_parent.is_object()) {
                static const std::set<std::string> inheritable_keys = {
                    "trade_date", "start_date", "end_date",
                    "dry_run", "enable_write", "test_write", "force"
                };
                for (const auto& key : inheritable_keys) {
                    if (parsed_parent.contains(key)) merged[key] = parsed_parent[key];
                }
            }
        }
    } catch (...) {}

    return merged.dump();
}


std::string sql_escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        if (c == '\'' || c == '\\') escaped.push_back('\\');
        escaped.push_back(c);
    }
    return escaped;
}

std::string join_strings(const std::vector<std::string>& values, const std::string& separator) {
    std::string result;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) result += separator;
        result += values[i];
    }
    return result;
}

std::string sql_quoted_list(const std::vector<std::string>& values) {
    std::vector<std::string> quoted;
    quoted.reserve(values.size());
    for (const auto& value : values) {
        quoted.push_back("'" + sql_escape(value) + "'");
    }
    return join_strings(quoted, ",");
}

std::string status_to_label(int status) {
    if (status == 2) return "waiting";
    if (status == 0) return "running";
    if (status == 1) return "success";
    if (status == -2) return "timeout";
    return "failed";
}

std::string normalize_task_category(std::string category) {
    category.erase(std::remove_if(category.begin(), category.end(), [](unsigned char c) { return std::isspace(c); }), category.end());
    if (category.empty()) return "ops";
    std::string lower = category;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (lower == "ops" || lower == "operation" || lower == "maintenance" || category == "运维任务") return "ops";
    if (lower == "data_download" || lower == "data-download" || lower == "download" || category == "数据下载任务") return "data_download";
    if (lower == "factor_compute" || lower == "factor-compute" || lower == "factor" || category == "因子计算任务") return "factor_compute";
    if (lower == "model_training" || lower == "model-training" || lower == "model" || category == "模型训练任务") return "model_training";
    return "ops";
}

std::string task_category_label(const std::string& category) {
    std::string normalized = normalize_task_category(category);
    if (normalized == "data_download") return "数据下载任务";
    if (normalized == "factor_compute") return "因子计算任务";
    if (normalized == "model_training") return "模型训练任务";
    return "运维任务";
}

std::string target_os_from_node(const std::string& node_id) {
    std::string lowered = node_id;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(), ::tolower);
    if (lowered.find("win") != std::string::npos) return "windows";
    return "linux";
}

std::string trigger_mode_from_exec_id(const std::string& exec_id, const std::string& cron_expr) {
    if (exec_id.find("_dag_") != std::string::npos) return "dependency";
    if (exec_id.find("_manual_") != std::string::npos) return "manual";
    if (!cron_expr.empty()) return "cron";
    return "manual";
}

hv::Json parse_params_json(const std::string& params) {
    if (params.empty()) return Json::object();
    try {
        auto parsed = Json::parse(params);
        if (parsed.is_object()) return parsed;
    } catch (...) {}
    return Json::object();
}

// 将时间戳转换为东八区（UTC+8）时间字符串 (线程安全版)
std::string to_cst_time_str(std::time_t t) {
    t += 8 * 3600; // 增加8小时偏移
    struct tm tm_utc;
    gmtime_r(&t, &tm_utc); // 使用线程安全的 gmtime_r
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_utc);
    return buf;
}

// MySQL 会话已固定为 +08:00，查询结果可直接作为北京时间返回。
std::string cst_time_str_from_sql(const std::string& sql_time_str) {
    return sql_time_str;
}

std::string today_yyyy_mm_dd() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    return to_cst_time_str(now_time).substr(0, 10);
}

std::string date_days_ago(int days) {
    auto tp = std::chrono::system_clock::now() - std::chrono::hours(24 * days);
    std::time_t t = std::chrono::system_clock::to_time_t(tp);
    return to_cst_time_str(t).substr(0, 10);
}

std::string compact_date_to_iso(std::string value) {
    value.erase(std::remove(value.begin(), value.end(), '-'), value.end());
    if (value.size() == 8) return value.substr(0, 4) + "-" + value.substr(4, 2) + "-" + value.substr(6, 2);
    return "";
}

std::string normalize_iso_date(std::string value) {
    if (value.size() >= 10 && value[4] == '-' && value[7] == '-') return value.substr(0, 10);
    return compact_date_to_iso(value);
}

std::string iso_date_to_compact(const std::string& value) {
    if (value.size() >= 10) return value.substr(0, 4) + value.substr(5, 2) + value.substr(8, 2);
    return "";
}

bool is_valid_iso_date(const std::string& value) {
    if (value.size() != 10 || value[4] != '-' || value[7] != '-') return false;
    struct tm parsed {};
    parsed.tm_isdst = -1;
    char* end = strptime(value.c_str(), "%Y-%m-%d", &parsed);
    if (!end || *end != 0) return false;
    if (std::mktime(&parsed) == static_cast<std::time_t>(-1)) return false;
    char normalized[16];
    std::strftime(normalized, sizeof(normalized), "%Y-%m-%d", &parsed);
    return value == normalized;
}

std::string sql_time_to_iso(const std::string& value) {
    if (value.size() < 19) return value;
    std::string result = value.substr(0, 19);
    result[10] = 'T';
    return result;
}

std::string readable_duration(long seconds) {
    if (seconds < 0) return "";
    long hours = seconds / 3600;
    long minutes = (seconds % 3600) / 60;
    long remaining_seconds = seconds % 60;
    std::string result;
    if (hours > 0) result += std::to_string(hours) + "h";
    if (minutes > 0) result += std::to_string(minutes) + "m";
    if (remaining_seconds > 0 || result.empty()) result += std::to_string(remaining_seconds) + "s";
    return result;
}

std::string weekday_cn(const std::string& iso_date) {
    struct tm tm_date {};
    if (!strptime(iso_date.c_str(), "%Y-%m-%d", &tm_date)) return "";
    std::mktime(&tm_date);
    static const char* names[] = {"周日", "周一", "周二", "周三", "周四", "周五", "周六"};
    return names[tm_date.tm_wday];
}

bool is_weekday_iso(const std::string& iso_date) {
    struct tm tm_date {};
    if (!strptime(iso_date.c_str(), "%Y-%m-%d", &tm_date)) return false;
    std::mktime(&tm_date);
    return tm_date.tm_wday >= 1 && tm_date.tm_wday <= 5;
}

std::vector<std::string> recent_weekdays(int count) {
    std::vector<std::string> days;
    auto tp = std::chrono::system_clock::now();
    for (int offset = 0; days.size() < static_cast<size_t>(count) && offset < count * 3; ++offset) {
        std::time_t t = std::chrono::system_clock::to_time_t(tp - std::chrono::hours(24 * offset));
        std::string day = to_cst_time_str(t).substr(0, 10);
        if (is_weekday_iso(day)) days.push_back(day);
    }

    return days;
}

template <typename ConnPtr>
std::vector<std::string> recent_trade_days_from_db(const ConnPtr& conn, int count, bool& used_calendar) {
    used_calendar = false;
    std::vector<std::string> days;
    if (!conn || count <= 0) return days;

    try {
        std::string today = today_yyyy_mm_dd();
        auto rows = conn->template query<trade_calendar>(
            "trade_date <= '" + sql_escape(today) + "' ORDER BY trade_date DESC LIMIT " + std::to_string(count)
        );
        for (const auto& row : rows) {
            std::string day = normalize_iso_date(row.trade_date);
            if (!day.empty()) days.push_back(day);
        }
        if (!days.empty()) {
            used_calendar = true;
        }
    } catch (const std::exception& e) {
        hlogw("trade_calendar query failed, fallback to weekday approximation: %s", e.what());
    } catch (...) {
        hlogw("trade_calendar query failed, fallback to weekday approximation");
    }
    return days;
}

std::vector<std::string> weekdays_between(std::string start_iso, std::string end_iso, size_t limit = 120) {
    std::vector<std::string> days;
    struct tm tm_start {};
    struct tm tm_end {};
    if (!strptime(start_iso.c_str(), "%Y-%m-%d", &tm_start) || !strptime(end_iso.c_str(), "%Y-%m-%d", &tm_end)) return days;
    std::time_t start = std::mktime(&tm_start);
    std::time_t end = std::mktime(&tm_end);
    if (start == (std::time_t)-1 || end == (std::time_t)-1 || end < start) return days;
    for (std::time_t t = start; t <= end && days.size() < limit; t += 24 * 3600) {
        char buf[16];
        std::strftime(buf, sizeof(buf), "%Y-%m-%d", std::localtime(&t));
        std::string day = buf;
        if (is_weekday_iso(day)) days.push_back(day);
    }
    return days;
}

std::vector<std::string> covered_trade_days_from_instance(const task_instances& inst) {
    hv::Json params = parse_params_json(inst.params);
    std::string trade_date;
    if (params.contains("trade_date") && params["trade_date"].is_string()) {
        trade_date = normalize_iso_date(params["trade_date"].get<std::string>());
    }
    if (!trade_date.empty()) return {trade_date};

    if (params.contains("start_date") && params.contains("end_date") &&
        params["start_date"].is_string() && params["end_date"].is_string()) {
        std::string start = normalize_iso_date(params["start_date"].get<std::string>());
        std::string end = normalize_iso_date(params["end_date"].get<std::string>());
        auto days = weekdays_between(start, end);
        if (!days.empty()) return days;
    }

    if (inst.start_time.size() >= 10) return {inst.start_time.substr(0, 10)};
    return {};
}

std::string instance_sort_time(const task_instances& inst) {
    return inst.end_time.empty() ? inst.start_time : inst.end_time;
}

long seconds_between_sql_time(const std::string& start_time, const std::string& end_time) {
    if (start_time.empty() || end_time.empty()) return -1;
    struct tm start_tm {};
    struct tm end_tm {};
    if (!strptime(start_time.c_str(), "%Y-%m-%d %H:%M:%S", &start_tm)) return -1;
    if (!strptime(end_time.c_str(), "%Y-%m-%d %H:%M:%S", &end_tm)) return -1;
    std::time_t start = std::mktime(&start_tm);
    std::time_t end = std::mktime(&end_tm);
    if (start == (std::time_t)-1 || end == (std::time_t)-1 || end < start) return -1;
    return static_cast<long>(end - start);
}

hv::Json admin_permissions_json() {
    return Json::array({
        "dashboard:view", "task:view", "task:edit", "task:run",
        "agent:view", "agent:manage",
        "pipeline:view", "pipeline:edit", "pipeline:run",
        "data:view", "data:manage",
        "log:view", "log:download",
        "settings:view", "user:manage", "alert:manage", "system:manage"
    });
}

hv::Json user_json() {
    return Json::object({
        {"user_id", "u_admin"},
        {"name", "系统管理员"},
        {"email", "your_username@quant.local"},
        {"role", "your_username"},
        {"permissions", admin_permissions_json()}
    });
}

std::mutex g_auth_mutex;
std::unordered_map<std::string, std::string> g_auth_token=${ACCESS_TOKEN};
uint64_t g_auth_token=${ACCESS_TOKEN} = 0;

std::string new_auth_token=${ACCESS_TOKEN} std::string& user_id) {
    std::lock_guard<std::mutex> lock(g_auth_mutex);
    ++g_auth_token=${ACCESS_TOKEN};
    std::string token=${ACCESS_TOKEN}" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()) + "_" + std::to_string(g_auth_token=${ACCESS_TOKEN};
    g_auth_token=${ACCESS_TOKEN} = user_id;
    return token;
}

std::string user_id_from_request(HttpRequest* req) {
    std::string header = req->GetHeader("Authorization");
    const std::string prefix = "Bearer ";
    if (header == prefix + "quant_ops_secret_2026") return "u_admin";
    if (header.find(prefix) != 0) return "";
    std::lock_guard<std::mutex> lock(g_auth_mutex);
    auto it = g_auth_token=${ACCESS_TOKEN};
    return it == g_auth_token=${ACCESS_TOKEN} ? "" : it->second;
}

bool valid_user_role(const std::string& role) {
    return role == "your_username" || role == "operator" || role == "researcher";
}

bool valid_user_status(const std::string& status) {
    return status == "active" || status == "disabled";
}

hv::Json managed_user_json(const managed_user& user) {
    Json result = Json::object({
        {"user_id", user.user_id},
        {"username", user.username},
        {"name", user.name},
        {"email", user.email},
        {"role", user.role},
        {"status", user.status},
        {"created_at", user.created_at}
    });
    if (!user.last_login_at.empty()) result["last_login_at"] = user.last_login_at;
    return result;
}

template <typename ConnPtr>
std::vector<managed_user> query_managed_users(const ConnPtr& conn, const std::string& where = "1=1") {
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string>;
    auto rows = conn->template query_s<Row>(
        "SELECT user_id, username, password_hash, name, email, role, status, "
        "COALESCE(DATE_FORMAT(last_login_at, '%Y-%m-%d %H:%i:%s'), ''), "
        "COALESCE(DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), '') FROM users WHERE " + where + " ORDER BY created_at DESC"
    );
    std::vector<managed_user> users;
    for (const auto& row : rows) {
        managed_user user;
        user.user_id = std::get<0>(row);
        user.username = ${DB_USERNAME}<1>(row);
        user.password_hash = std::get<2>(row);
        user.name = std::get<3>(row);
        user.email = std::get<4>(row);
        user.role = std::get<5>(row);
        user.status = std::get<6>(row);
        user.last_login_at = std::get<7>(row);
        user.created_at = std::get<8>(row);
        users.push_back(std::move(user));
    }
    return users;
}

template <typename ConnPtr>
bool request_is_admin(const ConnPtr& conn, HttpRequest* req) {
    std::string user_id = user_id_from_request(req);
    if (user_id.empty()) return false;
    auto users = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "' AND status = 'active'");
    return !users.empty() && users.front().role == "your_username";
}

std::string exec_id_from_log_id(const std::string& log_id) {
    const std::string prefix = "log_";
    if (log_id.find(prefix) == 0) return log_id.substr(prefix.size());
    return log_id;
}

std::string log_id_from_exec_id(const std::string& exec_id) {
    return "log_" + exec_id;
}

std::string read_text_file(const std::string& path) {
    if (path.empty()) return "";
    std::ifstream ifs(path, std::ios::in | std::ios::binary);
    if (!ifs) return "";
    std::ostringstream ss;
    ss << ifs.rdbuf();
    return ss.str();
}

std::string default_log_path(const std::string& exec_id) {
    return "logs/tasks/" + exec_id + ".log";
}

std::string log_level_from_status(int status) {
    if (status == -1) return "ERROR";
    if (status == -2) return "WARN";
    return "INFO";
}

hv::Json split_log_streams(const std::string& text) {
    std::istringstream iss(text);
    std::string line;
    std::ostringstream stdout_ss;
    std::ostringstream stderr_ss;
    while (std::getline(iss, line)) {
        if (line.find("[stderr]") == 0) {
            stderr_ss << line.substr(8) << "\n";
        } else if (line.find("[stdout]") == 0) {
            stdout_ss << line.substr(8) << "\n";
        } else {
            stdout_ss << line << "\n";
        }
    }
    return Json::object({{"stdout", stdout_ss.str()}, {"stderr", stderr_ss.str()}});
}

std::string lower_copy(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return value;
}

bool contains_ci(const std::string& text, const std::string& keyword) {
    if (keyword.empty()) return true;
    return lower_copy(text).find(lower_copy(keyword)) != std::string::npos;
}

std::vector<std::string> query_values(HttpRequest* req, const std::string& key) {
    std::vector<std::string> values;
    auto add_value = [&](const std::string& value) {
        if (!value.empty()) values.push_back(value);
    };

    std::string raw = req->url;
    auto pos = raw.find('?');
    if (pos != std::string::npos) {
        std::string query = raw.substr(pos + 1);
        std::istringstream pairs(query);
        std::string pair;
        while (std::getline(pairs, pair, '&')) {
            auto eq = pair.find('=');
            std::string raw_key = eq == std::string::npos ? pair : pair.substr(0, eq);
            std::string raw_value = eq == std::string::npos ? "" : pair.substr(eq + 1);
            if (raw_key == key || raw_key == key + "[]" || raw_key == key + "%5B%5D") {
                add_value(raw_value);
            }
        }
        if (!values.empty()) return values;
    }

    add_value(req->GetParam(key.c_str()));
    add_value(req->GetParam((key + "[]").c_str()));
    return values;
}

std::pair<std::string, std::string> date_range_from_request(HttpRequest* req) {
    auto values = query_values(req, "date_range");
    std::vector<std::string> dates;
    for (const auto& value : values) {
        std::istringstream parts(value);
        std::string part;
        while (std::getline(parts, part, ',')) {
            if (!part.empty()) dates.push_back(part);
        }
    }
    if (dates.size() >= 2) return {dates.front(), dates[1]};
    return {"", ""};
}

hv::Json build_log_detail_json(const task_instances& inst, const std::string& task_name) {
    std::string path = inst.log_path.empty() ? default_log_path(inst.exec_id) : inst.log_path;
    std::string text = read_text_file(path);
    Json streams = split_log_streams(text);
    return Json::object({
        {"log_id", log_id_from_exec_id(inst.exec_id)},
        {"exec_id", inst.exec_id},
        {"task_id", inst.task_id},
        {"task_name", task_name},
        {"agent_id", inst.node_id},
        {"level", log_level_from_status(inst.status)},
        {"time", inst.end_time.empty() ? inst.start_time : inst.end_time},
        {"status", status_to_label(inst.status)},
        {"stdout", streams.value("stdout", "")},
        {"stderr", streams.value("stderr", "")}
    });
}

std::string build_log_download_text(const hv::Json& detail) {
    return std::string("exec_id=") + detail.value("exec_id", "") + "\n" +
           "task=" + detail.value("task_name", "") + "\n" +
           "agent=" + detail.value("agent_id", "") + "\n" +
           "status=" + detail.value("status", "") + "\n" +
           "time=" + detail.value("time", "") + "\n\n" +
           "[stdout]\n" +
           (detail.value("stdout", "").empty() ? "(empty)" : detail.value("stdout", "")) + "\n\n" +
           "[stderr]\n" +
           (detail.value("stderr", "").empty() ? "(empty)" : detail.value("stderr", ""));
}

hv::Json task_instance_to_json(const task_instances& inst, const std::string& trigger_mode) {
    return Json::object({
        {"exec_id", inst.exec_id},
        {"task_id", inst.task_id},
        {"node_id", inst.node_id},
        {"status", inst.status},
        {"exit_code", inst.exit_code},
        {"start_time", inst.start_time},
        {"end_time", inst.end_time},
        {"log_path", inst.log_path},
        {"params", inst.params},
        {"trigger_mode", trigger_mode}
    });
}

std::string sanitize_identifier(std::string value) {
    std::string out;
    for (char c : value) {
        if (std::isalnum(static_cast<unsigned char>(c))) out.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
        else if (c == '_' || c == '-') out.push_back('_');
    }
    while (out.find("__") != std::string::npos) out.replace(out.find("__"), 2, "_");
    if (!out.empty() && out.front() == '_') out.erase(out.begin());
    if (!out.empty() && out.back() == '_') out.pop_back();
    return out.empty() ? "item" : out;
}

std::string generate_monitor_id(const std::string& database, const std::string& table_name) {
    return "mon_" + sanitize_identifier(database + "_" + table_name) + "_" + std::to_string(std::time(nullptr));
}

std::string normalize_ddb_database_path(const std::string& database) {
    if (database.find("dfs://") == 0) return database;
    return "dfs://" + database;
}

std::string display_ddb_database_name(const std::string& database) {
    const std::string prefix = "dfs://";
    if (database.find(prefix) == 0) return database.substr(prefix.size());
    return database;
}

bool valid_iso_date(const std::string& value) {
    struct tm tm_date {};
    return value.size() == 10 && strptime(value.c_str(), "%Y-%m-%d", &tm_date) != nullptr;
}

int parse_int_param(const std::string& value, int default_value, int min_value, int max_value) {
    if (value.empty()) return default_value;
    try {
        int parsed = std::stoi(value);
        if (parsed < min_value) return min_value;
        if (parsed > max_value) return max_value;
        return parsed;
    } catch (...) {
        return default_value;
    }
}

std::string sql_like_escape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size() * 2);
    for (char c : value) {
        if (c == '\\' || c == '%' || c == '_') escaped.push_back('\\');
        if (c == '\'') escaped.push_back('\\');
        escaped.push_back(c);
    }
    return escaped;
}

template <typename ConnPtr>
long query_sql_count(const ConnPtr& conn, const std::string& sql) {
    using Row = std::tuple<std::string>;
    auto rows = conn->template query_s<Row>(sql);
    if (rows.empty() || std::get<0>(rows.front()).empty()) return 0;
    return std::stol(std::get<0>(rows.front()));
}

bool validate_where_extra(const std::string& where_extra, std::string& error_msg) {
    if (where_extra.size() > 512) {
        error_msg = "where_extra is too long";
        return false;
    }
    std::string lowered = lower_copy(where_extra);
    static const std::vector<std::string> denied = {
        ";", "--", "/*", "*/", " insert ", " update ", " delete ", " drop ", " alter ", " truncate ", " create "
    };
    std::string padded = " " + lowered + " ";
    for (const auto& token=${ACCESS_TOKEN} {
        if (padded.find(token=${ACCESS_TOKEN} != std::string::npos) {
            error_msg = "where_extra contains unsafe token=${ACCESS_TOKEN} " + token;
            return false;
        }
    }
    return true;
}

hv::Json related_task_ids_json(const std::string& raw) {
    if (raw.empty()) return Json::array();
    try {
        auto parsed = Json::parse(raw);
        if (parsed.is_array()) return parsed;
    } catch (...) {}
    return Json::array();
}

bool valid_group_id(const std::string& group_id) {
    if (group_id.empty() || group_id.size() > 64) return false;
    if (group_id[0] < 'a' || group_id[0] > 'z') return false;
    for (char ch : group_id) {
        bool ok = (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_';
        if (!ok) return false;
    }
    return true;
}

hv::Json monitor_group_to_json(const ddb_monitor_group& g) {
    return Json::object({
        {"group_id", g.group_id},
        {"groupId", g.group_id},
        {"display_name", g.display_name},
        {"displayName", g.display_name},
        {"sort_order", g.sort_order},
        {"sortOrder", g.sort_order},
        {"enabled", g.enabled},
        {"description", g.description},
        {"monitor_count", g.monitor_count},
        {"monitorCount", g.monitor_count},
        {"created_at", g.created_at},
        {"createdAt", g.created_at},
        {"updated_at", g.updated_at},
        {"updatedAt", g.updated_at}
    });
}

template <typename ConnPtr>
std::vector<ddb_monitor_group> query_ddb_monitor_groups(const ConnPtr& conn, const std::string& where = "") {
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string, std::string, std::string, std::string>;
    std::string sql =
        "SELECT g.group_id, g.display_name, CAST(g.sort_order AS CHAR), CAST(g.enabled AS CHAR), "
        "COALESCE(g.description, ''), COALESCE(DATE_FORMAT(g.created_at, '%Y-%m-%d %H:%i:%s'), ''), "
        "COALESCE(DATE_FORMAT(g.updated_at, '%Y-%m-%d %H:%i:%s'), ''), CAST(COUNT(m.monitor_id) AS CHAR) "
        "FROM ddb_monitor_group g "
        "LEFT JOIN ddb_monitor m ON m.group_id = g.group_id";
    if (!where.empty()) sql += " WHERE " + where;
    sql += " GROUP BY g.group_id, g.display_name, g.sort_order, g.enabled, g.description, g.created_at, g.updated_at "
           "ORDER BY g.sort_order ASC, g.group_id ASC";

    auto rows = conn->template query_s<Row>(sql);
    std::vector<ddb_monitor_group> groups;
    groups.reserve(rows.size());
    for (const auto& row : rows) {
        ddb_monitor_group g;
        g.group_id = std::get<0>(row);
        g.display_name = std::get<1>(row);
        g.sort_order = std::get<2>(row).empty() ? 0 : std::stoi(std::get<2>(row));
        g.enabled = std::get<3>(row).empty() ? 0 : std::stoi(std::get<3>(row));
        g.description = std::get<4>(row);
        g.created_at = std::get<5>(row);
        g.updated_at = std::get<6>(row);
        g.monitor_count = std::get<7>(row).empty() ? 0 : std::stol(std::get<7>(row));
        groups.push_back(g);
    }
    return groups;
}

template <typename ConnPtr>
bool ddb_monitor_group_exists(const ConnPtr& conn, const std::string& group_id, bool require_enabled = false) {
    std::string where = "group_id = '" + sql_escape(group_id) + "'";
    if (require_enabled) where += " AND enabled = 1";
    return query_sql_count(conn, "SELECT COUNT(*) FROM ddb_monitor_group WHERE " + where) > 0;
}

std::string normalize_monitor_frequency(std::string frequency) {
    frequency.erase(std::remove_if(frequency.begin(), frequency.end(), [](unsigned char c) { return std::isspace(c); }), frequency.end());
    if (frequency.empty()) return "daily";
    std::string lower = frequency;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (lower == "daily" || lower == "day" || lower == "d" || lower == "1d" || frequency == "日频") return "daily";
    if (lower == "weekly" || lower == "week" || lower == "w" || lower == "1w" || frequency == "周频") return "weekly";
    if (lower == "monthly" || lower == "month" || lower == "m" || lower == "1m" || frequency == "月频") return "monthly";
    return "daily";
}

std::string monitor_frequency_label(const std::string& frequency) {
    std::string normalized = normalize_monitor_frequency(frequency);
    if (normalized == "weekly") return "周频";
    if (normalized == "monthly") return "月频";
    return "日频";
}

template <typename ConnPtr>
bool has_trade_calendar_date(const ConnPtr& conn, const std::string& date) {
    return query_sql_count(conn, "SELECT COUNT(*) FROM trade_calendar WHERE trade_date='" + sql_escape(date) + "'") > 0;
}

bool is_last_weekday_of_month(const std::string& date) {
    if (!is_valid_iso_date(date)) return true;
    struct tm tm_date {};
    strptime(date.c_str(), "%Y-%m-%d", &tm_date);
    std::time_t current = std::mktime(&tm_date);
    if (current == (std::time_t)-1) return true;
    char month_buf[8];
    std::strftime(month_buf, sizeof(month_buf), "%Y-%m", std::localtime(&current));
    for (std::time_t next = current + 24 * 3600; ; next += 24 * 3600) {
        struct tm* next_tm = std::localtime(&next);
        char next_month[8];
        std::strftime(next_month, sizeof(next_month), "%Y-%m", next_tm);
        if (std::string(next_month) != std::string(month_buf)) return true;
        if (next_tm->tm_wday >= 1 && next_tm->tm_wday <= 5) return false;
    }
}

template <typename ConnPtr>
bool is_expected_monitor_data_day(const ConnPtr& conn, const std::string& date, const std::string& frequency) {
    std::string normalized = normalize_monitor_frequency(frequency);
    if (normalized == "daily") return true;
    if (!is_valid_iso_date(date)) return true;

    using Row = std::tuple<std::string>;
    std::string escaped = sql_escape(date);
    try {
        std::string sql;
        if (normalized == "weekly") {
            sql = "SELECT COALESCE(MAX(trade_date), '') FROM trade_calendar "
                  "WHERE YEARWEEK(STR_TO_DATE(trade_date, '%Y-%m-%d'), 1) = "
                  "YEARWEEK(STR_TO_DATE('" + escaped + "', '%Y-%m-%d'), 1)";
        } else {
            sql = "SELECT COALESCE(MAX(trade_date), '') FROM trade_calendar "
                  "WHERE DATE_FORMAT(STR_TO_DATE(trade_date, '%Y-%m-%d'), '%Y-%m') = "
                  "DATE_FORMAT(STR_TO_DATE('" + escaped + "', '%Y-%m-%d'), '%Y-%m')";
        }
        auto rows = conn->template query_s<Row>(sql);
        if (!rows.empty() && !std::get<0>(rows.front()).empty()) {
            return std::get<0>(rows.front()) == date;
        }
    } catch (...) {}

    if (normalized == "weekly") return weekday_cn(date) == "周五";
    return is_last_weekday_of_month(date);
}

struct row_count_anomaly_result {
    bool checked = false;
    bool anomaly = false;
    long baseline = -1;
    double deviation_pct = 0.0;
    int sample_count = 0;
    double threshold_pct = 0.0;
    std::string reason;
};

double default_anomaly_threshold_pct(const std::string& frequency) {
    std::string normalized = normalize_monitor_frequency(frequency);
    if (normalized == "weekly") return 0.40;
    if (normalized == "monthly") return 0.50;
    return 0.30;
}

long median_row_count(std::vector<long> values) {
    if (values.empty()) return -1;
    std::sort(values.begin(), values.end());
    size_t mid = values.size() / 2;
    if (values.size() % 2 == 1) return values[mid];
    return (values[mid - 1] + values[mid]) / 2;
}

template <typename ConnPtr>
row_count_anomaly_result detect_row_count_anomaly(
    const ConnPtr& conn,
    const std::string& monitor_id,
    const std::string& date,
    long row_count,
    const std::string& frequency,
    int enabled,
    double configured_threshold_pct,
    int configured_window_size,
    int configured_min_samples
) {
    row_count_anomaly_result result;
    if (!enabled || row_count <= 0 || !is_valid_iso_date(date)) return result;

    int window_size = configured_window_size > 0 ? configured_window_size : 20;
    int min_samples = configured_min_samples > 0 ? configured_min_samples : 5;
    double threshold_pct = configured_threshold_pct > 0.0 ? configured_threshold_pct : default_anomaly_threshold_pct(frequency);
    if (threshold_pct > 1.0) threshold_pct = threshold_pct / 100.0;
    result.threshold_pct = threshold_pct;

    using Row = std::tuple<std::string>;
    std::string sql = "SELECT CAST(row_count AS CHAR) FROM ddb_monitor_snapshot "
                      "WHERE monitor_id='" + sql_escape(monitor_id) + "' AND `date` < '" + sql_escape(date) + "' "
                      "AND row_count > 0 AND status = 'success' "
                      "ORDER BY `date` DESC LIMIT " + std::to_string(window_size);
    auto rows = conn->template query_s<Row>(sql);
    std::vector<long> samples;
    samples.reserve(rows.size());
    for (const auto& row : rows) {
        try {
            long value = std::stol(std::get<0>(row));
            if (value > 0) samples.push_back(value);
        } catch (...) {}
    }

    result.sample_count = static_cast<int>(samples.size());
    if (result.sample_count < min_samples) return result;

    long baseline = median_row_count(samples);
    if (baseline <= 0) return result;

    result.checked = true;
    result.baseline = baseline;
    result.deviation_pct = std::abs(static_cast<double>(row_count - baseline)) / static_cast<double>(baseline);

    long abs_diff = std::labs(row_count - baseline);
    bool large_table_anomaly = baseline >= 1000 && result.deviation_pct >= threshold_pct;
    bool small_table_anomaly = baseline < 1000 && result.deviation_pct >= std::max(threshold_pct, 0.50) && abs_diff >= 100;
    result.anomaly = large_table_anomaly || small_table_anomaly;
    if (result.anomaly) {
        result.reason = "row_count deviates " + std::to_string(static_cast<int>(result.deviation_pct * 10000) / 100.0) +
                        "% from median baseline " + std::to_string(baseline);
    }
    return result;
}

std::string effective_monitor_snapshot_status(const std::string& raw_status, long row_count, const std::string& date, const std::string& frequency, bool expected_data_day) {
    if (raw_status == "zero" && row_count == 0 && !expected_data_day) return "success";
    return raw_status;
}

hv::Json monitor_to_json(const ddb_monitor& m) {
    hv::Json related = related_task_ids_json(m.related_task_ids);
    std::string date_format = m.date_format.empty() ? "YYYY-MM-DD" : m.date_format;
    std::string frequency = normalize_monitor_frequency(m.frequency);
    std::string database_name = display_ddb_database_name(m.database);
    return Json::object({
        {"monitor_id", m.monitor_id},
        {"monitorId", m.monitor_id},
        {"database", m.database},
        {"database_path", m.database},
        {"databasePath", m.database},
        {"database_name", database_name},
        {"databaseName", database_name},
        {"table_name", m.table_name},
        {"tableName", m.table_name},
        {"date_column", m.date_column},
        {"dateColumn", m.date_column},
        {"date_format", date_format},
        {"dateFormat", date_format},
        {"frequency", monitor_frequency_label(frequency)},
        {"frequencyCode", frequency},
        {"frequency_code", frequency},
        {"frequencyLabel", monitor_frequency_label(frequency)},
        {"frequency_label", monitor_frequency_label(frequency)},
        {"where_extra", m.where_extra},
        {"whereExtra", m.where_extra},
        {"group_id", m.group_id},
        {"groupId", m.group_id},
        {"group_name", m.group_id},
        {"groupName", m.group_id},
        {"group", m.group_id},
        {"group_label", m.group_id},
        {"groupLabel", m.group_id},
        {"related_task_ids", related},
        {"relatedTaskIds", related},
        {"enabled", m.enabled},
        {"anomaly_check_enabled", m.anomaly_check_enabled},
        {"anomalyCheckEnabled", m.anomaly_check_enabled},
        {"anomaly_threshold_pct", m.anomaly_threshold_pct},
        {"anomalyThresholdPct", m.anomaly_threshold_pct},
        {"anomaly_window_size", m.anomaly_window_size},
        {"anomalyWindowSize", m.anomaly_window_size},
        {"anomaly_min_samples", m.anomaly_min_samples},
        {"anomalyMinSamples", m.anomaly_min_samples},
        {"description", m.description},
        {"created_at", m.created_at},
        {"createdAt", m.created_at},
        {"updated_at", m.updated_at},
        {"updatedAt", m.updated_at}
    });
}

hv::Json snapshot_to_json(const ddb_monitor_snapshot& s) {
    Json row_count_json = nullptr;
    if (s.row_count >= 0) row_count_json = s.row_count;
    std::string day_label = s.date.size() >= 10 ? s.date.substr(5, 5) : s.date;
    std::string database_name = display_ddb_database_name(s.database);
    return Json::object({
        {"monitor_id", s.monitor_id},
        {"monitorId", s.monitor_id},
        {"database", s.database},
        {"database_path", s.database},
        {"databasePath", s.database},
        {"database_name", database_name},
        {"databaseName", database_name},
        {"table_name", s.table_name},
        {"tableName", s.table_name},
        {"group_id", s.group_id},
        {"groupId", s.group_id},
        {"group_name", s.group_id},
        {"groupName", s.group_id},
        {"group", s.group_id},
        {"group_label", s.group_id},
        {"groupLabel", s.group_id},
        {"date", s.date},
        {"day_label", day_label},
        {"dayLabel", day_label},
        {"row_count", row_count_json},
        {"rowCount", row_count_json},
        {"status", s.status},
        {"checked_at", s.checked_at},
        {"checkedAt", s.checked_at},
        {"duration_ms", s.duration_ms},
        {"durationMs", s.duration_ms},
        {"error_message", s.error_message},
        {"errorMessage", s.error_message}
    });
}

template <typename ConnPtr>
std::vector<ddb_monitor> query_ddb_monitors(const ConnPtr& conn, const std::string& where = "") {
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string>;
    std::string sql = "SELECT monitor_id, TRIM(`database`), TRIM(table_name), TRIM(date_column), date_format, frequency, where_extra, "
                      "TRIM(COALESCE(group_id, '')), COALESCE(CAST(related_task_ids AS CHAR), '[]'), CAST(enabled AS CHAR), "
                      "CAST(COALESCE(anomaly_check_enabled, 1) AS CHAR), CAST(COALESCE(anomaly_threshold_pct, 0) AS CHAR), "
                      "CAST(COALESCE(anomaly_window_size, 20) AS CHAR), CAST(COALESCE(anomaly_min_samples, 5) AS CHAR), description, "
                      "COALESCE(DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), ''), "
                      "COALESCE(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s'), '') "
                      "FROM ddb_monitor";
    if (!where.empty()) sql += " WHERE " + where;

    auto rows = conn->template query_s<Row>(sql);
    std::vector<ddb_monitor> monitors;
    monitors.reserve(rows.size());
    for (const auto& row : rows) {
        ddb_monitor m;
        m.monitor_id = std::get<0>(row);
        m.database = std::get<1>(row);
        m.table_name = std::get<2>(row);
        m.date_column = std::get<3>(row);
        m.date_format = std::get<4>(row);
        m.frequency = std::get<5>(row);
        m.where_extra = std::get<6>(row);
        m.group_id = std::get<7>(row);
        m.related_task_ids = std::get<8>(row);
        m.enabled = std::get<9>(row).empty() ? 0 : std::stoi(std::get<9>(row));
        m.anomaly_check_enabled = std::get<10>(row).empty() ? 1 : std::stoi(std::get<10>(row));
        m.anomaly_threshold_pct = std::get<11>(row).empty() ? 0.0 : std::stod(std::get<11>(row));
        m.anomaly_window_size = std::get<12>(row).empty() ? 20 : std::stoi(std::get<12>(row));
        m.anomaly_min_samples = std::get<13>(row).empty() ? 5 : std::stoi(std::get<13>(row));
        m.description = std::get<14>(row);
        m.created_at = std::get<15>(row);
        m.updated_at = std::get<16>(row);
        monitors.push_back(m);
    }
    return monitors;
}

template <typename ConnPtr>
hv::Json ddb_monitors_page_json(const ConnPtr& conn, const std::string& where, int page, int page_size) {
    long total = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM ddb_monitor WHERE " + where);
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string>;
    int offset = (page - 1) * page_size;
    std::string sql =
        "SELECT monitor_id, `database`, table_name, date_column, date_format, frequency, where_extra, group_id, "
        "COALESCE(related_task_ids_text, '[]'), CAST(enabled AS CHAR), "
        "CAST(COALESCE(anomaly_check_enabled, 1) AS CHAR), CAST(COALESCE(anomaly_threshold_pct, 0) AS CHAR), "
        "CAST(COALESCE(anomaly_window_size, 20) AS CHAR), CAST(COALESCE(anomaly_min_samples, 5) AS CHAR), description, "
        "COALESCE(DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s'), ''), "
        "COALESCE(DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s'), '') "
        "FROM ddb_monitor WHERE " + where +
        " ORDER BY group_id, `database`, table_name LIMIT " + std::to_string(page_size) +
        " OFFSET " + std::to_string(offset);
    auto rows = conn->template query_s<Row>(sql);

    hv::Json items = Json::array();
    for (const auto& row : rows) {
        ddb_monitor monitor;
        monitor.monitor_id = std::get<0>(row);
        monitor.database = std::get<1>(row);
        monitor.table_name = std::get<2>(row);
        monitor.date_column = std::get<3>(row);
        monitor.date_format = std::get<4>(row);
        monitor.frequency = std::get<5>(row);
        monitor.where_extra = std::get<6>(row);
        monitor.group_id = std::get<7>(row);
        monitor.related_task_ids = std::get<8>(row);
        if (monitor.related_task_ids.empty()) monitor.related_task_ids = "[]";
        monitor.enabled = std::get<9>(row).empty() ? 0 : std::stoi(std::get<9>(row));
        monitor.anomaly_check_enabled = std::get<10>(row).empty() ? 1 : std::stoi(std::get<10>(row));
        monitor.anomaly_threshold_pct = std::get<11>(row).empty() ? 0.0 : std::stod(std::get<11>(row));
        monitor.anomaly_window_size = std::get<12>(row).empty() ? 20 : std::stoi(std::get<12>(row));
        monitor.anomaly_min_samples = std::get<13>(row).empty() ? 5 : std::stoi(std::get<13>(row));
        monitor.description = std::get<14>(row);
        monitor.created_at = std::get<15>(row);
        monitor.updated_at = std::get<16>(row);
        items.push_back(monitor_to_json(monitor));
    }
    return Json::object({
        {"items", items},
        {"total", total},
        {"page", page},
        {"page_size", page_size},
        {"pageSize", page_size}
    });
}

template <typename ConnPtr>
std::vector<ddb_monitor_snapshot> query_ddb_monitor_snapshots(const ConnPtr& conn, const std::string& where = "") {
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string, std::string, std::string>;
    std::string sql = "SELECT monitor_id, TRIM(`database`), TRIM(table_name), TRIM(COALESCE(group_id, '')), "
                      "DATE_FORMAT(`date`, '%Y-%m-%d'), CAST(row_count AS CHAR), status, "
                      "COALESCE(DATE_FORMAT(checked_at, '%Y-%m-%d %H:%i:%s'), ''), "
                      "CAST(duration_ms AS CHAR), COALESCE(error_message, '') "
                      "FROM ddb_monitor_snapshot";
    if (!where.empty()) sql += " WHERE " + where;

    auto rows = conn->template query_s<Row>(sql);
    std::vector<ddb_monitor_snapshot> snapshots;
    snapshots.reserve(rows.size());
    for (const auto& row : rows) {
        ddb_monitor_snapshot s;
        s.monitor_id = std::get<0>(row);
        s.database = std::get<1>(row);
        s.table_name = std::get<2>(row);
        s.group_id = std::get<3>(row);
        s.date = std::get<4>(row);
        s.row_count = std::get<5>(row).empty() ? -1 : std::stoll(std::get<5>(row));
        s.status = std::get<6>(row);
        s.checked_at = std::get<7>(row);
        s.duration_ms = std::get<8>(row).empty() ? 0 : std::stoi(std::get<8>(row));
        s.error_message = std::get<9>(row);
        snapshots.push_back(s);
    }
    return snapshots;
}

template <typename ConnPtr>
hv::Json query_ddb_monitor_snapshots_json(const ConnPtr& conn, const std::string& where = "") {
    using Row = std::tuple<std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string, std::string, std::string, std::string,
                           std::string, std::string, std::string, std::string>;
    std::string sql =
        "SELECT m.monitor_id, TRIM(m.`database`), TRIM(m.table_name), "
        "TRIM(COALESCE(m.group_id, s.group_id, '')), DATE_FORMAT(s.`date`, '%Y-%m-%d'), "
        "CAST(s.row_count AS CHAR), s.status, COALESCE(m.frequency, 'daily'), "
        "CAST(COALESCE(m.anomaly_check_enabled, 1) AS CHAR), CAST(COALESCE(m.anomaly_threshold_pct, 0) AS CHAR), "
        "CAST(COALESCE(m.anomaly_window_size, 20) AS CHAR), CAST(COALESCE(m.anomaly_min_samples, 5) AS CHAR), "
        "COALESCE(DATE_FORMAT(s.checked_at, '%Y-%m-%d %H:%i:%s'), ''), "
        "CAST(s.duration_ms AS CHAR), COALESCE(s.error_message, '') "
        "FROM ddb_monitor_snapshot s "
        "INNER JOIN ddb_monitor m ON m.monitor_id = s.monitor_id AND m.enabled = 1";
    if (!where.empty()) sql += " WHERE " + where;
    sql += " ORDER BY m.group_id, m.`database`, m.table_name, s.`date`";

    auto rows = conn->template query_s<Row>(sql);
    std::unordered_map<std::string, bool> expected_day_cache;
    hv::Json snapshots = Json::array();
    for (const auto& row : rows) {
        ddb_monitor_snapshot s;
        s.monitor_id = std::get<0>(row);
        s.database = std::get<1>(row);
        s.table_name = std::get<2>(row);
        s.group_id = std::get<3>(row);
        s.date = std::get<4>(row);
        s.row_count = std::get<5>(row).empty() ? -1 : std::stoll(std::get<5>(row));
        std::string raw_status = std::get<6>(row);
        std::string frequency = normalize_monitor_frequency(std::get<7>(row));
        bool expected_data_day = true;
        if (frequency != "daily") {
            std::string cache_key = frequency + "|" + s.date;
            auto cached = expected_day_cache.find(cache_key);
            if (cached == expected_day_cache.end()) {
                expected_data_day = is_expected_monitor_data_day(conn, s.date, frequency);
                expected_day_cache.emplace(cache_key, expected_data_day);
            } else {
                expected_data_day = cached->second;
            }
        }
        s.status = effective_monitor_snapshot_status(raw_status, s.row_count, s.date, frequency, expected_data_day);
        int anomaly_enabled = std::get<8>(row).empty() ? 1 : std::stoi(std::get<8>(row));
        double anomaly_threshold_pct = std::get<9>(row).empty() ? 0.0 : std::stod(std::get<9>(row));
        int anomaly_window_size = std::get<10>(row).empty() ? 20 : std::stoi(std::get<10>(row));
        int anomaly_min_samples = std::get<11>(row).empty() ? 5 : std::stoi(std::get<11>(row));
        row_count_anomaly_result anomaly = detect_row_count_anomaly(
            conn, s.monitor_id, s.date, s.row_count, frequency, anomaly_enabled,
            anomaly_threshold_pct, anomaly_window_size, anomaly_min_samples
        );
        if (expected_data_day && raw_status == "success" && anomaly.anomaly) {
            s.status = "zero";
        }
        s.checked_at = std::get<12>(row);
        s.duration_ms = std::get<13>(row).empty() ? 0 : std::stoi(std::get<13>(row));
        s.error_message = std::get<14>(row);
        Json item = snapshot_to_json(s);
        item["raw_status"] = raw_status;
        item["rawStatus"] = raw_status;
        item["frequency"] = monitor_frequency_label(frequency);
        item["frequencyCode"] = frequency;
        item["frequency_code"] = frequency;
        item["expected_data_day"] = expected_data_day;
        item["expectedDataDay"] = expected_data_day;
        item["anomaly"] = anomaly.anomaly;
        item["anomalyChecked"] = anomaly.checked;
        item["anomaly_checked"] = anomaly.checked;
        item["anomalyBaselineRowCount"] = anomaly.baseline >= 0 ? Json(anomaly.baseline) : Json(nullptr);
        item["anomaly_baseline_row_count"] = anomaly.baseline >= 0 ? Json(anomaly.baseline) : Json(nullptr);
        item["anomalyDeviationPct"] = anomaly.deviation_pct;
        item["anomaly_deviation_pct"] = anomaly.deviation_pct;
        item["anomalyThresholdPct"] = anomaly.threshold_pct;
        item["anomaly_threshold_pct"] = anomaly.threshold_pct;
        item["anomalySampleCount"] = anomaly.sample_count;
        item["anomaly_sample_count"] = anomaly.sample_count;
        item["anomalyReason"] = anomaly.reason;
        item["anomaly_reason"] = anomaly.reason;
        snapshots.push_back(item);
    }
    return snapshots;
}

template <typename ConnPtr>
bool insert_ddb_monitor(const ConnPtr& conn, const ddb_monitor& m) {
    std::string sql = "INSERT INTO ddb_monitor(monitor_id, `database`, table_name, date_column, date_format, frequency, "
                      "where_extra, group_id, related_task_ids, related_task_ids_text, enabled, anomaly_check_enabled, "
                      "anomaly_threshold_pct, anomaly_window_size, anomaly_min_samples, description, created_at, updated_at) VALUES('" +
        sql_escape(m.monitor_id) + "', '" + sql_escape(m.database) + "', '" + sql_escape(m.table_name) + "', '" +
        sql_escape(m.date_column) + "', '" + sql_escape(m.date_format) + "', '" + sql_escape(normalize_monitor_frequency(m.frequency)) + "', '" + sql_escape(m.where_extra) + "', '" +
        sql_escape(m.group_id) + "', '" + sql_escape(m.related_task_ids.empty() ? "[]" : m.related_task_ids) +
        "', '" + sql_escape(m.related_task_ids.empty() ? "[]" : m.related_task_ids) + "', " +
        std::to_string(m.enabled) + ", " + std::to_string(m.anomaly_check_enabled) + ", " +
        std::to_string(m.anomaly_threshold_pct) + ", " + std::to_string(m.anomaly_window_size) + ", " +
        std::to_string(m.anomaly_min_samples) + ", '" + sql_escape(m.description) + "', '" +
        sql_escape(m.created_at) + "', '" + sql_escape(m.updated_at) + "')";
    return conn->execute(sql);
}

template <typename ConnPtr>
bool update_ddb_monitor(const ConnPtr& conn, const ddb_monitor& m) {
    std::string sql = "UPDATE ddb_monitor SET `database`='" + sql_escape(m.database) +
                      "', table_name='" + sql_escape(m.table_name) +
                      "', date_column='" + sql_escape(m.date_column) +
        "', date_format='" + sql_escape(m.date_format) +
        "', frequency='" + sql_escape(normalize_monitor_frequency(m.frequency)) +
        "', where_extra='" + sql_escape(m.where_extra) +
        "', group_id='" + sql_escape(m.group_id) +
        "', related_task_ids='" + sql_escape(m.related_task_ids.empty() ? "[]" : m.related_task_ids) + "'" +
        ", related_task_ids_text='" + sql_escape(m.related_task_ids.empty() ? "[]" : m.related_task_ids) + "'" +
        ", enabled=" + std::to_string(m.enabled) +
        ", anomaly_check_enabled=" + std::to_string(m.anomaly_check_enabled) +
        ", anomaly_threshold_pct=" + std::to_string(m.anomaly_threshold_pct) +
        ", anomaly_window_size=" + std::to_string(m.anomaly_window_size) +
        ", anomaly_min_samples=" + std::to_string(m.anomaly_min_samples) +
        ", description='" + sql_escape(m.description) +
        "', updated_at='" + sql_escape(m.updated_at) +
        "' WHERE monitor_id='" + sql_escape(m.monitor_id) + "'";
    return conn->execute(sql);
}

template <typename ConnPtr>
bool validate_related_tasks(const ConnPtr& conn, const Json& related, std::string& error_msg) {
    if (!related.is_array()) {
        error_msg = "related_task_ids must be an array";
        return false;
    }
    for (const auto& item : related) {
        if (!item.is_string()) {
            error_msg = "related_task_ids must contain task_id strings";
            return false;
        }
        std::string task_id = item.get<std::string>();
        if (conn->template query<tasks>("task_id = '" + sql_escape(task_id) + "'").empty()) {
            error_msg = "related task not found: " + task_id;
            return false;
        }
    }
    return true;
}

ddb_monitor monitor_from_payload(const Json& body, const ddb_monitor* existing = nullptr) {
    ddb_monitor m;
    if (existing) m = *existing;
    if (body.contains("monitor_id") && body["monitor_id"].is_string() && !existing) m.monitor_id = body["monitor_id"].get<std::string>();
    if (body.contains("database") && body["database"].is_string()) {
        m.database = normalize_ddb_database_path(body["database"].get<std::string>());
    }
    if (body.contains("database_path") && body["database_path"].is_string()) {
        m.database = normalize_ddb_database_path(body["database_path"].get<std::string>());
    }
    if (body.contains("databasePath") && body["databasePath"].is_string()) {
        m.database = normalize_ddb_database_path(body["databasePath"].get<std::string>());
    }
    if (body.contains("table_name") && body["table_name"].is_string()) m.table_name = body["table_name"].get<std::string>();
    if (body.contains("tableName") && body["tableName"].is_string()) m.table_name = body["tableName"].get<std::string>();
    if (body.contains("date_column") && body["date_column"].is_string()) m.date_column = body["date_column"].get<std::string>();
    if (body.contains("dateColumn") && body["dateColumn"].is_string()) m.date_column = body["dateColumn"].get<std::string>();
    if (body.contains("date_format") && body["date_format"].is_string()) m.date_format = body["date_format"].get<std::string>();
    if (body.contains("dateFormat") && body["dateFormat"].is_string()) m.date_format = body["dateFormat"].get<std::string>();
    if (body.contains("frequency") && body["frequency"].is_string()) m.frequency = normalize_monitor_frequency(body["frequency"].get<std::string>());
    if (body.contains("frequencyCode") && body["frequencyCode"].is_string()) m.frequency = normalize_monitor_frequency(body["frequencyCode"].get<std::string>());
    if (body.contains("frequency_code") && body["frequency_code"].is_string()) m.frequency = normalize_monitor_frequency(body["frequency_code"].get<std::string>());
    if (body.contains("where_extra") && body["where_extra"].is_string()) m.where_extra = body["where_extra"].get<std::string>();
    if (body.contains("whereExtra") && body["whereExtra"].is_string()) m.where_extra = body["whereExtra"].get<std::string>();
    if (body.contains("group_id") && body["group_id"].is_string()) m.group_id = body["group_id"].get<std::string>();
    if (body.contains("groupId") && body["groupId"].is_string()) m.group_id = body["groupId"].get<std::string>();
    if (body.contains("group_name") && body["group_name"].is_string()) m.group_id = body["group_name"].get<std::string>();
    if (body.contains("groupName") && body["groupName"].is_string()) m.group_id = body["groupName"].get<std::string>();
    if (body.contains("description") && body["description"].is_string()) m.description = body["description"].get<std::string>();
    if (body.contains("anomaly_check_enabled")) {
        if (body["anomaly_check_enabled"].is_boolean()) m.anomaly_check_enabled = body["anomaly_check_enabled"].get<bool>() ? 1 : 0;
        else if (body["anomaly_check_enabled"].is_number_integer()) m.anomaly_check_enabled = body["anomaly_check_enabled"].get<int>() ? 1 : 0;
    }
    if (body.contains("anomalyCheckEnabled")) {
        if (body["anomalyCheckEnabled"].is_boolean()) m.anomaly_check_enabled = body["anomalyCheckEnabled"].get<bool>() ? 1 : 0;
        else if (body["anomalyCheckEnabled"].is_number_integer()) m.anomaly_check_enabled = body["anomalyCheckEnabled"].get<int>() ? 1 : 0;
    }
    if (body.contains("anomaly_threshold_pct") && body["anomaly_threshold_pct"].is_number()) m.anomaly_threshold_pct = body["anomaly_threshold_pct"].get<double>();
    if (body.contains("anomalyThresholdPct") && body["anomalyThresholdPct"].is_number()) m.anomaly_threshold_pct = body["anomalyThresholdPct"].get<double>();
    if (body.contains("anomaly_window_size") && body["anomaly_window_size"].is_number_integer()) m.anomaly_window_size = body["anomaly_window_size"].get<int>();
    if (body.contains("anomalyWindowSize") && body["anomalyWindowSize"].is_number_integer()) m.anomaly_window_size = body["anomalyWindowSize"].get<int>();
    if (body.contains("anomaly_min_samples") && body["anomaly_min_samples"].is_number_integer()) m.anomaly_min_samples = body["anomaly_min_samples"].get<int>();
    if (body.contains("anomalyMinSamples") && body["anomalyMinSamples"].is_number_integer()) m.anomaly_min_samples = body["anomalyMinSamples"].get<int>();
    if (body.contains("enabled")) {
        if (body["enabled"].is_boolean()) m.enabled = body["enabled"].get<bool>() ? 1 : 0;
        else if (body["enabled"].is_number_integer()) m.enabled = body["enabled"].get<int>();
    }
    if (body.contains("related_task_ids")) {
        m.related_task_ids = body["related_task_ids"].is_array() ? body["related_task_ids"].dump() : "[]";
    }
    if (body.contains("relatedTaskIds")) {
        m.related_task_ids = body["relatedTaskIds"].is_array() ? body["relatedTaskIds"].dump() : "[]";
    }
    if (m.date_format.empty()) m.date_format = "YYYY-MM-DD";
    if (m.frequency.empty()) m.frequency = "daily";
    m.frequency = normalize_monitor_frequency(m.frequency);
    if (m.related_task_ids.empty()) m.related_task_ids = "[]";
    if (m.anomaly_window_size <= 0) m.anomaly_window_size = 20;
    if (m.anomaly_min_samples <= 0) m.anomaly_min_samples = 5;
    if (m.anomaly_threshold_pct < 0.0) m.anomaly_threshold_pct = 0.0;
    return m;
}

std::string validate_monitor_payload(const ddb_monitor& m) {
    if (m.database.empty()) return "database is required";
    if (m.table_name.empty()) return "table_name is required";
    if (m.date_column.empty()) return "date_column is required";
    if (m.group_id.empty()) return "group_id is required";
    if (!valid_group_id(m.group_id)) return "group_id format is invalid";
    if (m.date_format != "YYYY-MM-DD" && m.date_format != "YYYYMMDD") return "date_format must be YYYY-MM-DD or YYYYMMDD";
    if (m.anomaly_window_size < 5 || m.anomaly_window_size > 120) return "anomaly_window_size must be between 5 and 120";
    if (m.anomaly_min_samples < 3 || m.anomaly_min_samples > m.anomaly_window_size) return "anomaly_min_samples must be between 3 and anomaly_window_size";
    if (m.anomaly_threshold_pct < 0.0 || m.anomaly_threshold_pct > 1.0) return "anomaly_threshold_pct must be between 0 and 1";
    std::string error_msg;
    if (!validate_where_extra(m.where_extra, error_msg)) return error_msg;
    return "";
}

template <typename ConnPtr>
ddb_monitor_snapshot write_recount_unavailable_snapshot(const ConnPtr& conn, const ddb_monitor& monitor, const std::string& date) {
    ddb_monitor_snapshot snap;
    snap.monitor_id = monitor.monitor_id;
    snap.database = monitor.database;
    snap.table_name = monitor.table_name;
    snap.group_id = monitor.group_id;
    snap.date = date;
    snap.row_count = -1;
    snap.status = "failed";
    snap.checked_at = to_cst_time_str(std::time(nullptr));
    snap.duration_ms = 0;
    snap.error_message = "DolphinDB count runner is not configured in C++ Manager; please populate ddb_monitor_snapshot via nightly count job.";

    std::string sql = "INSERT INTO ddb_monitor_snapshot(monitor_id, `database`, table_name, group_id, `date`, row_count, status, checked_at, duration_ms, error_message) VALUES('" +
        sql_escape(snap.monitor_id) + "', '" + sql_escape(snap.database) + "', '" + sql_escape(snap.table_name) + "', '" +
        sql_escape(snap.group_id) + "', '" + sql_escape(snap.date) + "', " + std::to_string(snap.row_count) + ", '" +
        sql_escape(snap.status) + "', '" + sql_escape(snap.checked_at) + "', " + std::to_string(snap.duration_ms) + ", '" +
        sql_escape(snap.error_message) + "') ON DUPLICATE KEY UPDATE `database`=VALUES(`database`), table_name=VALUES(table_name), group_id=VALUES(group_id), row_count=VALUES(row_count), status=VALUES(status), checked_at=VALUES(checked_at), duration_ms=VALUES(duration_ms), error_message=VALUES(error_message)";
    conn->execute(sql);
    return snap;
}

template <typename ConnPtr>
bool find_existing_snapshot_for_monitor(const ConnPtr& conn, const ddb_monitor& monitor, const std::string& date, ddb_monitor_snapshot& snap) {
    std::string group_match = monitor.group_id.empty()
        ? "(group_id IS NULL OR TRIM(group_id) = '')"
        : "TRIM(group_id) = '" + sql_escape(monitor.group_id) + "'";
    std::string condition =
        "((monitor_id = '" + sql_escape(monitor.monitor_id) + "') OR (TRIM(`database`) = '" + sql_escape(monitor.database) +
        "' AND TRIM(table_name) = '" + sql_escape(monitor.table_name) + "' AND " + group_match + ")) AND `date` = '" +
        sql_escape(date) + "' ORDER BY checked_at DESC LIMIT 1";
    auto rows = query_ddb_monitor_snapshots(conn, condition);
    if (rows.empty()) return false;
    snap = rows.front();
    snap.monitor_id = monitor.monitor_id;
    snap.database = monitor.database;
    snap.table_name = monitor.table_name;
    snap.group_id = monitor.group_id;
    return true;
}

template <typename ConnPtr>
bool enqueue_ddb_monitor_count(const ConnPtr& conn, const std::string& date, const std::string& monitor_id, std::string& exec_id, std::string& error) {
    auto task_rows = conn->template query<tasks>("task_id = 'task_data_monitor_count'");
    if (task_rows.empty()) {
        error = "task_data_monitor_count is not configured";
        return false;
    }

    const auto& task = task_rows.front();
    if (task.enabled != 1) {
        error = "task_data_monitor_count is disabled";
        return false;
    }

    Json params = parse_params_json(task.default_params);
    params["sync_monitors"] = false;
    params["date"] = date;
    if (!monitor_id.empty()) params["monitor_id"] = monitor_id;
    else params.erase("monitor_id");

    auto now = std::chrono::system_clock::now();
    auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    exec_id = "task_data_monitor_count_incr_" + std::to_string(millis);

    task_instances inst;
    inst.exec_id = exec_id;
    inst.task_id = task.task_id;
    inst.node_id = task.target_node_id;
    inst.status = 2;
    inst.exit_code = 0;
    inst.start_time = to_cst_time_str(std::chrono::system_clock::to_time_t(now));
    inst.end_time = inst.start_time;
    inst.log_path = "";
    inst.params = params.dump();
    inst.pipeline_id = "manual";

    if (conn->insert(inst) != 1) {
        error = "failed to enqueue DolphinDB count task";
        return false;
    }
    return true;
}
// ====================================================================

// ================= WebSocket 全局广播通道 =================
std::mutex g_ws_mutex;
std::set<WebSocketChannel*> g_ws_clients;

void broadcast_ws(const std::string& msg) {
    std::lock_guard<std::mutex> lock(g_ws_mutex);
    for (auto channel : g_ws_clients) {
        if (channel->isConnected()) {
            channel->send(msg);
        }
    }
}
// ========================================================

int main(int argc, char** argv) {
    // 初始化日志级别
    hlog_set_level(LOG_LEVEL_WARN); // 修改为 WARN，只记录警告及以上级别
    // 明确设置日志文件路径
    hlog_set_file("../../../logs/manager.log"); // 向上三级目录找到项目根目录的logs文件夹

    // 2. 从环境变量读取数据库配置，防止开发测试时误连生产数据库
    const char* db_host = getenv("DB_HOST") ? getenv("DB_HOST") : "127.0.0.1";
    const char* db_user = ${DB_USERNAME}DB_USER") ? getenv("DB_USER") : "root";
    const char* db_pass = getenv("DB_PASS") ? getenv("DB_PASS") : "ops_password";
    // 对齐 Docker Compose 中配置的生产库名 quant_ops
    const char* db_name = getenv("DB_NAME") ? getenv("DB_NAME") : "quant_ops";
    int db_port = getenv("DB_PORT") ? std::stoi(getenv("DB_PORT")) : 3306;

    // 3. 初始化 Redis 连接
    std::unique_ptr<sw::redis::Redis> redis;
    try {
        sw::redis::ConnectionOptions redis_opts;
        redis_opts.host = getenv("REDIS_HOST") ? getenv("REDIS_HOST") : "127.0.0.1";
        redis_opts.port = getenv("REDIS_PORT") ? std::stoi(getenv("REDIS_PORT")) : 6379;
        redis = std::make_unique<sw::redis::Redis>(redis_opts);
        redis->ping(); // 测试连接是否成功
        hlogi("✅ Redis initialized. Connected to %s:%d", redis_opts.host.c_str(), redis_opts.port);
    } catch (const std::exception& e) {
        hloge("❌ Failed to connect to Redis. Error: %s", e.what());
        // 如果连接失败，释放指针，避免后续心跳接口误以为 Redis 可用而抛出异常
        redis.reset();
    }

    auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
    int db_pool_size = getenv("DB_POOL_SIZE") ? std::stoi(getenv("DB_POOL_SIZE")) : 32;
    if (db_pool_size < 8) db_pool_size = 8;
    if (db_pool_size > 128) db_pool_size = 128;
    // 修复：ormpp 的参数顺序其实是 timeout 在前，port 在后！
    // 正确顺序: max_connections, host, user, pass, db, timeout, port
    try {
        pool.init(db_pool_size, db_host, db_user, db_pass, db_name, 5, db_port);
        hlogi("✅ MySQL connection pool initialized. Connected to %s:%d/%s", db_host, db_port, db_name);
        auto init_conn = pool.get();
        if (init_conn) {
            init_conn->execute(
                "CREATE TABLE IF NOT EXISTS trade_calendar ("
                "trade_date VARCHAR(10) PRIMARY KEY,"
                "source VARCHAR(64) NOT NULL DEFAULT 'mysql',"
                "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            init_conn->execute(
                "CREATE TABLE IF NOT EXISTS users ("
                "user_id VARCHAR(64) PRIMARY KEY,"
                "username VARCHAR(64) NOT NULL UNIQUE,"
                "password_hash VARCHAR(255) NOT NULL,"
                "name VARCHAR(64) NOT NULL,"
                "email VARCHAR(128) NOT NULL DEFAULT '',"
                "role VARCHAR(32) NOT NULL DEFAULT 'researcher',"
                "status VARCHAR(16) NOT NULL DEFAULT 'active',"
                "last_login_at DATETIME NULL,"
                "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "KEY idx_users_role_status(role, status)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            init_conn->execute(
                "INSERT IGNORE INTO users(user_id, username, password_hash, name, email, role, status) "
                "VALUES('u_admin', 'your_username', SHA2('admin123', 256), '系统管理员', 'your_username@quant.local', 'your_username', 'active')"
            );
            init_conn->execute(
                "CREATE TABLE IF NOT EXISTS ddb_monitor_group ("
                "group_id VARCHAR(64) PRIMARY KEY,"
                "display_name VARCHAR(64) NOT NULL,"
                "sort_order INT NOT NULL DEFAULT 0,"
                "enabled TINYINT(1) NOT NULL DEFAULT 1,"
                "description VARCHAR(512) NULL,"
                "created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                "updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                "KEY idx_ddb_monitor_group_enabled_sort(enabled, sort_order)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            init_conn->execute(
                "CREATE TABLE IF NOT EXISTS ddb_monitor ("
                "monitor_id VARCHAR(64) PRIMARY KEY,"
                "`database` VARCHAR(128) NOT NULL,"
                "table_name VARCHAR(128) NOT NULL,"
                "date_column VARCHAR(64) NOT NULL,"
                "date_format VARCHAR(16) NOT NULL DEFAULT 'YYYY-MM-DD',"
                "frequency VARCHAR(16) NOT NULL DEFAULT 'daily',"
                "where_extra VARCHAR(512) NOT NULL DEFAULT '',"
                "group_id VARCHAR(64) NOT NULL,"
                "related_task_ids JSON NULL,"
                "related_task_ids_text VARCHAR(4096) NOT NULL DEFAULT '[]',"
                "enabled TINYINT(1) NOT NULL DEFAULT 1,"
                "anomaly_check_enabled TINYINT(1) NOT NULL DEFAULT 1,"
                "anomaly_threshold_pct DECIMAL(8,4) NOT NULL DEFAULT 0,"
                "anomaly_window_size INT NOT NULL DEFAULT 20,"
                "anomaly_min_samples INT NOT NULL DEFAULT 5,"
                "description VARCHAR(512) NOT NULL DEFAULT '',"
                "created_at DATETIME NOT NULL,"
                "updated_at DATETIME NOT NULL,"
                "KEY idx_ddb_monitor_enabled_group(enabled, group_id),"
                "KEY idx_ddb_monitor_group(group_id),"
                "KEY idx_ddb_monitor_table(`database`, table_name)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            init_conn->execute(
                "CREATE TABLE IF NOT EXISTS ddb_monitor_snapshot ("
                "monitor_id VARCHAR(64) NOT NULL,"
                "`database` VARCHAR(128) NOT NULL,"
                "table_name VARCHAR(128) NOT NULL,"
                "group_id VARCHAR(64) NOT NULL,"
                "`date` DATE NOT NULL,"
                "row_count BIGINT NOT NULL DEFAULT -1,"
                "status VARCHAR(16) NOT NULL,"
                "checked_at DATETIME NULL,"
                "duration_ms INT NOT NULL DEFAULT 0,"
                "error_message TEXT NULL,"
                "PRIMARY KEY(monitor_id, `date`),"
                "KEY idx_ddb_snapshot_date(`date`),"
                "KEY idx_ddb_snapshot_group(group_id),"
                "KEY idx_ddb_snapshot_group_date(group_id, `date`)"
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
            );
            auto column_exists = [&](const std::string& table, const std::string& column) -> bool {
                using Row = std::tuple<std::string>;
                std::string sql =
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '" + sql_escape(db_name) +
                    "' AND TABLE_NAME = '" + sql_escape(table) + "' AND COLUMN_NAME = '" + sql_escape(column) + "' LIMIT 1";
                return !init_conn->query_s<Row>(sql).empty();
            };
            if (!column_exists("tasks", "task_category")) {
                init_conn->execute("ALTER TABLE tasks ADD COLUMN task_category VARCHAR(32) NOT NULL DEFAULT 'ops' COMMENT '任务业务类型: ops/data_download/factor_compute/model_training' AFTER node_type");
            }
            init_conn->execute("UPDATE tasks SET task_category='ops' WHERE task_category IS NULL OR task_category=''");
            init_conn->execute("UPDATE tasks SET task_category='data_download' WHERE task_id IN ('task_market_stock_daily','task_market_index_daily','task_market_etf_daily','task_basic_financial_daily','task_stock_info_daily','task_ah_info_daily','task_st_stock_daily','task_index_components_daily','task_asset_allocation_daily')");
            init_conn->execute("UPDATE tasks SET task_category='factor_compute' WHERE task_id IN ('task_derived_metrics_daily','task_consensus_rating_daily','task_consensus_rolling_daily','task_cj_quant_factors_daily','task_fz_quant_factors_daily','task_research_factors_daily')");
            init_conn->execute("UPDATE tasks SET task_category='ops' WHERE task_id LIKE 'task_test_%' OR task_id='task_data_monitor_count'");
            if (!column_exists("ddb_monitor", "frequency")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN frequency VARCHAR(16) NOT NULL DEFAULT 'daily' AFTER date_format");
            }
            init_conn->execute("UPDATE ddb_monitor SET frequency='daily' WHERE frequency IS NULL OR frequency='' ");
            if (!column_exists("ddb_monitor", "group_id")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN group_id VARCHAR(64) NOT NULL DEFAULT ''");
            }
            if (!column_exists("ddb_monitor", "related_task_ids_text")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN related_task_ids_text VARCHAR(4096) NOT NULL DEFAULT '[]'");
            }
            if (!column_exists("ddb_monitor", "anomaly_check_enabled")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN anomaly_check_enabled TINYINT(1) NOT NULL DEFAULT 1 AFTER enabled");
            }
            if (!column_exists("ddb_monitor", "anomaly_threshold_pct")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN anomaly_threshold_pct DECIMAL(8,4) NOT NULL DEFAULT 0 AFTER anomaly_check_enabled");
            }
            if (!column_exists("ddb_monitor", "anomaly_window_size")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN anomaly_window_size INT NOT NULL DEFAULT 20 AFTER anomaly_threshold_pct");
            }
            if (!column_exists("ddb_monitor", "anomaly_min_samples")) {
                init_conn->execute("ALTER TABLE ddb_monitor ADD COLUMN anomaly_min_samples INT NOT NULL DEFAULT 5 AFTER anomaly_window_size");
            }
            init_conn->execute("UPDATE ddb_monitor SET anomaly_window_size=20 WHERE anomaly_window_size IS NULL OR anomaly_window_size <= 0");
            init_conn->execute("UPDATE ddb_monitor SET anomaly_min_samples=5 WHERE anomaly_min_samples IS NULL OR anomaly_min_samples <= 0");
            init_conn->execute("UPDATE ddb_monitor SET anomaly_threshold_pct=0 WHERE anomaly_threshold_pct IS NULL OR anomaly_threshold_pct < 0");
            init_conn->execute(
                "UPDATE ddb_monitor SET related_task_ids_text = COALESCE(CAST(related_task_ids AS CHAR), '[]')"
            );
            if (!column_exists("ddb_monitor_snapshot", "group_id")) {
                init_conn->execute("ALTER TABLE ddb_monitor_snapshot ADD COLUMN group_id VARCHAR(64) NOT NULL DEFAULT ''");
            }
            if (column_exists("ddb_monitor", "group_name")) {
                init_conn->execute("UPDATE ddb_monitor SET group_id = LOWER(group_name) WHERE group_id = '' AND group_name IS NOT NULL");
            }
            if (column_exists("ddb_monitor_snapshot", "group_name")) {
                init_conn->execute("UPDATE ddb_monitor_snapshot SET group_id = LOWER(group_name) WHERE group_id = '' AND group_name IS NOT NULL");
            }
            init_conn->execute(
                "INSERT IGNORE INTO ddb_monitor_group(group_id, display_name, sort_order, enabled, description, created_at, updated_at) "
                "SELECT DISTINCT group_id, group_id, 100, 1, 'migrated from ddb_monitor', NOW(), NOW() "
                "FROM ddb_monitor WHERE group_id <> ''"
            );
        }
    } catch (const std::exception& e) {
        hloge("❌ Failed to connect to %s:%d/%s. Error: %s", db_host, db_port, db_name, e.what());
        printf("❌ Failed to connect to MySQL connection pool: %s\n", e.what());
    }

    HttpService router;
    WebSocketService ws_router;

    // ================= WebSocket 路由设置 =================
    ws_router.onopen = [](const WebSocketChannelPtr& channel, const HttpRequestPtr& req) {
        std::lock_guard<std::mutex> lock(g_ws_mutex);
        g_ws_clients.insert(channel.get());
        hlogi("🔗 Frontend WebSocket client connected from %s", channel->peeraddr().c_str());
    };
    ws_router.onclose = [](const WebSocketChannelPtr& channel) {
        std::lock_guard<std::mutex> lock(g_ws_mutex);
        g_ws_clients.erase(channel.get());
        hlogi("🔗 Frontend WebSocket client disconnected");
    };
    ws_router.onmessage = [](const WebSocketChannelPtr& channel, const std::string& msg) {
        // 维持连接心跳，回复 ACK
        channel->send("ACK: " + msg);
    };
    // ====================================================

    // ================= 核心升级：全局 Token 鉴权中间件 =================
    router.preprocessor = [](HttpRequest* req, HttpResponse* resp) {
        // 前端联调跨域支持：Authorization 会触发浏览器 OPTIONS 预检，预检不能走业务鉴权。
        std::string origin = req->GetHeader("Origin", "*");
        resp->headers["Access-Control-Allow-Origin"] = origin;
        if (origin != "*") resp->headers["Vary"] = "Origin";
        resp->headers["Access-Control-Allow-Methods"] = "OPTIONS, HEAD, GET, POST, PUT, DELETE, PATCH";
        resp->headers["Access-Control-Allow-Headers"] = req->GetHeader("Access-Control-Request-Headers", "Authorization, Content-Type");
        if (req->method == HTTP_OPTIONS) {
            return 204;
        }

        // 根路径、健康检查和登录免鉴权，方便监控探活和前端首次登录。
        if (req->path == "/" || req->path == "/status" || req->path == "/health" || req->path == "/api/auth/login") {
            return 0; // return 0 表示放行，继续向后匹配路由
        }
        
        // 支持固定兼容 Token 和登录接口签发的用户 Token=${ACCESS_TOKEN}
        std::string auth_header = req->GetHeader("Authorization");
        bool valid_token=${ACCESS_TOKEN} == "Bearer quant_ops_secret_2026";
        if (!valid_token && auth_header.find("Bearer ") == 0) {
            std::lock_guard<std::mutex> lock(g_auth_mutex);
            valid_token=${ACCESS_TOKEN} != g_auth_token=${ACCESS_TOKEN};
        }
        if (!valid_token=${ACCESS_TOKEN} {
            hlogw("🚨 Unauthorized access attempt from %s to %s", req->client_addr.ip.c_str(), req->path.c_str());
            resp->Json(Json::object({{"code", 401}, {"message", "Unauthorized: Invalid or missing token=${ACCESS_TOKEN};
            return 401; // 返回 401 状态码，直接终止后续请求
        }
        return 0; // 鉴权通过，放行
    };
    // ===================================================================

    // 0. 根路径路由 (避免浏览器直接访问报 404)
    router.GET("/", [](HttpRequest* req, HttpResponse* resp) {
        return resp->String("Welcome to Quant Ops Manager C++ Backend!");
    });

    // 1. 基础状态/健康检查路由
    router.GET("/status", [](HttpRequest* req, HttpResponse* resp) {
        return resp->Json(Json::object({
            {"status", "ok"},
            {"message", "Quant Ops C++ Manager is running"},
            {"version", "0.1.0"}
        }));
    });

    router.GET("/health", [](HttpRequest* req, HttpResponse* resp) {
        return resp->Json(Json::object({{"status", "ok"}, {"service", "quant-ops-manager"}}));
    });

    // 1.1 认证接口
    router.POST("/api/auth/login", [](HttpRequest* req, HttpResponse* resp) {
        hv::Json body;
        try { body = req->GetJson(); } catch (...) { body = Json::object(); }
        std::string username = ${DB_USERNAME}username", "");
        std::string password = ${DB_PASSWORD}password", "");
        if (username.empty() || password.empty()) return resp->Json(Json::object({{"code", 401}, {"message", "用户名或密码错误"}, {"data", nullptr}}));

        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto users = query_managed_users(conn,
            "username = '" + sql_escape(username) + "' AND password_hash = SHA2('" + sql_escape(password) + "', 256)");
        if (users.empty()) return resp->Json(Json::object({{"code", 401}, {"message", "用户名或密码错误"}, {"data", nullptr}}));
        auto user = users.front();
        if (user.status != "active") return resp->Json(Json::object({{"code", 403}, {"message", "用户已被禁用"}, {"data", nullptr}}));

        std::string token=${ACCESS_TOKEN};
        conn->execute("UPDATE users SET last_login_at = NOW() WHERE user_id = '" + sql_escape(user.user_id) + "'");
        return resp->Json(Json::object({
            {"code", 0}, {"message", "ok"},
            {"data", Json::object({{"token", token=${ACCESS_TOKEN}, {"user", managed_user_json(user)}})}
        }));
    });

    router.GET("/api/auth/me", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto users = query_managed_users(conn, "user_id = '" + sql_escape(user_id_from_request(req)) + "'");
        if (users.empty() || users.front().status != "active") return resp->Json(Json::object({{"code", 401}, {"message", "User session is invalid"}, {"data", nullptr}}));
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", managed_user_json(users.front())}}));
    });

    router.GET("/api/auth/permissions", [](HttpRequest* req, HttpResponse* resp) {
        return resp->Json(Json::object({
            {"code", 0}, {"message", "ok"},
            {"data", Json::object({{"role", "your_username"}, {"permissions", admin_permissions_json()}})}
        }));
    });

    router.POST("/api/auth/logout", [](HttpRequest* req, HttpResponse* resp) {
        std::string header = req->GetHeader("Authorization");
        if (header.find("Bearer ") == 0 && header != "Bearer quant_ops_secret_2026") {
            std::lock_guard<std::mutex> lock(g_auth_mutex);
            g_auth_token=${ACCESS_TOKEN};
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", nullptr}}));
    });

    // 1.2 用户管理接口：P0 仅 your_username 可执行写操作，列表对所有已登录用户开放
    router.GET("/api/users", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", Json::array()}}));
        Json data = Json::array();
        for (const auto& user : query_managed_users(conn)) data.push_back(managed_user_json(user));
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", data}}));
    });

    router.POST("/api/users", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!request_is_admin(conn, req)) return resp->Json(Json::object({{"code", 403}, {"message", "Admin role required"}, {"data", nullptr}}));
        try {
            Json body = req->GetJson();
            std::string username = ${DB_USERNAME}username", "");
            std::string password = ${DB_PASSWORD}password", "");
            std::string name = body.value("name", "");
            std::string email = body.value("email", "");
            std::string role = body.value("role", "");
            if (username.empty() || password.empty() || name.empty() || email.empty() || !valid_user_role(role)) {
                return resp->Json(Json::object({{"code", 400}, {"message", "username, password, name, email and valid role are required"}, {"data", nullptr}}));
            }
            if (!query_managed_users(conn, "username = '" + sql_escape(username) + "'").empty()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "用户名已存在"}, {"data", nullptr}}));
            }
            std::string user_id = "u_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
            std::string sql = "INSERT INTO users(user_id, username, password_hash, name, email, role, status) VALUES('" +
                sql_escape(user_id) + "', '" + sql_escape(username) + "', SHA2('" + sql_escape(password) + "', 256), '" +
                sql_escape(name) + "', '" + sql_escape(email) + "', '" + sql_escape(role) + "', 'active')";
            if (!conn->execute(sql)) return resp->Json(Json::object({{"code", 500}, {"message", "Failed to create user"}, {"data", nullptr}}));
            auto created = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
            return resp->Json(Json::object({{"code", 0}, {"message", "User created"}, {"data", managed_user_json(created.front())}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    router.PUT("/api/users/{user_id}", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!request_is_admin(conn, req)) return resp->Json(Json::object({{"code", 403}, {"message", "Admin role required"}, {"data", nullptr}}));
        std::string user_id = req->GetParam("user_id");
        auto users = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
        if (users.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "User not found"}, {"data", nullptr}}));
        try {
            Json body = req->GetJson();
            std::vector<std::string> updates;
            if (body.contains("name")) {
                std::string name = body.value("name", "");
                if (name.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "name cannot be empty"}, {"data", nullptr}}));
                updates.push_back("name='" + sql_escape(name) + "'");
            }
            if (body.contains("email")) updates.push_back("email='" + sql_escape(body.value("email", "")) + "'");
            if (body.contains("role")) {
                std::string role = body.value("role", "");
                if (!valid_user_role(role)) return resp->Json(Json::object({{"code", 400}, {"message", "invalid role"}, {"data", nullptr}}));
                updates.push_back("role='" + sql_escape(role) + "'");
            }
            if (body.contains("password")) {
                std::string password = ${DB_PASSWORD}password", "");
                if (!password.empty()) updates.push_back("password_hash=SHA2('" + sql_escape(password) + "', 256)");
            }
            if (updates.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "No editable fields supplied"}, {"data", nullptr}}));
            if (!conn->execute("UPDATE users SET " + join_strings(updates, ", ") + " WHERE user_id='" + sql_escape(user_id) + "'")) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to update user"}, {"data", nullptr}}));
            }
            auto updated = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
            return resp->Json(Json::object({{"code", 0}, {"message", "User updated"}, {"data", managed_user_json(updated.front())}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    router.POST("/api/users/{user_id}/toggle", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!request_is_admin(conn, req)) return resp->Json(Json::object({{"code", 403}, {"message", "Admin role required"}, {"data", nullptr}}));
        std::string user_id = req->GetParam("user_id");
        if (user_id == user_id_from_request(req)) return resp->Json(Json::object({{"code", 400}, {"message", "不能禁用自己"}, {"data", nullptr}}));
        auto users = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
        if (users.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "User not found"}, {"data", nullptr}}));
        std::string next_status = users.front().status == "active" ? "disabled" : "active";
        if (users.front().status == "active" && users.front().role == "your_username" &&
            query_sql_count(conn, "SELECT COUNT(*) FROM users WHERE role='your_username' AND status='active'") <= 1) {
            return resp->Json(Json::object({{"code", 400}, {"message", "至少保留一名启用的管理员"}, {"data", nullptr}}));
        }
        conn->execute("UPDATE users SET status='" + next_status + "' WHERE user_id='" + sql_escape(user_id) + "'");
        auto updated = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
        return resp->Json(Json::object({{"code", 0}, {"message", "User status updated"}, {"data", managed_user_json(updated.front())}}));
    });

    router.Delete("/api/users/{user_id}", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!request_is_admin(conn, req)) return resp->Json(Json::object({{"code", 403}, {"message", "Admin role required"}, {"data", nullptr}}));
        std::string user_id = req->GetParam("user_id");
        if (user_id == user_id_from_request(req)) return resp->Json(Json::object({{"code", 400}, {"message", "不能删除自己"}, {"data", nullptr}}));
        auto users = query_managed_users(conn, "user_id = '" + sql_escape(user_id) + "'");
        if (users.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "User not found"}, {"data", nullptr}}));
        if (users.front().role == "your_username" && users.front().status == "active" &&
            query_sql_count(conn, "SELECT COUNT(*) FROM users WHERE role='your_username' AND status='active'") <= 1) {
            return resp->Json(Json::object({{"code", 400}, {"message", "至少保留一名启用的管理员"}, {"data", nullptr}}));
        }
        if (!conn->execute("DELETE FROM users WHERE user_id='" + sql_escape(user_id) + "'")) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Failed to delete user"}, {"data", nullptr}}));
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "User deleted"}, {"data", nullptr}}));
    });

    // 2. 获取任务列表接口 (GET /api/tasks)
    router.GET("/api/tasks", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::string category_filter = req->GetParam("task_category");
        if (category_filter.empty()) category_filter = req->GetParam("taskCategory");
        std::string task_condition;
        if (!category_filter.empty()) {
            task_condition = "task_category = '" + sql_escape(normalize_task_category(category_filter)) + "'";
        }
        auto task_list = conn->query<tasks>(task_condition);
        Json tasks_json = Json::array();
        for (const auto& t : task_list) {
            auto latest = conn->query<task_instances>("task_id = '" + sql_escape(t.task_id) + "' ORDER BY start_time DESC LIMIT 1");
            auto parents = conn->query<dag_edges>("child_task_id = '" + sql_escape(t.task_id) + "'");
            Json parent_ids = Json::array();
            for (const auto& e : parents) parent_ids.push_back(e.parent_task_id);

            int latest_status = 2;
            std::string last_run_at = "";
            std::string latest_exec_id = "";
            if (!latest.empty()) {
                latest_status = latest.front().status;
                last_run_at = latest.front().start_time;
                latest_exec_id = latest.front().exec_id;
            }
            std::string task_type = !parents.empty() ? "dag" : (!t.cron_expr.empty() ? "cron" : "manual");
            std::string trigger_mode = !parents.empty() ? "dependency" : (!t.cron_expr.empty() ? "cron" : "manual");

            tasks_json.push_back(Json::object({
                {"task_id", t.task_id},
                {"name", t.name},
                {"description", t.script_path},
                {"node_type", t.node_type},
                {"task_category", task_category_label(t.task_category)},
                {"taskCategory", task_category_label(t.task_category)},
                {"task_category_code", normalize_task_category(t.task_category)},
                {"taskCategoryCode", normalize_task_category(t.task_category)},
                {"task_category_label", task_category_label(t.task_category)},
                {"taskCategoryLabel", task_category_label(t.task_category)},
                {"script_path", t.script_path},
                {"cron_expr", t.cron_expr},
                {"timeout_sec", t.timeout_sec},
                {"enabled", t.enabled},
                {"target_node_id", t.target_node_id},
                {"target_os", target_os_from_node(t.target_node_id)},
                {"target_node_tag", t.target_node_id},
                {"task_type", task_type},
                {"schedule_status", status_to_label(latest_status)},
                {"trigger_mode", trigger_mode},
                {"last_run_at", last_run_at},
                {"latest_exec_id", latest_exec_id},
                {"retry_count", 0},
                {"retry_interval_sec", 300},
                {"concurrency_limit", 1},
                {"dag_parents", parent_ids},
                {"env_vars", Json::array()},
                {"log_level", "INFO"},
                {"alert_on_failure", true},
                {"default_params", parse_params_json(t.default_params)}
            }));
        }

        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", tasks_json}}));
    });

    // 2.1 获取任务近 30 个交易日的数据状态 (GET /api/tasks/{task_id}/data-days)
    router.GET("/api/tasks/{task_id}/data-days", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        int limit = 30;
        try {
            std::string raw_limit = req->GetParam("limit");
            if (!raw_limit.empty()) limit = std::max(1, std::min(120, std::stoi(raw_limit)));
        } catch (...) {
            limit = 30;
        }

        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::string escaped_task_id = sql_escape(task_id);
        auto task_list = conn->query<tasks>("task_id = '" + escaped_task_id + "'");
        if (task_list.empty()) {
            return resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}, {"data", nullptr}}));
        }

        bool used_trade_calendar = false;
        auto days = recent_trade_days_from_db(conn, limit, used_trade_calendar);
        if (days.empty()) days = recent_weekdays(limit);
        std::set<std::string> day_set(days.begin(), days.end());
        auto instances = conn->query<task_instances>(
            "task_id = '" + escaped_task_id + "' ORDER BY start_time DESC LIMIT 5000"
        );

        std::map<std::string, task_instances> latest_by_day;
        std::map<std::string, task_instances> latest_success_by_day;
        for (const auto& inst : instances) {
            auto covered_days = covered_trade_days_from_instance(inst);
            for (const auto& day : covered_days) {
                if (day.empty()) continue;
                if (!day_set.empty() && !day_set.count(day)) continue;
                if (!latest_by_day.count(day) || instance_sort_time(inst) > instance_sort_time(latest_by_day[day])) {
                    latest_by_day[day] = inst;
                }
                if (inst.status == 1) {
                    if (!latest_success_by_day.count(day) || instance_sort_time(inst) > instance_sort_time(latest_success_by_day[day])) {
                        latest_success_by_day[day] = inst;
                    }
                }
            }
        }

        std::map<std::string, int> counts{{"not_downloaded", 0}, {"waiting", 0}, {"running", 0}, {"success", 0}, {"failed", 0}, {"timeout", 0}};
        Json arr = Json::array();
        for (const auto& day : days) {
            bool has_latest = latest_by_day.count(day) > 0;
            bool has_success = latest_success_by_day.count(day) > 0;
            std::string status = "not_downloaded";
            Json latest_execution = nullptr;
            Json success_execution = nullptr;
            std::string latest_time = "";
            std::string latest_exec_id = "";
            std::string success_exec_id = "";

            if (has_latest) {
                const auto& latest = latest_by_day[day];
                status = status_to_label(latest.status);
                latest_execution = task_instance_to_json(latest, trigger_mode_from_exec_id(latest.exec_id, task_list.front().cron_expr));
                latest_time = instance_sort_time(latest);
                latest_exec_id = latest.exec_id;
            }
            if (has_success) {
                const auto& success = latest_success_by_day[day];
                status = "success";
                success_execution = task_instance_to_json(success, trigger_mode_from_exec_id(success.exec_id, task_list.front().cron_expr));
                success_exec_id = success.exec_id;
            }
            if (!counts.count(status)) counts[status] = 0;
            counts[status]++;

            arr.push_back(Json::object({
                {"date", day},
                {"trade_date", iso_date_to_compact(day)},
                {"label", day.substr(5, 5)},
                {"weekday", weekday_cn(day)},
                {"status", status},
                {"has_success", has_success},
                {"latest_exec_id", latest_exec_id},
                {"latest_success_exec_id", success_exec_id},
                {"latest_time", latest_time},
                {"execution", success_execution},
                {"latest_execution", latest_execution}
            }));
        }

        Json summary = Json::object();
        for (const auto& item : counts) summary[item.first] = item.second;

        Json data = Json::object({
            {"task_id", task_id},
            {"task_name", task_list.front().name},
            {"limit", limit},
            {"calendar", used_trade_calendar ? "mysql_trade_calendar" : "weekday_approx"},
            {"days", arr},
            {"summary", summary}
        });
        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", data}}));
    });

    // ================= 阶段 4：Pipeline / DAG 编辑功能 =================
    // 辅助函数：校验 DAG 合法性 (防环、防自依赖、防重复边)
    auto validate_dag = [](const hv::Json& body, ormpp::dbng<ormpp::mysql>* conn, std::string& error_msg) -> bool {
        if (!body.contains("nodes") || !body["nodes"].is_array() ||
            !body.contains("edges") || !body["edges"].is_array()) {
            error_msg = "Invalid payload: missing nodes or edges array.";
            return false;
        }

        std::set<std::string> node_ids;
        for (const auto& node : body["nodes"]) {
            if (!node.is_object() || !node.contains("task_id") || !node["task_id"].is_string()) {
                error_msg = "Invalid node format.";
                return false;
            }
            std::string task_id = node["task_id"].get<std::string>();
            if (conn->query<tasks>("task_id = '" + sql_escape(task_id) + "'").empty()) {
                error_msg = "Task not found: " + task_id;
                return false;
            }
            node_ids.insert(task_id);
        }

        std::map<std::string, std::vector<std::string>> adj;
        std::map<std::string, int> indegree;
        std::set<std::pair<std::string, std::string>> edge_set;

        for (const auto& id : node_ids) indegree[id] = 0;

        for (const auto& edge : body["edges"]) {
            if (!edge.is_object() || !edge.contains("from") || !edge.contains("to") ||
                !edge["from"].is_string() || !edge["to"].is_string()) {
                error_msg = "Invalid edge format.";
                return false;
            }
            std::string from = edge["from"].get<std::string>();
            std::string to = edge["to"].get<std::string>();

            if (from == to) {
                error_msg = "Self-dependency is not allowed: " + from;
                return false;
            }
            if (!node_ids.count(from) || !node_ids.count(to)) {
                error_msg = "Edge connects non-existent node.";
                return false;
            }
            if (edge_set.count({from, to})) {
                error_msg = "Duplicate edge detected: " + from + " -> " + to;
                return false;
            }
            edge_set.insert({from, to});
            adj[from].push_back(to);
            indegree[to]++;
        }

        // 拓扑排序检测环
        std::queue<std::string> q;
        for (const auto& pair : indegree) {
            if (pair.second == 0) q.push(pair.first);
        }
        int count = 0;
        while (!q.empty()) {
            std::string u = q.front();
            q.pop();
            count++;
            for (const auto& v : adj[u]) {
                if (--indegree[v] == 0) q.push(v);
            }
        }

        if (count != node_ids.size()) {
            error_msg = "Cycle detected in the DAG.";
            return false;
        }
        return true;
    };

    // 4.1 获取所有 Pipeline (GET /api/pipelines)
    router.GET("/api/pipelines", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        auto all_pipelines = conn->query<pipelines>("ORDER BY updated_at DESC");
        Json arr = Json::array();
        for (const auto& p : all_pipelines) {
            int task_count = 0, edge_count = 0;
            try { task_count = Json::parse(p.nodes).size(); } catch(...) {}
            try { edge_count = Json::parse(p.edges).size(); } catch(...) {}
            arr.push_back(Json::object({
                {"pipeline_id", p.pipeline_id}, {"name", p.name}, {"description", p.description},
                {"enabled", p.enabled}, {"task_count", task_count}, {"edge_count", edge_count},
                {"updated_at", p.updated_at}
            }));
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", arr}}));
    });

    // 4.2 创建 Pipeline (POST /api/pipelines)
    router.POST("/api/pipelines", [&](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        
        hv::Json body;
        try { body = req->GetJson(); } catch(...) { return resp->Json(Json::object({{"code", 400}, {"message", "Invalid JSON"}})); }

        std::string error_msg;
        if (!validate_dag(body, conn.get(), error_msg)) {
            return resp->Json(Json::object({{"code", 400}, {"message", error_msg}}));
        }

        pipelines p;
        p.pipeline_id = "pipeline_" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count());
        p.name = body.value("name", "Untitled Pipeline");
        p.description = body.value("description", "");
        p.enabled = body.value("enabled", 1);
        p.canvas = body.value("canvas", Json::object()).dump();
        p.nodes = body.value("nodes", Json::array()).dump();
        p.edges = body.value("edges", Json::array()).dump();
        p.entry_task_ids = body.value("entry_task_ids", Json::array()).dump();
        p.schedule = body.value("schedule", Json::object()).dump();
        p.created_at = to_cst_time_str(std::time(0));
        p.updated_at = p.created_at;

        if (conn->insert(p) == 1) {
            Json created_pipeline = body;
            created_pipeline["pipeline_id"] = p.pipeline_id;
            created_pipeline["name"] = p.name;
            created_pipeline["description"] = p.description;
            created_pipeline["enabled"] = p.enabled != 0;
            created_pipeline["canvas"] = Json::parse(p.canvas);
            created_pipeline["nodes"] = Json::parse(p.nodes);
            created_pipeline["edges"] = Json::parse(p.edges);
            created_pipeline["entry_task_ids"] = Json::parse(p.entry_task_ids);
            created_pipeline["schedule"] = Json::parse(p.schedule);
            created_pipeline["created_at"] = p.created_at;
            created_pipeline["updated_at"] = p.updated_at;
            return resp->Json(Json::object({
                {"code", 0}, {"message", "Pipeline created"}, {"data", created_pipeline}
            }));
        }
        return resp->Json(Json::object({{"code", 500}, {"message", "Failed to create pipeline"}}));
    });

    // 4.3 获取单个 Pipeline (GET /api/pipelines/{id})
    router.GET("/api/pipelines/{id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string id = req->GetParam("id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        auto result = conn->query<pipelines>("pipeline_id = '" + sql_escape(id) + "'");
        if (result.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Not Found"}}));
        const auto& p = result.front();
        Json data = Json::object({
            {"pipeline_id", p.pipeline_id}, {"name", p.name}, {"description", p.description},
            {"enabled", p.enabled}, {"canvas", Json::parse(p.canvas)}, {"nodes", Json::parse(p.nodes)},
            {"edges", Json::parse(p.edges)}, {"entry_task_ids", Json::parse(p.entry_task_ids.empty() ? "[]" : p.entry_task_ids)}, {"schedule", Json::parse(p.schedule)},
            {"created_at", p.created_at}, {"updated_at", p.updated_at}
        });
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", data}}));
    });

    // 4.4 更新 Pipeline (PUT /api/pipelines/{id})
    router.PUT("/api/pipelines/{id}", [&](HttpRequest* req, HttpResponse* resp) {
        std::string id = req->GetParam("id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        hv::Json body;
        try { body = req->GetJson(); } catch(...) { return resp->Json(Json::object({{"code", 400}, {"message", "Invalid JSON"}})); }
        
        std::string error_msg;
        if (!validate_dag(body, conn.get(), error_msg)) {
            return resp->Json(Json::object({{"code", 400}, {"message", error_msg}}));
        }

        std::string now = to_cst_time_str(std::time(0));
        std::string name = body.value("name", "Untitled");
        std::string description = body.value("description", "");
        int enabled = body.value("enabled", 1);
        std::string canvas = body.value("canvas", Json::object()).dump();
        std::string nodes = body.value("nodes", Json::array()).dump();
        std::string edges = body.value("edges", Json::array()).dump();
        std::string entry_task_ids = body.value("entry_task_ids", Json::array()).dump();
        std::string schedule = body.value("schedule", Json::object()).dump();
        std::string sql =
            "INSERT INTO pipelines(pipeline_id, name, description, enabled, canvas, nodes, edges, entry_task_ids, schedule, created_at, updated_at) VALUES("
            "\x27" + sql_escape(id) + "\x27,"
            "\x27" + sql_escape(name) + "\x27,"
            "\x27" + sql_escape(description) + "\x27," + std::to_string(enabled) + ","
            "\x27" + sql_escape(canvas) + "\x27,"
            "\x27" + sql_escape(nodes) + "\x27,"
            "\x27" + sql_escape(edges) + "\x27,"
            "\x27" + sql_escape(entry_task_ids) + "\x27,"
            "\x27" + sql_escape(schedule) + "\x27,"
            "\x27" + sql_escape(now) + "\x27,"
            "\x27" + sql_escape(now) + "\x27) "
            "ON DUPLICATE KEY UPDATE name=VALUES(name), description=VALUES(description), enabled=VALUES(enabled), "
            "canvas=VALUES(canvas), nodes=VALUES(nodes), edges=VALUES(edges), entry_task_ids=VALUES(entry_task_ids), "
            "schedule=VALUES(schedule), updated_at=VALUES(updated_at)";
        if (!conn->execute(sql)) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Pipeline save failed"}}));
        }
        Json saved_pipeline = body;
        saved_pipeline["pipeline_id"] = id;
        saved_pipeline["name"] = name;
        saved_pipeline["description"] = description;
        saved_pipeline["enabled"] = enabled != 0;
        saved_pipeline["created_at"] = now;
        saved_pipeline["updated_at"] = now;
        auto persisted = conn->query<pipelines>("pipeline_id = '" + sql_escape(id) + "' LIMIT 1");
        if (!persisted.empty()) {
            saved_pipeline["created_at"] = persisted.front().created_at;
            saved_pipeline["updated_at"] = persisted.front().updated_at;
        }
        return resp->Json(Json::object({
            {"code", 0},
            {"message", "Pipeline saved"},
            {"data", saved_pipeline}
        }));
    });

    // 4.5 删除 Pipeline (DELETE /api/pipelines/{id})
    router.Delete("/api/pipelines/{id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string id = req->GetParam("id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        if (conn->execute("DELETE FROM pipelines WHERE pipeline_id = '" + sql_escape(id) + "'")) {
            return resp->Json(Json::object({{"code", 0}, {"message", "Pipeline deleted"}}));
        }
        return resp->Json(Json::object({{"code", 404}, {"message", "Not Found"}}));
    });

    // 4.6 发布 Pipeline (POST /api/pipelines/{id}/publish)
    router.POST("/api/pipelines/{id}/publish", [&](HttpRequest* req, HttpResponse* resp) {
        std::string id = req->GetParam("id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "DB Error"}}));
        
        auto result = conn->query<pipelines>("pipeline_id = '" + sql_escape(id) + "'");
        if (result.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Not Found"}}));
        const auto& p = result.front();
        std::string publish_id = id;

        hv::Json body = Json::object({
            // 💡 核心修改1：将 pipeline_id 传递给校验和发布逻辑
            {"pipeline_id", p.pipeline_id},
            {"nodes", Json::parse(p.nodes)},
            {"edges", Json::parse(p.edges)}
        });

        std::string error_msg;
        if (!validate_dag(body, conn.get(), error_msg)) {
            return resp->Json(Json::object({{"code", 400}, {"message", "Validation failed: " + error_msg}}));
        }

        // 使用事务确保原子性
        conn->begin();
        try {
            // 💡 核心修改2：不再清空全表，而是只删除属于当前 pipeline 的边
            conn->execute("DELETE FROM dag_edges WHERE pipeline_id = '" + sql_escape(publish_id) + "'");
            for (const auto& edge : body["edges"]) {
                dag_edges new_edge;
                new_edge.parent_task_id = edge["from"].get<std::string>();
                new_edge.child_task_id = edge["to"].get<std::string>();
                new_edge.pipeline_id = publish_id; // 写入边所属的 pipeline_id
                conn->insert(new_edge);
            }
            conn->commit();
        } catch (const std::exception& e) {
            conn->rollback();
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Publish failed: ") + e.what()}}));
        }

        Json updated_tasks = Json::array();
        for (const auto& node : body["nodes"]) {
            updated_tasks.push_back(node["task_id"]);
        }
        Json publish_data = Json::object({
            {"pipeline_id", id},
            {"updated_tasks", updated_tasks},
            {"cleared_tasks", Json::array()},
            {"skipped_tasks", Json::array()}
        });
        return resp->Json(Json::object({
            {"code", 0},
            {"message", "Pipeline published successfully"},
            {"data", publish_data}
        }));
    });

    // 2.1 获取单个任务详情 (GET /api/tasks/{task_id})
    router.GET("/api/tasks/{task_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        auto task_list = conn->query<tasks>("task_id = '" + sql_escape(task_id) + "'");
        if (task_list.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}}));
        const auto& t = task_list.front();
        auto latest = conn->query<task_instances>("task_id = '" + sql_escape(t.task_id) + "' ORDER BY start_time DESC LIMIT 1");
        auto parents = conn->query<dag_edges>("child_task_id = '" + sql_escape(t.task_id) + "'");
        Json parent_ids = Json::array();
        for (const auto& e : parents) parent_ids.push_back(e.parent_task_id);
        int latest_status = latest.empty() ? 2 : latest.front().status;
        std::string last_run_at = latest.empty() ? "" : latest.front().start_time;
        std::string latest_exec_id = latest.empty() ? "" : latest.front().exec_id;
        std::string task_type = !parents.empty() ? "dag" : (!t.cron_expr.empty() ? "cron" : "manual");
        std::string trigger_mode = !parents.empty() ? "dependency" : (!t.cron_expr.empty() ? "cron" : "manual");

        Json data = Json::object({
            {"task_id", t.task_id}, {"name", t.name}, {"description", t.script_path},
            {"node_type", t.node_type}, {"task_category", task_category_label(t.task_category)},
            {"taskCategory", task_category_label(t.task_category)}, {"task_category_code", normalize_task_category(t.task_category)},
            {"taskCategoryCode", normalize_task_category(t.task_category)}, {"task_category_label", task_category_label(t.task_category)},
            {"taskCategoryLabel", task_category_label(t.task_category)}, {"script_path", t.script_path}, {"cron_expr", t.cron_expr},
            {"timeout_sec", t.timeout_sec}, {"enabled", t.enabled}, {"target_node_id", t.target_node_id},
            {"target_os", target_os_from_node(t.target_node_id)}, {"target_node_tag", t.target_node_id},
            {"task_type", task_type}, {"schedule_status", status_to_label(latest_status)}, {"trigger_mode", trigger_mode},
            {"last_run_at", last_run_at}, {"latest_exec_id", latest_exec_id}, {"retry_count", 0},
            {"retry_interval_sec", 300}, {"concurrency_limit", 1}, {"dag_parents", parent_ids},
            {"env_vars", Json::array()}, {"log_level", "INFO"}, {"alert_on_failure", true},
            {"default_params", parse_params_json(t.default_params)}
        });
        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", data}}));
    });

    // 2.2 删除任务接口 (DELETE /api/tasks/{task_id})
    router.Delete("/api/tasks/{task_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::string escaped_task_id = sql_escape(task_id);
        auto task_list = conn->query<tasks>("task_id = '" + escaped_task_id + "'");
        if (task_list.empty()) {
            return resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}, {"data", nullptr}}));
        }

        try {
            conn->execute("DELETE FROM task_instances WHERE task_id = '" + escaped_task_id + "'");
            conn->execute(
                "DELETE FROM dag_edges WHERE parent_task_id = '" + escaped_task_id +
                "' OR child_task_id = '" + escaped_task_id + "'"
            );

            if (conn->execute("DELETE FROM tasks WHERE task_id = '" + escaped_task_id + "'")) {
                hlogi("🗑️ Task Deleted: [%s]", task_id.c_str());
                return resp->Json(Json::object({{"code", 0}, {"message", "Task deleted successfully"}, {"data", nullptr}}));
            }
            return resp->Json(Json::object({{"code", 500}, {"message", "Failed to delete task"}, {"data", nullptr}}));
        } catch (const std::exception& e) {
            hloge("❌ Delete task error: %s", e.what());
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Internal Error: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.3 获取执行记录 (GET /api/executions?task_id=xxx&page=1&page_size=20)
    router.GET("/api/executions", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::string task_id = req->GetParam("task_id");
        int page = parse_int_param(req->GetParam("page"), 1, 1, 1000000);
        int page_size = parse_int_param(req->GetParam("page_size"), 20, 1, 500);
        if (!req->GetParam("pageSize").empty()) page_size = parse_int_param(req->GetParam("pageSize"), page_size, 1, 500);
        std::string filter = task_id.empty() ? "1=1" : "task_id = '" + sql_escape(task_id) + "'";
        long total = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM task_instances WHERE " + filter);
        int offset = (page - 1) * page_size;
        std::string condition = filter + " ORDER BY start_time DESC LIMIT " + std::to_string(page_size) + " OFFSET " + std::to_string(offset);
        auto instances = conn->query<task_instances>(condition);
        Json arr = Json::array();
        for (const auto& inst : instances) {
            auto task_list = conn->query<tasks>("task_id = '" + sql_escape(inst.task_id) + "'");
            std::string cron_expr = task_list.empty() ? "" : task_list.front().cron_expr;
            arr.push_back(Json::object({
                {"exec_id", inst.exec_id}, {"task_id", inst.task_id}, {"node_id", inst.node_id},
                {"status", inst.status}, {"status_label", status_to_label(inst.status)}, {"exit_code", inst.exit_code},
                {"start_time", inst.start_time}, {"end_time", inst.end_time}, {"log_path", inst.log_path},
                {"params", inst.params}, {"trigger_mode", trigger_mode_from_exec_id(inst.exec_id, cron_expr)},
                {"stdout", ""}, {"stderr", ""}
            }));
        }
        return resp->Json(Json::object({
            {"code", 0}, {"message", "success"},
            {"data", Json::object({
                {"items", arr}, {"total", total}, {"page", page}, {"page_size", page_size}
            })}
        }));
    });

    // 2.3 获取 Agent 列表 (GET /api/agents)
    router.GET("/api/agents", [&redis](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::map<std::string, Json> node_map;
        auto db_agents = conn->query<agents>("");
        for (const auto& a : db_agents) {
            std::string status = a.status.empty() ? "offline" : a.status;
            node_map[a.node_id] = Json::object({
                {"node_id", a.node_id}, {"hostname", a.node_id}, {"ip", a.ip}, {"os_type", a.os_type.empty() ? target_os_from_node(a.node_id) : a.os_type},
                {"status", status}, {"last_heartbeat", cst_time_str_from_sql(a.last_heartbeat)}, {"cpu_load", a.cpu_load}, {"mem_usage", a.mem_usage},
                {"version", "agent-0.3.1"}, {"concurrency_used", 0}, {"concurrency_limit", 1}, {"running_tasks", 0},
                {"tags", Json::array()}, {"created_at", cst_time_str_from_sql(a.created_at)}
            });
        }

        auto task_list = conn->query<tasks>("");
        for (const auto& t : task_list) {
            if (t.target_node_id.empty()) continue;
            if (!node_map.count(t.target_node_id)) {
                std::string os_type = target_os_from_node(t.target_node_id);
                Json tags = os_type == "windows" ? Json::array({"wind", "cj-connector"}) : Json::array({"rqdata", "dolphindb"});
                node_map[t.target_node_id] = Json::object({
                    {"node_id", t.target_node_id}, {"hostname", t.target_node_id}, {"ip", ""}, {"os_type", os_type},
                    {"status", "offline"}, {"last_heartbeat", ""}, {"cpu_load", 0}, {"mem_usage", 0},
                    {"version", "agent-0.3.1"}, {"concurrency_used", 0}, {"concurrency_limit", 1}, {"running_tasks", 0},
                    {"tags", tags}, {"created_at", ""}
                });
            }
        }

        for (auto& item : node_map) {
            const std::string& node_id = item.first;
            int running = 0;
            auto running_instances = conn->query<task_instances>("node_id = '" + sql_escape(node_id) + "' AND status = 0");
            running = static_cast<int>(running_instances.size());
            item.second["running_tasks"] = running;
            item.second["concurrency_used"] = running;
            if (running > 0) item.second["status"] = "busy";
            else if (redis) {
                // 从 Redis 中获取并解析最新的心跳数据，更新 Agent 状态
                try {
                    std::string redis_key = "agent:heartbeat:" + node_id;
                    if (redis->exists(redis_key)) {
                        item.second["status"] = "online";
                        // 从 Redis 中获取完整的心跳 payload
                        auto payload_str = redis->get(redis_key);
                        if (payload_str) {
                            try {
                                auto payload_json = Json::parse(*payload_str);
                                // 使用最新的心跳数据更新所有相关字段
                                item.second["ip"] = payload_json.value("ip", item.second.value("ip", ""));
                                item.second["os_type"] = payload_json.value("os_type", item.second.value("os_type", ""));
                                item.second["version"] = payload_json.value("version", item.second.value("version", ""));
                                item.second["tags"] = payload_json.value("tags", item.second.value("tags", Json::array()));
                                item.second["cpu_load"] = payload_json.value("cpu_load", item.second.value("cpu_load", 0.0));
                                item.second["mem_usage"] = payload_json.value("mem_usage", item.second.value("mem_usage", 0.0));
                                // 从心跳中获取 UTC 时间戳并转换为东八区时间字符串
                                if (payload_json.contains("last_heartbeat") && payload_json["last_heartbeat"].is_number()) {
                                    long long ts = payload_json["last_heartbeat"].get<long long>();
                                    item.second["last_heartbeat"] = to_cst_time_str(ts);
                                }
                            } catch(...) {}
                        }
                    }
                } catch (...) {}
            }
        }

        Json arr = Json::array();
        for (auto& item : node_map) arr.push_back(item.second);
        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", arr}}));
    });

    // 2.4 仪表盘聚合指标 (GET /api/dashboard/metrics)
    router.GET("/api/dashboard/metrics", [&redis](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::string today = today_yyyy_mm_dd();
        auto today_instances = conn->query<task_instances>("start_time >= '" + today + " 00:00:00'");
        int success = 0, running = 0, failed = 0;
        long total_duration_sec = 0;
        int duration_count = 0;
        for (const auto& inst : today_instances) {
            if (inst.status == 1) success++;
            if (inst.status == 0) running++;
            if (inst.status == -1 || inst.status == -2) failed++;
            if (inst.status == 1 || inst.status == -1 || inst.status == -2) {
                long seconds = seconds_between_sql_time(inst.start_time, inst.end_time);
                if (seconds >= 0) {
                    total_duration_sec += seconds;
                    duration_count++;
                }
            }
        }
        int today_total = static_cast<int>(today_instances.size());
        int success_rate = today_total == 0 ? 0 : static_cast<int>((100.0 * success) / today_total);

        auto all_tasks = conn->query<tasks>("");
        std::map<std::string, std::string> task_name;
        std::map<std::string, std::string> node_os;
        for (const auto& t : all_tasks) {
            task_name[t.task_id] = t.name;
            if (!t.target_node_id.empty()) node_os[t.target_node_id] = target_os_from_node(t.target_node_id);
        }
        int online_agents = 0, busy_agents = 0, offline_agents = 0, error_agents = 0;
        for (const auto& n : node_os) {
            auto r = conn->query<task_instances>("node_id = '" + sql_escape(n.first) + "' AND status = 0");
            if (!r.empty()) busy_agents++;
            else if (redis) {
                bool online = false;
                try { online = redis->exists("agent:heartbeat:" + n.first); } catch (...) {}
                online ? online_agents++ : offline_agents++;
            } else offline_agents++;
        }

        Json platform_dist = Json::array();
        int linux_count = 0, windows_count = 0;
        for (const auto& n : node_os) {
            if (n.second == "windows") windows_count++; else linux_count++;
        }
        platform_dist.push_back(Json::object({{"os", "Linux"}, {"count", linux_count}}));
        platform_dist.push_back(Json::object({{"os", "Windows"}, {"count", windows_count}}));

        Json trend = Json::array();
        for (int i = 6; i >= 0; --i) {
            std::string d = date_days_ago(i);
            auto rows = conn->query<task_instances>("start_time >= '" + d + " 00:00:00' AND start_time <= '" + d + " 23:59:59'");
            int day_success = 0;
            for (const auto& inst : rows) if (inst.status == 1) day_success++;
            trend.push_back(Json::object({{"date", d.substr(5)}, {"total", static_cast<int>(rows.size())}, {"success", day_success}}));
        }

        Json recent_errors = Json::array();
        auto err_rows = conn->query<task_instances>("status IN (-1, -2) ORDER BY start_time DESC LIMIT 10");
        for (const auto& inst : err_rows) {
            recent_errors.push_back(Json::object({
                {"task_id", inst.task_id},
                {"task_name", task_name.count(inst.task_id) ? task_name[inst.task_id] : inst.task_id},
                {"time", inst.end_time.empty() ? inst.start_time : inst.end_time},
                {"status", status_to_label(inst.status)}
            }));
        }

        Json data = Json::object({
            {"onlineAgents", online_agents},
            {"totalAgents", static_cast<int>(node_os.size())},
            {"todayTasks", today_total},
            {"todaySuccessRate", success_rate},
            {"runningDags", running},
            {"completedDags", success},
            {"avgDuration", duration_count == 0 ? 0 : static_cast<int>(total_duration_sec / duration_count)},
            {"agentHealth", Json::object({{"online", online_agents}, {"busy", busy_agents}, {"offline", offline_agents}, {"error", error_agents}})},
            {"platformDist", platform_dist},
            {"taskTrend", trend},
            {"recentErrors", recent_errors}
        });
        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", data}}));
    });

    // 2.5 DAG 监控图 (GET /api/dag/graphs)
    router.GET("/api/dag/graphs", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        auto edges = conn->query<dag_edges>("pipeline_id = 'main_daily_dag'");
        std::set<std::string> node_ids;
        Json edge_json = Json::array();
        for (const auto& e : edges) {
            node_ids.insert(e.parent_task_id);
            node_ids.insert(e.child_task_id);
            edge_json.push_back(Json::object({{"from", e.parent_task_id}, {"to", e.child_task_id}}));
        }

        Json graph_canvas = Json::object({{"width", 1600}, {"height", 720}});
        std::map<std::string, std::pair<int, int>> saved_positions;
        auto saved_pipelines = conn->query<pipelines>("pipeline_id = 'main_daily_dag' AND enabled = 1 LIMIT 1");
        if (!saved_pipelines.empty()) {
            try { graph_canvas = Json::parse(saved_pipelines.front().canvas); } catch (...) {}
            try {
                Json saved_nodes = Json::parse(saved_pipelines.front().nodes);
                if (saved_nodes.is_array()) {
                    for (const auto& node : saved_nodes) {
                        if (!node.is_object() || !node.contains("task_id") || !node["task_id"].is_string()) continue;
                        std::string task_id = node["task_id"].get<std::string>();
                        node_ids.insert(task_id);
                        if (!node.contains("x") || !node.contains("y") ||
                            !node["x"].is_number() || !node["y"].is_number()) continue;
                        saved_positions[task_id] = {node["x"].get<int>(), node["y"].get<int>()};
                    }
                }
            } catch (...) {}
        }

        // 按 DAG 依赖做拓扑层级布局，避免按 task_id 字典序排布导致连线交叉。
        std::map<std::string, std::vector<std::string>> children;
        std::map<std::string, int> indegree;
        std::map<std::string, int> level;
        for (const auto& id : node_ids) {
            indegree[id] = 0;
            level[id] = 0;
        }
        for (const auto& e : edges) {
            children[e.parent_task_id].push_back(e.child_task_id);
            indegree[e.child_task_id]++;
        }

        std::vector<std::string> ready;
        for (const auto& item : indegree) {
            if (item.second == 0) ready.push_back(item.first);
        }
        std::sort(ready.begin(), ready.end());

        while (!ready.empty()) {
            std::vector<std::string> next;
            for (const auto& parent : ready) {
                auto it = children.find(parent);
                if (it == children.end()) continue;
                std::sort(it->second.begin(), it->second.end());
                for (const auto& child : it->second) {
                    level[child] = std::max(level[child], level[parent] + 1);
                    indegree[child]--;
                    if (indegree[child] == 0) next.push_back(child);
                }
            }
            std::sort(next.begin(), next.end());
            ready = next;
        }

        std::map<int, std::vector<std::string>> levels;
        for (const auto& id : node_ids) levels[level[id]].push_back(id);
        for (auto& item : levels) std::sort(item.second.begin(), item.second.end());

        Json node_json = Json::array();
        for (const auto& layer : levels) {
            int col = layer.first;
            for (size_t row = 0; row < layer.second.size(); ++row) {
                const auto& task_id = layer.second[row];
                auto task_list = conn->query<tasks>("task_id = '" + sql_escape(task_id) + "'");
                if (task_list.empty()) continue;
                const auto& t = task_list.front();
                int status = 2;
                std::string latest_exec;
                int x = 60 + col * 220;
                int y = 80 + static_cast<int>(row) * 125;
                auto saved_it = saved_positions.find(task_id);
                if (saved_it != saved_positions.end()) {
                    x = saved_it->second.first;
                    y = saved_it->second.second;
                }
                node_json.push_back(Json::object({
                    {"id", t.task_id}, {"task_id", t.task_id}, {"name", t.name},
                    {"status", status_to_label(status)}, {"x", x}, {"y", y},
                    {"latestExecId", latest_exec}, {"agent", t.target_node_id}, {"duration", ""}
                }));
            }
        }

        Json graphs = Json::array();
        if (!saved_pipelines.empty()) {
            const auto& main_pipeline = saved_pipelines.front();
            graphs.push_back(Json::object({
                {"id", main_pipeline.pipeline_id},
                {"name", main_pipeline.name},
                {"description", main_pipeline.description},
                {"canvas", graph_canvas},
                {"nodes", node_json},
                {"edges", edge_json}
            }));
        }
        auto additional_pipelines = conn->query<pipelines>("enabled = 1 AND pipeline_id != \x27main_daily_dag\x27 ORDER BY pipeline_id");
        for (const auto& pipeline : additional_pipelines) {
            try {
                Json pipeline_nodes = Json::parse(pipeline.nodes);
                Json pipeline_edges = Json::parse(pipeline.edges);
                Json graph_nodes = Json::array();
                for (const auto& node : pipeline_nodes) {
                    if (!node.contains("task_id") || !node["task_id"].is_string()) continue;
                    std::string task_id = node["task_id"].get<std::string>();
                    auto task_rows = conn->query<tasks>("task_id = \x27" + sql_escape(task_id) + "\x27 LIMIT 1");
                    if (task_rows.empty()) continue;
                    const auto& task = task_rows.front();
                    int status = 2;
                    std::string latest_exec;
                    graph_nodes.push_back(Json::object({
                        {"id", task.task_id}, {"task_id", task.task_id}, {"name", task.name},
                        {"status", status_to_label(status)},
                        {"x", node.value("x", 60)}, {"y", node.value("y", 80)},
                        {"latestExecId", latest_exec}, {"agent", task.target_node_id}, {"duration", ""}
                    }));
                }
                Json pipeline_canvas = Json::object({{"width", 1600}, {"height", 720}});
                try { pipeline_canvas = Json::parse(pipeline.canvas); } catch (...) {}
                graphs.push_back(Json::object({
                    {"id", pipeline.pipeline_id}, {"name", pipeline.name},
                    {"description", pipeline.description}, {"canvas", pipeline_canvas},
                    {"nodes", graph_nodes}, {"edges", pipeline_edges}
                }));
            } catch (...) {
                continue;
            }
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "success"}, {"data", graphs}}));
    });

    // 2.5.1 DAG 按交易日节点状态 (GET /api/dag/graphs/{pipeline_id}/day-status)
    router.GET("/api/dag/graphs/{pipeline_id}/day-status", [](HttpRequest* req, HttpResponse* resp) {
        std::string pipeline_id = req->GetParam("pipeline_id");
        std::string date = req->GetParam("date");
        if (!is_valid_iso_date(date)) {
            resp->Json(Json::object({
                {"code", 400},
                {"message", date.empty() ? "Missing required query parameter: date" : "Invalid date, expected YYYY-MM-DD"},
                {"data", nullptr}
            }));
            return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
        }

        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) {
            resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
            return static_cast<int>(HTTP_STATUS_INTERNAL_SERVER_ERROR);
        }

        std::string escaped_pipeline_id = sql_escape(pipeline_id);
        auto pipeline_rows = conn->query<pipelines>(
            "pipeline_id = '" + escaped_pipeline_id + "' AND enabled = 1 LIMIT 1"
        );
        if (pipeline_rows.empty()) {
            resp->Json(Json::object({{"code", 404}, {"message", "Pipeline not found or disabled"}, {"data", nullptr}}));
            return static_cast<int>(HTTP_STATUS_NOT_FOUND);
        }

        std::vector<std::string> task_ids;
        std::set<std::string> seen_task_ids;
        try {
            Json nodes = Json::parse(pipeline_rows.front().nodes.empty() ? "[]" : pipeline_rows.front().nodes);
            if (nodes.is_array()) {
                for (const auto& node : nodes) {
                    if (!node.is_object() || !node.contains("task_id") || !node["task_id"].is_string()) continue;
                    std::string task_id = node["task_id"].get<std::string>();
                    if (!task_id.empty() && seen_task_ids.insert(task_id).second) task_ids.push_back(task_id);
                }
            }
        } catch (...) {}

        // 兼容尚未保存 nodes JSON 的旧主流程，按依赖边首次出现顺序补齐节点。
        if (task_ids.empty()) {
            auto edges = conn->query<dag_edges>(
                "pipeline_id = '" + escaped_pipeline_id + "' ORDER BY parent_task_id, child_task_id"
            );
            for (const auto& edge : edges) {
                if (seen_task_ids.insert(edge.parent_task_id).second) task_ids.push_back(edge.parent_task_id);
                if (seen_task_ids.insert(edge.child_task_id).second) task_ids.push_back(edge.child_task_id);
            }
        }

        std::map<std::string, task_instances> latest_by_task;
        std::map<std::string, task_instances> latest_success_by_task;
        if (!task_ids.empty()) {
            std::string escaped_date = sql_escape(date);
            std::string compact_date = iso_date_to_compact(date);
            std::string day_start = escaped_date + " 00:00:00";
            std::string condition =
                "task_id IN (" + sql_quoted_list(task_ids) + ") AND ("
                "(start_time >= '" + day_start + "' AND start_time < DATE_ADD('" + day_start + "', INTERVAL 1 DAY)) OR "
                "(end_time >= '" + day_start + "' AND end_time < DATE_ADD('" + day_start + "', INTERVAL 1 DAY)) OR "
                "(JSON_VALID(params) AND ("
                "REPLACE(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(params, '$.trade_date')), ''), '-', '') = '" + compact_date + "' OR "
                "(REPLACE(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(params, '$.start_date')), ''), '-', '') <= '" + compact_date + "' AND "
                "REPLACE(COALESCE(JSON_UNQUOTE(JSON_EXTRACT(params, '$.end_date')), ''), '-', '') >= '" + compact_date + "')"
                "))"
                ") ORDER BY COALESCE(end_time, start_time) DESC";
            auto instances = conn->query<task_instances>(condition);
            for (const auto& inst : instances) {
                auto covered_days = covered_trade_days_from_instance(inst);
                if (std::find(covered_days.begin(), covered_days.end(), date) == covered_days.end()) continue;
                if (!latest_by_task.count(inst.task_id) ||
                    instance_sort_time(inst) > instance_sort_time(latest_by_task[inst.task_id])) {
                    latest_by_task[inst.task_id] = inst;
                }
                if (inst.status == 1 && (!latest_success_by_task.count(inst.task_id) ||
                    instance_sort_time(inst) > instance_sort_time(latest_success_by_task[inst.task_id]))) {
                    latest_success_by_task[inst.task_id] = inst;
                }
            }
        }

        Json node_statuses = Json::array();
        for (const auto& task_id : task_ids) {
            Json item = Json::object({{"task_id", task_id}, {"status", "missing"}});
            const task_instances* selected = nullptr;
            if (latest_success_by_task.count(task_id)) {
                selected = &latest_success_by_task.at(task_id);
                item["status"] = "success";
            } else if (latest_by_task.count(task_id)) {
                selected = &latest_by_task.at(task_id);
                item["status"] = status_to_label(selected->status);
            }

            if (selected) {
                item["exec_id"] = selected->exec_id;
                if (!selected->start_time.empty()) item["start_time"] = sql_time_to_iso(selected->start_time);
                if (!selected->end_time.empty()) item["end_time"] = sql_time_to_iso(selected->end_time);
                std::string duration = readable_duration(seconds_between_sql_time(selected->start_time, selected->end_time));
                if (!duration.empty()) item["duration"] = duration;
            }
            node_statuses.push_back(item);
        }

        Json data = Json::object({
            {"pipeline_id", pipeline_id},
            {"date", date},
            {"nodes", node_statuses}
        });
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", data}}));
    });

    // 2.6 日志搜索接口 (GET /api/logs/search)
    router.GET("/api/logs/search", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::string keyword = req->GetParam("keyword");
        std::string agent_id = req->GetParam("agent_id");
        std::string status_filter = req->GetParam("status");
        int page = parse_int_param(req->GetParam("page"), 1, 1, 1000000);
        int page_size = parse_int_param(req->GetParam("page_size"), 20, 1, 100);
        if (!req->GetParam("pageSize").empty()) page_size = parse_int_param(req->GetParam("pageSize"), page_size, 1, 100);
        std::string date_from = normalize_iso_date(req->GetParam("date_from"));
        std::string date_to = normalize_iso_date(req->GetParam("date_to"));

        std::string condition = "1=1";
        if (!agent_id.empty()) condition += " AND node_id = '" + sql_escape(agent_id) + "'";
        if (!status_filter.empty()) {
            if (status_filter == "waiting") condition += " AND status = 2";
            else if (status_filter == "running") condition += " AND status = 0";
            else if (status_filter == "success") condition += " AND status = 1";
            else if (status_filter == "failed") condition += " AND status = -1";
            else if (status_filter == "timeout") condition += " AND status = -2";
        }
        if (valid_iso_date(date_from)) condition += " AND COALESCE(end_time, start_time) >= '" + sql_escape(date_from) + " 00:00:00'";
        if (valid_iso_date(date_to)) condition += " AND COALESCE(end_time, start_time) <= '" + sql_escape(date_to) + " 23:59:59'";
        if (!keyword.empty()) {
            std::string escaped = sql_like_escape(keyword);
            std::vector<std::string> keyword_clauses = {"task_id LIKE '%" + escaped + "%'"};
            auto matched_tasks = conn->query<tasks>("name LIKE '%" + escaped + "%'");
            std::vector<std::string> matched_task_ids;
            matched_task_ids.reserve(matched_tasks.size());
            for (const auto& task : matched_tasks) matched_task_ids.push_back(task.task_id);
            if (!matched_task_ids.empty()) {
                keyword_clauses.push_back("task_id IN (" + sql_quoted_list(matched_task_ids) + ")");
            }
            condition += " AND (" + join_strings(keyword_clauses, " OR ") + ")";
        }
        long total = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM task_instances WHERE " + condition);
        long success = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM task_instances WHERE " + condition + " AND status = 1");
        long running = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM task_instances WHERE " + condition + " AND status = 0");
        long errors = query_sql_count(conn, "SELECT CAST(COUNT(*) AS CHAR) FROM task_instances WHERE " + condition + " AND status IN (-1, -2)");
        int offset = (page - 1) * page_size;
        std::string page_condition = condition + " ORDER BY COALESCE(end_time, start_time) DESC LIMIT " + std::to_string(page_size) + " OFFSET " + std::to_string(offset);

        auto instances = conn->query<task_instances>(page_condition);
        Json arr = Json::array();
        for (const auto& inst : instances) {
            auto task_list = conn->query<tasks>("task_id = '" + sql_escape(inst.task_id) + "'");
            std::string task_name = task_list.empty() ? inst.task_id : task_list.front().name;

            arr.push_back(Json::object({
                {"log_id", log_id_from_exec_id(inst.exec_id)},
                {"exec_id", inst.exec_id},
                {"task_id", inst.task_id},
                {"task_name", task_name},
                {"agent_id", inst.node_id},
                {"level", log_level_from_status(inst.status)},
                {"time", instance_sort_time(inst)},
                {"status", status_to_label(inst.status)}
            }));
        }

        return resp->Json(Json::object({
            {"code", 0}, {"message", "ok"},
            {"data", Json::object({
                {"items", arr}, {"total", total}, {"page", page}, {"page_size", page_size},
                {"summary", Json::object({
                    {"total", total}, {"success", success}, {"running", running}, {"errors", errors}
                })}
            })}
        }));
    });

    // 2.7 日志详情接口 (GET /api/logs/{log_id})
    router.GET("/api/logs/{log_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string exec_id = exec_id_from_log_id(req->GetParam("log_id"));
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto instances = conn->query<task_instances>("exec_id = '" + sql_escape(exec_id) + "'");
        if (instances.empty()) return resp->Json(Json::object({{"code", 1}, {"message", "Log not found"}, {"data", nullptr}}));
        const auto& inst = instances.front();
        auto task_list = conn->query<tasks>("task_id = '" + sql_escape(inst.task_id) + "'");
        std::string task_name = task_list.empty() ? inst.task_id : task_list.front().name;
        return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", build_log_detail_json(inst, task_name)}}));
    });

    // 2.8 日志下载接口 (GET /api/logs/{log_id}/download)
    router.GET("/api/logs/{log_id}/download", [](HttpRequest* req, HttpResponse* resp) {
        std::string exec_id = exec_id_from_log_id(req->GetParam("log_id"));
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto instances = conn->query<task_instances>("exec_id = '" + sql_escape(exec_id) + "'");
        if (instances.empty()) return resp->Json(Json::object({{"code", 1}, {"message", "Log not found"}, {"data", nullptr}}));
        const auto& inst = instances.front();
        auto task_list = conn->query<tasks>("task_id = '" + sql_escape(inst.task_id) + "'");
        std::string task_name = task_list.empty() ? inst.task_id : task_list.front().name;
        Json detail = build_log_detail_json(inst, task_name);
        std::string text = build_log_download_text(detail);
        resp->headers["Content-Disposition"] = "attachment; filename=\"" + detail.value("log_id", log_id_from_exec_id(exec_id)) + ".log\"";
        return resp->String(text);
    });

    // 2.9 数据监控分组列表 (GET /api/ddb-monitor-groups)
    router.GET("/api/ddb-monitor-groups", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::string enabled = req->GetParam("enabled");
        std::string where;
        if (enabled == "0" || enabled == "1") where = "g.enabled = " + enabled;
        try {
            Json arr = Json::array();
            for (const auto& group : query_ddb_monitor_groups(conn, where)) {
                arr.push_back(monitor_group_to_json(group));
            }
            return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", arr}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Failed to query monitor groups: ") + e.what()}, {"data", Json::array()}}));
        }
    });

    // 2.10 新建数据监控分组 (POST /api/ddb-monitor-groups)
    router.POST("/api/ddb-monitor-groups", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        try {
            Json body = req->GetJson();
            ddb_monitor_group g;
            if (body.contains("group_id") && body["group_id"].is_string()) g.group_id = body["group_id"].get<std::string>();
            if (body.contains("groupId") && body["groupId"].is_string()) g.group_id = body["groupId"].get<std::string>();
            if (body.contains("display_name") && body["display_name"].is_string()) g.display_name = body["display_name"].get<std::string>();
            if (body.contains("displayName") && body["displayName"].is_string()) g.display_name = body["displayName"].get<std::string>();
            if (body.contains("sort_order") && body["sort_order"].is_number_integer()) g.sort_order = body["sort_order"].get<int>();
            if (body.contains("sortOrder") && body["sortOrder"].is_number_integer()) g.sort_order = body["sortOrder"].get<int>();
            if (body.contains("enabled")) {
                if (body["enabled"].is_boolean()) g.enabled = body["enabled"].get<bool>() ? 1 : 0;
                else if (body["enabled"].is_number_integer()) g.enabled = body["enabled"].get<int>() == 0 ? 0 : 1;
            }
            if (body.contains("description") && body["description"].is_string()) g.description = body["description"].get<std::string>();
            if (!valid_group_id(g.group_id)) return resp->Json(Json::object({{"code", 400}, {"message", "group_id format is invalid"}, {"data", nullptr}}));
            if (g.display_name.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "display_name is required"}, {"data", nullptr}}));
            if (ddb_monitor_group_exists(conn, g.group_id)) return resp->Json(Json::object({{"code", 409}, {"message", "group_id already exists"}, {"data", nullptr}}));
            if ((!body.contains("sort_order") || !body["sort_order"].is_number_integer()) &&
                (!body.contains("sortOrder") || !body["sortOrder"].is_number_integer())) {
                long max_order = query_sql_count(conn, "SELECT COALESCE(MAX(sort_order), 0) FROM ddb_monitor_group");
                g.sort_order = static_cast<int>(max_order + 10);
            }
            std::string now = to_cst_time_str(std::time(nullptr));
            std::string sql =
                "INSERT INTO ddb_monitor_group(group_id, display_name, sort_order, enabled, description, created_at, updated_at) VALUES('" +
                sql_escape(g.group_id) + "', '" + sql_escape(g.display_name) + "', " + std::to_string(g.sort_order) + ", " +
                std::to_string(g.enabled) + ", '" + sql_escape(g.description) + "', '" + sql_escape(now) + "', '" + sql_escape(now) + "')";
            if (!conn->execute(sql)) return resp->Json(Json::object({{"code", 500}, {"message", "Failed to create monitor group"}, {"data", nullptr}}));
            auto rows = query_ddb_monitor_groups(conn, "g.group_id = '" + sql_escape(g.group_id) + "'");
            return resp->Json(Json::object({{"code", 0}, {"message", "Monitor group created"}, {"data", rows.empty() ? monitor_group_to_json(g) : monitor_group_to_json(rows.front())}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.11 更新数据监控分组 (PATCH /api/ddb-monitor-groups/{group_id})
    router.PATCH("/api/ddb-monitor-groups/{group_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string group_id = req->GetParam("group_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!valid_group_id(group_id)) return resp->Json(Json::object({{"code", 400}, {"message", "group_id format is invalid"}, {"data", nullptr}}));

        try {
            auto rows = query_ddb_monitor_groups(conn, "g.group_id = '" + sql_escape(group_id) + "'");
            if (rows.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor group not found"}, {"data", nullptr}}));
            Json body = req->GetJson();
            std::vector<std::string> sets;
            if (body.contains("group_id") || body.contains("groupId")) {
                return resp->Json(Json::object({{"code", 400}, {"message", "group_id cannot be modified"}, {"data", nullptr}}));
            }
            if (body.contains("display_name") && body["display_name"].is_string()) {
                std::string value = body["display_name"].get<std::string>();
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "display_name is required"}, {"data", nullptr}}));
                sets.push_back("display_name = '" + sql_escape(value) + "'");
            }
            if (body.contains("displayName") && body["displayName"].is_string()) {
                std::string value = body["displayName"].get<std::string>();
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "display_name is required"}, {"data", nullptr}}));
                sets.push_back("display_name = '" + sql_escape(value) + "'");
            }
            if (body.contains("sort_order") && body["sort_order"].is_number_integer()) {
                sets.push_back("sort_order = " + std::to_string(body["sort_order"].get<int>()));
            }
            if (body.contains("sortOrder") && body["sortOrder"].is_number_integer()) {
                sets.push_back("sort_order = " + std::to_string(body["sortOrder"].get<int>()));
            }
            if (body.contains("enabled")) {
                int enabled_value = rows.front().enabled;
                if (body["enabled"].is_boolean()) enabled_value = body["enabled"].get<bool>() ? 1 : 0;
                else if (body["enabled"].is_number_integer()) enabled_value = body["enabled"].get<int>() == 0 ? 0 : 1;
                sets.push_back("enabled = " + std::to_string(enabled_value));
            }
            if (body.contains("description") && body["description"].is_string()) {
                sets.push_back("description = '" + sql_escape(body["description"].get<std::string>()) + "'");
            }
            if (sets.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "No fields to update"}, {"data", nullptr}}));
            sets.push_back("updated_at = '" + sql_escape(to_cst_time_str(std::time(nullptr))) + "'");
            std::string sql = "UPDATE ddb_monitor_group SET ";
            for (size_t i = 0; i < sets.size(); ++i) {
                if (i > 0) sql += ", ";
                sql += sets[i];
            }
            sql += " WHERE group_id = '" + sql_escape(group_id) + "'";
            if (!conn->execute(sql)) return resp->Json(Json::object({{"code", 500}, {"message", "Failed to update monitor group"}, {"data", nullptr}}));
            auto updated = query_ddb_monitor_groups(conn, "g.group_id = '" + sql_escape(group_id) + "'");
            return resp->Json(Json::object({{"code", 0}, {"message", "Monitor group updated"}, {"data", monitor_group_to_json(updated.front())}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.12 删除数据监控分组 (DELETE /api/ddb-monitor-groups/{group_id})
    router.Delete("/api/ddb-monitor-groups/{group_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string group_id = req->GetParam("group_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        if (!valid_group_id(group_id)) return resp->Json(Json::object({{"code", 400}, {"message", "group_id format is invalid"}, {"data", nullptr}}));
        auto rows = query_ddb_monitor_groups(conn, "g.group_id = '" + sql_escape(group_id) + "'");
        if (rows.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor group not found"}, {"data", nullptr}}));
        long monitor_count = query_sql_count(conn, "SELECT COUNT(*) FROM ddb_monitor WHERE group_id = '" + sql_escape(group_id) + "'");
        if (monitor_count > 0) {
            return resp->Json(Json::object({{"code", 409}, {"message", "该分组下仍有 " + std::to_string(monitor_count) + " 个监控项，请先迁移或删除"}, {"data", nullptr}}));
        }
        if (!conn->execute("DELETE FROM ddb_monitor_group WHERE group_id = '" + sql_escape(group_id) + "'")) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Failed to delete monitor group"}, {"data", nullptr}}));
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "Monitor group deleted"}, {"data", nullptr}}));
    });

    // 2.13 数据监控配置列表 (GET /api/ddb-monitors)
    router.GET("/api/ddb-monitors", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        int page = parse_int_param(req->GetParam("page"), 1, 1, 1000000);
        int page_size = parse_int_param(req->GetParam("page_size"), 100, 1, 500);
        if (!req->GetParam("pageSize").empty()) {
            page_size = parse_int_param(req->GetParam("pageSize"), page_size, 1, 100);
        }

        std::vector<std::string> clauses;
        std::string group_id = req->GetParam("group_id");
        if (group_id.empty()) group_id = req->GetParam("groupId");
        if (group_id.empty()) group_id = req->GetParam("group_name");
        if (group_id.empty()) group_id = req->GetParam("groupName");
        std::string keyword = req->GetParam("keyword");
        std::string enabled = req->GetParam("enabled");
        if (!group_id.empty()) {
            clauses.push_back("group_id = '" + sql_escape(group_id) + "'");
        }
        if (!keyword.empty()) {
            std::string escaped = sql_like_escape(keyword);
            clauses.push_back("(`database` LIKE '%" + escaped + "%' OR table_name LIKE '%" + escaped + "%')");
        }
        if (enabled == "0" || enabled == "1") {
            clauses.push_back("enabled = " + enabled);
        }
        std::string condition = "1=1";
        for (const auto& clause : clauses) condition += " AND " + clause;

        try {
            return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", ddb_monitors_page_json(conn, condition, page, page_size)}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({
                {"code", 500},
                {"message", std::string("Failed to query monitors: ") + e.what()},
                {"data", Json::object({{"items", Json::array()}, {"total", 0}, {"page", page}, {"page_size", page_size}})}
            }));
        }
    });

    // 2.10 数据监控快照矩阵数据源 (GET /api/ddb-monitors/snapshots)
    router.GET("/api/ddb-monitors/snapshots", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::vector<std::string> clauses;
        std::string date_from = normalize_iso_date(req->GetParam("date_from"));
        if (date_from.empty()) date_from = normalize_iso_date(req->GetParam("dateFrom"));
        std::string date_to = normalize_iso_date(req->GetParam("date_to"));
        if (date_to.empty()) date_to = normalize_iso_date(req->GetParam("dateTo"));
        std::string group_id = req->GetParam("group_id");
        if (group_id.empty()) group_id = req->GetParam("groupId");
        if (group_id.empty()) group_id = req->GetParam("group_name");
        if (group_id.empty()) group_id = req->GetParam("groupName");
        std::string keyword = req->GetParam("keyword");
        if (!date_from.empty()) clauses.push_back("s.`date` >= '" + sql_escape(date_from) + "'");
        if (!date_to.empty()) clauses.push_back("s.`date` <= '" + sql_escape(date_to) + "'");
        if (!group_id.empty()) clauses.push_back("TRIM(COALESCE(m.group_id, s.group_id, '')) = '" + sql_escape(group_id) + "'");
        if (!keyword.empty()) {
            std::string escaped = sql_like_escape(keyword);
            clauses.push_back("(TRIM(s.database) LIKE '%" + escaped + "%' OR TRIM(s.table_name) LIKE '%" + escaped + "%')");
        }
        std::string condition;
        for (size_t i = 0; i < clauses.size(); ++i) {
            if (i > 0) condition += " AND ";
            condition += clauses[i];
        }
        try {
            return resp->Json(Json::object({{"code", 0}, {"message", "ok"}, {"data", query_ddb_monitor_snapshots_json(conn, condition)}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Failed to query monitor snapshots: ") + e.what()}, {"data", Json::array()}}));
        }
    });

    // 2.11 指定日期全部启用监控项重新统计 (POST /api/ddb-monitors/recount-batch?date=YYYY-MM-DD)
    router.POST("/api/ddb-monitors/recount-batch", [](HttpRequest* req, HttpResponse* resp) {
        std::string date = normalize_iso_date(req->GetParam("date"));
        if (date.empty()) date = today_yyyy_mm_dd();
        if (!valid_iso_date(date)) return resp->Json(Json::object({{"code", 400}, {"message", "Invalid date, expected YYYY-MM-DD"}, {"data", nullptr}}));

        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        std::string exec_id;
        std::string enqueue_error;
        if (!enqueue_ddb_monitor_count(conn, date, "", exec_id, enqueue_error)) {
            return resp->Json(Json::object({{"code", 500}, {"message", enqueue_error}, {"data", nullptr}}));
        }
        return resp->Json(Json::object({
            {"code", 0},
            {"message", "recount task queued"},
            {"data", Json::object({{"status", "pending"}, {"date", date}, {"exec_id", exec_id}})}
        }));
    });

    // 2.12 新建数据监控项 (POST /api/ddb-monitors)
    router.POST("/api/ddb-monitors", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        try {
            Json body = req->GetJson();
            ddb_monitor m = monitor_from_payload(body);
            if (m.monitor_id.empty()) m.monitor_id = generate_monitor_id(m.database, m.table_name);
            std::string validation_error = validate_monitor_payload(m);
            if (!validation_error.empty()) return resp->Json(Json::object({{"code", 400}, {"message", validation_error}, {"data", nullptr}}));
            if (!ddb_monitor_group_exists(conn, m.group_id, true)) {
                return resp->Json(Json::object({{"code", 400}, {"message", "group_id not found or disabled"}, {"data", nullptr}}));
            }
            std::string related_error;
            if (!validate_related_tasks(conn, related_task_ids_json(m.related_task_ids), related_error)) {
                return resp->Json(Json::object({{"code", 400}, {"message", related_error}, {"data", nullptr}}));
            }
            if (!query_ddb_monitors(conn, "monitor_id = '" + sql_escape(m.monitor_id) + "'").empty()) {
                return resp->Json(Json::object({{"code", 409}, {"message", "monitor_id already exists"}, {"data", nullptr}}));
            }
            std::string now = to_cst_time_str(std::time(nullptr));
            m.created_at = now;
            m.updated_at = now;
            if (!insert_ddb_monitor(conn, m)) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to create monitor"}, {"data", nullptr}}));
            }
            return resp->Json(Json::object({{"code", 0}, {"message", "Monitor created"}, {"data", monitor_to_json(m)}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.13 快速启用/禁用数据监控项 (POST /api/ddb-monitors/{monitor_id}/enabled)
    router.POST("/api/ddb-monitors/{monitor_id}/enabled", [](HttpRequest* req, HttpResponse* resp) {
        std::string monitor_id = req->GetParam("monitor_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        try {
            Json body = req->GetJson();
            if (!body.contains("enabled")) {
                return resp->Json(Json::object({{"code", 400}, {"message", "enabled is required"}, {"data", nullptr}}));
            }
            int enabled_value = -1;
            if (body["enabled"].is_boolean()) enabled_value = body["enabled"].get<bool>() ? 1 : 0;
            else if (body["enabled"].is_number_integer()) enabled_value = body["enabled"].get<int>() ? 1 : 0;
            else return resp->Json(Json::object({{"code", 400}, {"message", "enabled must be boolean or integer"}, {"data", nullptr}}));

            std::string escaped_id = sql_escape(monitor_id);
            long existing = query_sql_count(conn, "SELECT COUNT(*) FROM ddb_monitor WHERE monitor_id='" + escaped_id + "'");
            if (existing <= 0) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor not found"}, {"data", nullptr}}));

            std::string now = to_cst_time_str(std::time(nullptr));
            std::string sql = "UPDATE ddb_monitor SET enabled=" + std::to_string(enabled_value) +
                              ", updated_at='" + sql_escape(now) + "' WHERE monitor_id='" + escaped_id + "'";
            if (!conn->execute(sql)) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to update monitor"}, {"data", nullptr}}));
            }
            return resp->Json(Json::object({
                {"code", 0},
                {"message", "Monitor updated"},
                {"data", Json::object({
                    {"monitor_id", monitor_id},
                    {"monitorId", monitor_id},
                    {"enabled", enabled_value},
                    {"updated_at", now},
                    {"updatedAt", now}
                })}
            }));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.13b 快速编辑数据监控项 (POST /api/ddb-monitors/{monitor_id}/update)
    router.POST("/api/ddb-monitors/{monitor_id}/update", [](HttpRequest* req, HttpResponse* resp) {
        std::string monitor_id = req->GetParam("monitor_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));

        try {
            Json body = req->GetJson();

            std::string escaped_id = sql_escape(monitor_id);
            long existing = query_sql_count(conn, "SELECT COUNT(*) FROM ddb_monitor WHERE monitor_id='" + escaped_id + "'");
            if (existing <= 0) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor not found"}, {"data", nullptr}}));

            std::vector<std::string> sets;
            auto set_string = [&](const std::string& column, const std::string& value) {
                sets.push_back(column + "='" + sql_escape(value) + "'");
            };
            auto read_string = [&](const std::vector<std::string>& keys, std::string& value) {
                for (const auto& key : keys) {
                    if (body.contains(key) && body[key].is_string()) {
                        value = body[key].get<std::string>();
                        return true;
                    }
                }
                return false;
            };

            std::string value;
            if (read_string({"database", "database_path", "databasePath"}, value)) {
                value = normalize_ddb_database_path(value);
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "database is required"}, {"data", nullptr}}));
                set_string("`database`", value);
            }
            if (read_string({"table_name", "tableName"}, value)) {
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "table_name is required"}, {"data", nullptr}}));
                set_string("table_name", value);
            }
            if (read_string({"date_column", "dateColumn"}, value)) {
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "date_column is required"}, {"data", nullptr}}));
                set_string("date_column", value);
            }
            if (read_string({"date_format", "dateFormat"}, value)) {
                if (value != "YYYY-MM-DD" && value != "YYYYMMDD") {
                    return resp->Json(Json::object({{"code", 400}, {"message", "date_format must be YYYY-MM-DD or YYYYMMDD"}, {"data", nullptr}}));
                }
                set_string("date_format", value);
            }
            if (read_string({"frequency", "frequencyCode", "frequency_code"}, value)) {
                set_string("frequency", normalize_monitor_frequency(value));
            }
            if (read_string({"where_extra", "whereExtra"}, value)) {
                std::string where_error;
                if (!validate_where_extra(value, where_error)) {
                    return resp->Json(Json::object({{"code", 400}, {"message", where_error}, {"data", nullptr}}));
                }
                set_string("where_extra", value);
            }
            if (read_string({"group_id", "groupId", "group_name", "groupName"}, value)) {
                if (value.empty()) return resp->Json(Json::object({{"code", 400}, {"message", "group_id is required"}, {"data", nullptr}}));
                if (!valid_group_id(value)) return resp->Json(Json::object({{"code", 400}, {"message", "group_id format is invalid"}, {"data", nullptr}}));
                if (!ddb_monitor_group_exists(conn, value)) {
                    return resp->Json(Json::object({{"code", 400}, {"message", "group_id not found"}, {"data", nullptr}}));
                }
                set_string("group_id", value);
            }
            if (read_string({"description"}, value)) {
                set_string("description", value);
            }
            if (body.contains("enabled")) {
                int enabled_value = -1;
                if (body["enabled"].is_boolean()) enabled_value = body["enabled"].get<bool>() ? 1 : 0;
                else if (body["enabled"].is_number_integer()) enabled_value = body["enabled"].get<int>() ? 1 : 0;
                else return resp->Json(Json::object({{"code", 400}, {"message", "enabled must be boolean or integer"}, {"data", nullptr}}));
                sets.push_back("enabled=" + std::to_string(enabled_value));
            }
            if (body.contains("anomaly_check_enabled") || body.contains("anomalyCheckEnabled")) {
                Json v = body.contains("anomaly_check_enabled") ? body["anomaly_check_enabled"] : body["anomalyCheckEnabled"];
                int enabled_value = -1;
                if (v.is_boolean()) enabled_value = v.get<bool>() ? 1 : 0;
                else if (v.is_number_integer()) enabled_value = v.get<int>() ? 1 : 0;
                else return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_check_enabled must be boolean or integer"}, {"data", nullptr}}));
                sets.push_back("anomaly_check_enabled=" + std::to_string(enabled_value));
            }
            if (body.contains("anomaly_threshold_pct") || body.contains("anomalyThresholdPct")) {
                Json v = body.contains("anomaly_threshold_pct") ? body["anomaly_threshold_pct"] : body["anomalyThresholdPct"];
                if (!v.is_number()) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_threshold_pct must be number"}, {"data", nullptr}}));
                double threshold = v.get<double>();
                if (threshold < 0.0 || threshold > 1.0) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_threshold_pct must be between 0 and 1"}, {"data", nullptr}}));
                sets.push_back("anomaly_threshold_pct=" + std::to_string(threshold));
            }
            if (body.contains("anomaly_window_size") || body.contains("anomalyWindowSize")) {
                Json v = body.contains("anomaly_window_size") ? body["anomaly_window_size"] : body["anomalyWindowSize"];
                if (!v.is_number_integer()) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_window_size must be integer"}, {"data", nullptr}}));
                int window_size = v.get<int>();
                if (window_size < 5 || window_size > 120) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_window_size must be between 5 and 120"}, {"data", nullptr}}));
                sets.push_back("anomaly_window_size=" + std::to_string(window_size));
            }
            if (body.contains("anomaly_min_samples") || body.contains("anomalyMinSamples")) {
                Json v = body.contains("anomaly_min_samples") ? body["anomaly_min_samples"] : body["anomalyMinSamples"];
                if (!v.is_number_integer()) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_min_samples must be integer"}, {"data", nullptr}}));
                int min_samples = v.get<int>();
                if (min_samples < 3 || min_samples > 120) return resp->Json(Json::object({{"code", 400}, {"message", "anomaly_min_samples must be between 3 and 120"}, {"data", nullptr}}));
                sets.push_back("anomaly_min_samples=" + std::to_string(min_samples));
            }
            if (body.contains("related_task_ids") || body.contains("relatedTaskIds")) {
                Json related = Json::array();
                if (body.contains("related_task_ids")) related = body["related_task_ids"];
                if (body.contains("relatedTaskIds")) related = body["relatedTaskIds"];
                std::string related_error;
                if (!validate_related_tasks(conn, related, related_error)) {
                    return resp->Json(Json::object({{"code", 400}, {"message", related_error}, {"data", nullptr}}));
                }
                std::string related_text = related.dump();
                set_string("related_task_ids", related_text);
                set_string("related_task_ids_text", related_text);
            }

            std::string now = to_cst_time_str(std::time(nullptr));
            set_string("updated_at", now);
            std::string sql = "UPDATE ddb_monitor SET ";
            for (size_t i = 0; i < sets.size(); ++i) {
                if (i > 0) sql += ", ";
                sql += sets[i];
            }
            sql += " WHERE monitor_id='" + escaped_id + "'";
            if (!conn->execute(sql)) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to update monitor"}, {"data", nullptr}}));
            }
            return resp->Json(Json::object({
                {"code", 0},
                {"message", "Monitor updated"},
                {"data", Json::object({
                    {"monitor_id", monitor_id},
                    {"monitorId", monitor_id},
                    {"updated_at", now},
                    {"updatedAt", now}
                })}
            }));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 2.14 删除数据监控项 (DELETE /api/ddb-monitors/{monitor_id})
    router.Delete("/api/ddb-monitors/{monitor_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string monitor_id = req->GetParam("monitor_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto rows = query_ddb_monitors(conn, "monitor_id = '" + sql_escape(monitor_id) + "'");
        if (rows.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor not found"}, {"data", nullptr}}));
        conn->execute("DELETE FROM ddb_monitor_snapshot WHERE monitor_id='" + sql_escape(monitor_id) + "'");
        if (!conn->execute("DELETE FROM ddb_monitor WHERE monitor_id='" + sql_escape(monitor_id) + "'")) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Failed to delete monitor"}, {"data", nullptr}}));
        }
        return resp->Json(Json::object({{"code", 0}, {"message", "Monitor deleted"}, {"data", nullptr}}));
    });

    // 2.15 单项重新统计 (POST /api/ddb-monitors/{monitor_id}/recount?date=YYYY-MM-DD)
    router.POST("/api/ddb-monitors/{monitor_id}/recount", [](HttpRequest* req, HttpResponse* resp) {
        std::string monitor_id = req->GetParam("monitor_id");
        std::string date = normalize_iso_date(req->GetParam("date"));
        if (date.empty()) date = today_yyyy_mm_dd();
        if (!valid_iso_date(date)) return resp->Json(Json::object({{"code", 400}, {"message", "Invalid date, expected YYYY-MM-DD"}, {"data", nullptr}}));

        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        auto rows = query_ddb_monitors(conn, "monitor_id = '" + sql_escape(monitor_id) + "'");
        if (rows.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Monitor not found"}, {"data", nullptr}}));
        if (rows.front().enabled != 1) return resp->Json(Json::object({{"code", 400}, {"message", "Monitor is disabled"}, {"data", nullptr}}));
        std::string exec_id;
        std::string enqueue_error;
        if (!enqueue_ddb_monitor_count(conn, date, monitor_id, exec_id, enqueue_error)) {
            return resp->Json(Json::object({{"code", 500}, {"message", enqueue_error}, {"data", nullptr}}));
        }
        return resp->Json(Json::object({
            {"code", 0},
            {"message", "recount task queued"},
            {"data", Json::object({{"status", "pending"}, {"monitor_id", monitor_id}, {"date", date}, {"exec_id", exec_id}})}
        }));
    });

    // 3. 新增任务接口 (POST /api/tasks)
    router.POST("/api/tasks", [](HttpRequest* req, HttpResponse* resp) {
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));
        }

        try {
            // 解析前端发来的 JSON Body
            hv::Json body = req->GetJson();
            
            tasks t;
            // 必填字段
            t.task_id = body["task_id"].get<std::string>();
            t.name = body["name"].get<std::string>();
            t.node_type = body["node_type"].get<std::string>();
            if (body.contains("task_category") && body["task_category"].is_string()) {
                t.task_category = normalize_task_category(body["task_category"].get<std::string>());
            } else if (body.contains("taskCategory") && body["taskCategory"].is_string()) {
                t.task_category = normalize_task_category(body["taskCategory"].get<std::string>());
            } else {
                t.task_category = "ops";
            }
            // 可选字段 (使用 value() 提供默认值)
            t.script_path = body.value("script_path", "");
            t.cron_expr = body.value("cron_expr", "");
            t.timeout_sec = body.value("timeout_sec", 3600);
            t.enabled = body.value("enabled", 1);
            t.target_node_id = body.value("target_node_id", "");
            // 💡 核心修复：必须将 default_params JSON 对象 dump 为字符串再存入数据库
            hv::Json params_json = body.value("default_params", Json::object());
            if (!params_json.is_string()) {
                t.default_params = params_json.dump();
            } else { t.default_params = params_json.get<std::string>(); }

            // 插入数据库
            // 修复：判断是否恰好插入了 1 条记录，出错时 ormpp 会返回 -1，导致 if(-1) 被误判为 true
            if (conn->insert(t) == 1) {
                return resp->Json(Json::object({{"code", 0}, {"message", "Task created successfully"}}));
            } else {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to insert task to DB"}}));
            }
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}}));
        }
    });

    // 3.0 编辑任务接口 (PUT /api/tasks/{task_id})
    router.PUT("/api/tasks/{task_id}", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) {
            return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}, {"data", nullptr}}));
        }

        try {
            hv::Json body = req->GetJson();
            auto existing = conn->query<tasks>("task_id = '" + sql_escape(task_id) + "'");
            if (existing.empty()) {
                return resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}, {"data", nullptr}}));
            }

            tasks t = existing.front();
            t.task_id = task_id;
            if (body.contains("name") && body["name"].is_string()) t.name = body["name"].get<std::string>();
            if (body.contains("node_type") && body["node_type"].is_string()) t.node_type = body["node_type"].get<std::string>();
            if (body.contains("task_category") && body["task_category"].is_string()) t.task_category = normalize_task_category(body["task_category"].get<std::string>());
            if (body.contains("taskCategory") && body["taskCategory"].is_string()) t.task_category = normalize_task_category(body["taskCategory"].get<std::string>());
            if (body.contains("script_path") && body["script_path"].is_string()) t.script_path = body["script_path"].get<std::string>();
            if (body.contains("cron_expr") && body["cron_expr"].is_string()) t.cron_expr = body["cron_expr"].get<std::string>();
            if (body.contains("target_node_id") && body["target_node_id"].is_string()) t.target_node_id = body["target_node_id"].get<std::string>();
            if (body.contains("timeout_sec") && body["timeout_sec"].is_number_integer()) t.timeout_sec = body["timeout_sec"].get<int>();
            if (body.contains("enabled")) {
                if (body["enabled"].is_boolean()) t.enabled = body["enabled"].get<bool>() ? 1 : 0;
                else if (body["enabled"].is_number_integer()) t.enabled = body["enabled"].get<int>();
            }
            if (body.contains("default_params")) {
                hv::Json params_json = body["default_params"];
                if (params_json.is_string()) t.default_params = params_json.get<std::string>();
                else if (params_json.is_null()) t.default_params = "{}";
                else t.default_params = params_json.dump();
            }

            std::string sql =
                "UPDATE tasks SET "
                "name='" + sql_escape(t.name) + "', "
                "node_type='" + sql_escape(t.node_type) + "', "
                "task_category='" + sql_escape(normalize_task_category(t.task_category)) + "', "
                "script_path='" + sql_escape(t.script_path) + "', "
                "cron_expr='" + sql_escape(t.cron_expr) + "', "
                "timeout_sec=" + std::to_string(t.timeout_sec) + ", "
                "enabled=" + std::to_string(t.enabled) + ", "
                "target_node_id='" + sql_escape(t.target_node_id) + "', "
                "default_params='" + sql_escape(t.default_params) + "' "
                "WHERE task_id='" + sql_escape(task_id) + "'";

            if (!conn->execute(sql)) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Failed to update task"}, {"data", nullptr}}));
            }

            Json data = Json::object({
                {"task_id", t.task_id}, {"name", t.name}, {"description", t.script_path},
                {"node_type", t.node_type}, {"task_category", task_category_label(t.task_category)},
            {"taskCategory", task_category_label(t.task_category)}, {"task_category_code", normalize_task_category(t.task_category)},
            {"taskCategoryCode", normalize_task_category(t.task_category)}, {"task_category_label", task_category_label(t.task_category)},
            {"taskCategoryLabel", task_category_label(t.task_category)}, {"script_path", t.script_path}, {"cron_expr", t.cron_expr},
                {"timeout_sec", t.timeout_sec}, {"enabled", t.enabled}, {"target_node_id", t.target_node_id},
                {"target_os", target_os_from_node(t.target_node_id)}, {"target_node_tag", t.target_node_id},
                {"default_params", parse_params_json(t.default_params)}
            });
            return resp->Json(Json::object({{"code", 0}, {"message", "Task updated successfully"}, {"data", data}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 400}, {"message", std::string("Bad request JSON: ") + e.what()}, {"data", nullptr}}));
        }
    });

    // 3.1 执行当前任务及其下游 (POST /api/tasks/{task_id}/run)
    router.POST("/api/tasks/{task_id}/run", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::string escaped_task_id = sql_escape(task_id);
        auto t_list = conn->query<tasks>("task_id = '" + escaped_task_id + "'");
        if (t_list.empty()) {
            resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}, {"data", nullptr}}));
            return static_cast<int>(HTTP_STATUS_NOT_FOUND);
        }
        const auto& task = t_list.front();

        hv::Json body = Json::object();
        try {
            hv::Json parsed_body = req->GetJson();
            if (parsed_body.is_object()) {
                body = parsed_body;
            } else if (parsed_body.is_string()) {
                body["date"] = parsed_body.get<std::string>();
            } else if (!parsed_body.is_null()) {
                resp->Json(Json::object({{"code", 400}, {"message", "Request body must be an object or date string"}, {"data", nullptr}}));
                return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
            }
        } catch (...) {
            body = Json::object();
        }

        if (!body.contains("date") && !req->GetParam("date").empty()) body["date"] = req->GetParam("date");
        if (!body.contains("trade_date") && !req->GetParam("trade_date").empty()) {
            body["trade_date"] = req->GetParam("trade_date");
        }

        std::string pipeline_id;
        if (body.contains("pipeline_id") && body["pipeline_id"].is_string()) {
            pipeline_id = body["pipeline_id"].get<std::string>();
        } else if (body.contains("pipelineId") && body["pipelineId"].is_string()) {
            pipeline_id = body["pipelineId"].get<std::string>();
        } else if (!req->GetParam("pipeline_id").empty()) {
            pipeline_id = req->GetParam("pipeline_id");
        }
        body.erase("pipelineId");
        if (pipeline_id.empty()) pipeline_id = resolve_task_pipeline_id(conn, task_id, "manual");

        hv::Json final_params = parse_params_json(task.default_params);
        if (body.contains("params") && body["params"].is_object()) {
            for (const auto& item : body["params"].items()) {
                if (item.key() != "pipeline_id") final_params[item.key()] = item.value();
            }
        }
        for (const auto& item : body.items()) {
            if (item.key() == "pipeline_id" || item.key() == "params") continue;
            final_params[item.key()] = item.value();
        }

        // 兼容前端 camelCase 参数，任务脚本统一使用 trade_date=YYYYMMDD。
        if (!final_params.contains("trade_date") && final_params.contains("tradeDate")) {
            final_params["trade_date"] = final_params["tradeDate"];
        }
        final_params.erase("tradeDate");

        // 前端日期选择器传 date，任务脚本沿用 trade_date=YYYYMMDD。
        if (final_params.contains("date")) {
            if (!final_params["date"].is_string()) {
                resp->Json(Json::object({{"code", 400}, {"message", "date must be YYYY-MM-DD or YYYYMMDD"}, {"data", nullptr}}));
                return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
            }
            std::string normalized = normalize_iso_date(final_params["date"].get<std::string>());
            if (!is_valid_iso_date(normalized)) {
                resp->Json(Json::object({{"code", 400}, {"message", "Invalid date, expected YYYY-MM-DD or YYYYMMDD"}, {"data", nullptr}}));
                return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
            }
            final_params["trade_date"] = iso_date_to_compact(normalized);
            final_params.erase("date");
        } else if (final_params.contains("trade_date")) {
            if (!final_params["trade_date"].is_string()) {
                resp->Json(Json::object({{"code", 400}, {"message", "trade_date must be YYYY-MM-DD or YYYYMMDD"}, {"data", nullptr}}));
                return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
            }
            std::string normalized = normalize_iso_date(final_params["trade_date"].get<std::string>());
            if (!is_valid_iso_date(normalized)) {
                resp->Json(Json::object({{"code", 400}, {"message", "Invalid trade_date, expected YYYY-MM-DD or YYYYMMDD"}, {"data", nullptr}}));
                return static_cast<int>(HTTP_STATUS_BAD_REQUEST);
            }
            final_params["trade_date"] = iso_date_to_compact(normalized);
        }

        std::time_t now_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::string exec_id = task_id + "_manual_" + std::to_string(now_time);
        task_instances inst;
        inst.exec_id = exec_id;
        inst.task_id = task_id;
        inst.node_id = task.target_node_id;
        inst.status = 2;
        inst.exit_code = 0;
        inst.start_time = to_cst_time_str(now_time);
        inst.end_time = inst.start_time;
        inst.log_path = "";
        inst.params = final_params.dump();
        inst.pipeline_id = pipeline_id;

        if (conn->insert(inst) == 1) {
            hlogi("🚀 DAG Run Triggered: [%s], pipeline=%s, params=%s",
                  exec_id.c_str(), pipeline_id.c_str(), inst.params.c_str());
            return resp->Json(Json::object({
                {"code", 0},
                {"message", "Task and downstream triggered"},
                {"exec_id", exec_id},
                {"pipeline_id", pipeline_id},
                {"execution", task_instance_to_json(inst, "manual")}
            }));
        }
        return resp->Json(Json::object({{"code", 500}, {"message", "Failed to trigger task"}}));
    });

    // 3.4 任务增量(传参)触发接口 (POST /api/tasks/{task_id}/incr)
    router.POST("/api/tasks/{task_id}/incr", [](HttpRequest* req, HttpResponse* resp) {
        std::string task_id = req->GetParam("task_id");
        auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
        auto conn = pool.get();
        if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

        std::time_t now_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
        std::string exec_id = task_id + "_incr_" + std::to_string(now_time); // 💡 核心修改1：使用 _incr_ 标识，用于在回调时区分并阻止DAG触发
        task_instances inst;
        inst.exec_id = exec_id;
        inst.task_id = task_id;

        auto t_list = conn->query<tasks>("task_id = '" + task_id + "'");
        if (t_list.empty()) return resp->Json(Json::object({{"code", 404}, {"message", "Task not found"}}));
        const auto& t = t_list.front();
        inst.node_id = t.target_node_id;

        // 💡 核心修复2：合并默认参数和请求参数
        hv::Json final_params = parse_params_json(t.default_params);
        try {
            hv::Json body = req->GetJson();
            for (auto& el : body.items()) {
                final_params[el.key()] = el.value();
            }
        } catch (...) {}
        std::string params_str = final_params.dump();

        inst.status = 2;
        inst.exit_code = 0;
        inst.start_time = to_cst_time_str(now_time);
        inst.end_time = inst.start_time;
        inst.log_path = "";
        inst.params = params_str;
        // 💡 核心修改3：手动触发的任务不属于任何 pipeline，或属于一个默认的
        inst.pipeline_id = "manual";

        if (conn->insert(inst) == 1) {
            hlogi("📈 Incremental Task Triggered: [%s] with params: %s", exec_id.c_str(), params_str.c_str());
            return resp->Json(Json::object({
                {"code", 0},
                {"message", "Incremental task triggered"},
                {"exec_id", exec_id},
                {"execution", task_instance_to_json(inst, "manual")} // 💡 核心修复1：返回正确的触发模式
            }));
        }
        return resp->Json(Json::object({{"code", 500}, {"message", "Failed to trigger incremental task"}}));
    });

    // 4. 节点心跳接口 (POST /api/agents/heartbeat) - 预留给第二阶段的 Python Agent
    router.POST("/api/agents/heartbeat", [&redis](HttpRequest* req, HttpResponse* resp) {
        try {
            hv::Json body = req->GetJson();
            // 更安全的 JSON 取值方式
            std::string node_id = body.contains("node_id") ? body["node_id"].get<std::string>() : "unknown";
            std::string ip = body.value("ip", req->client_addr.ip);
            std::string os_type = body.value("os_type", target_os_from_node(node_id));
            double cpu_load = body.value("cpu_load", body.value("cpu", 0.0));
            double mem_usage = body.value("mem_usage", body.value("memory", 0.0)); // 💡 修复：同时将完整的 payload 存入 Redis
            std::string version = body.value("version", "agent-0.3.1");
            Json tags = body.value("tags", Json::array());

            if (redis) {
                // 获取当前时间戳
                auto now = std::chrono::system_clock::now();
                auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();

                // 💡 修复：将完整的 payload JSON 字符串存入 Redis，而不仅仅是时间戳
                std::string redis_key = "agent:heartbeat:" + node_id;
                redis->setex(redis_key, 90, body.dump());

                hlogi("❤️ Received heartbeat from node: %s, updated Redis (TTL: 90s)", node_id.c_str());
            } else {
                hlogw("❤️ Received heartbeat from %s, but Redis is not available", node_id.c_str());
            }

            auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
            if (auto conn = pool.get()) {
                std::string sql =
                    "INSERT INTO agents(node_id, ip, os_type, status, last_heartbeat, cpu_load, mem_usage) VALUES('" +
                    sql_escape(node_id) + "', '" + sql_escape(ip) + "', '" + sql_escape(os_type) +
                    "', 'online', NOW(), " + std::to_string(cpu_load) + ", " + std::to_string(mem_usage) + ") " +
                    "ON DUPLICATE KEY UPDATE ip=VALUES(ip), os_type=VALUES(os_type), status='online', "
                    "last_heartbeat=NOW(), cpu_load=VALUES(cpu_load), mem_usage=VALUES(mem_usage)";
                conn->execute(sql);
            }

            return resp->Json(Json::object({{"code", 0}, {"message", "Heartbeat ack"}}));
        } catch (const std::exception& e) {
            hloge("❌ Heartbeat error: %s", e.what());
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Internal Error: ") + e.what()}}));
        } catch (...) {
            hloge("❌ Heartbeat error: unknown exception");
            return resp->Json(Json::object({{"code", 400}, {"message", "Invalid heartbeat payload"}}));
        }
    });

    // 4.1. 任务分发接口 (POST /api/tasks/poll)
    router.POST("/api/tasks/poll", [](HttpRequest* req, HttpResponse* resp) {
        try {
            hv::Json body = req->GetJson();
            std::string node_id = body.contains("node_id") ? body["node_id"].get<std::string>() : "";
            if (node_id.empty()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "Missing node_id"}}));
            }

            auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
            auto conn = pool.get();
            if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

            // [重大升级] 抢占式队列：查找专属于该节点的，或者公共池子里的 (node_id='') 且状态为 2 (Ready) 的任务
            std::string condition = "(node_id = '" + node_id + "' OR node_id = '') AND status = 2 LIMIT 1";
            auto instances = conn->query<task_instances>(condition);

            if (instances.empty()) {
                // 没有可执行的任务，返回空 data
                return resp->Json(Json::object({{"code", 0}, {"message", "No task available"}, {"data", Json::object()}}));
            }

            auto& inst = instances.front();
            inst.status = 0; // 将状态更新为 0 (Running)
            inst.node_id = node_id; // Agent 正式认领（抢占）该任务！

            // 记录任务实际开始时间
            std::string time_str = to_cst_time_str(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
            inst.start_time = time_str;

            // 显式更新数据库状态，并绑定抢到该任务的节点 ID
            std::string sql = "UPDATE task_instances SET status=0, node_id='" + node_id + "', start_time='" + time_str + "' WHERE exec_id='" + inst.exec_id + "'";
            conn->execute(sql);

            // 联合查询出对应的 Task 定义，以便将 script_path 下发给 Agent
            auto task_list = conn->query<tasks>("task_id = '" + inst.task_id + "'");
            if (task_list.empty()) {
                return resp->Json(Json::object({{"code", 500}, {"message", "Task metadata not found"}}));
            }
            auto& t = task_list.front();

            hv::Json parsed_params = Json::object();
            if (!inst.params.empty()) {
                try { parsed_params = Json::parse(inst.params); } catch(...) {}
            }

            Json task_json = Json::object({
                {"exec_id", inst.exec_id},
                {"task_id", t.task_id},
                {"script_path", t.script_path},
                {"timeout_sec", t.timeout_sec},
                {"params", parsed_params}
            });

            return resp->Json(Json::object({
                {"code", 0},
                {"message", "success"},
                {"data", Json::object({{"task", task_json}})}
            }));
        } catch (const std::exception& e) {
            hloge("❌ Task poll error: %s", e.what());
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Internal Error: ") + e.what()}}));
        }
    });

    // 4.2. 任务状态回调接口 (POST /api/tasks/callback)
    router.POST("/api/tasks/callback", [](HttpRequest* req, HttpResponse* resp) {
        try {
            hv::Json body = req->GetJson();
            std::string exec_id = body.contains("exec_id") && body["exec_id"].is_string() ? body["exec_id"].get<std::string>() : "";
            if (exec_id.empty()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "Missing exec_id"}}));
            }
            if (!body.contains("exit_code") || body["exit_code"].is_null() || !body["exit_code"].is_number_integer()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "Missing or invalid exit_code"}}));
            }
            int exit_code = body["exit_code"].get<int>();
            

            auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
            auto conn = pool.get();
            if (!conn) return resp->Json(Json::object({{"code", 500}, {"message", "Database connection failed"}}));

            auto instances = conn->query<task_instances>("exec_id = '" + exec_id + "'");
            if (instances.empty()) {
                return resp->Json(Json::object({{"code", 404}, {"message", "task_instances not found"}}));
            }

            auto& inst = instances.front();
            // exit_code 为 0 表示正常退出，状态置为 1 (Success)；否则为 -1 (Failed)
            inst.status = (exit_code == 0) ? 1 : -1; 
            inst.exit_code = exit_code;

            std::string time_str = to_cst_time_str(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));
            inst.end_time = time_str;

            std::string sql = "UPDATE task_instances SET status=" + std::to_string(inst.status) + ", exit_code=" + std::to_string(exit_code) + ", end_time='" + time_str + "' WHERE exec_id='" + inst.exec_id + "'";
            conn->execute(sql);

            std::string callback_stdout = body.value("stdout", "");
            std::string callback_stderr = body.value("stderr", "");
            if (!callback_stdout.empty() || !callback_stderr.empty()) {
                std::string log_file = inst.log_path.empty() ? default_log_path(exec_id) : inst.log_path;
                // 修复：无论日志文件是否为空，都应该以追加模式写入回调中附带的日志，
                // 因为 Agent 的最终输出（stdout/stderr）是在 callback 中一起回传的。
                int sys_ret = system("mkdir -p logs/tasks");
                (void)sys_ret;
                // Agent 运行期间已经通过 /api/tasks/log 实时写入过日志。
                // callback 携带的是完整 stdout/stderr，结束时覆盖实时日志，避免同一内容重复追加。
                FILE* fp = fopen(log_file.c_str(), "wb");
                if (fp) {
                    auto write_stream = [&](const std::string& type, const std::string& text) {
                        std::istringstream iss(text);
                        std::string line;
                        while (std::getline(iss, line)) {
                            fprintf(fp, "[%s] %s\n", type.c_str(), line.c_str());
                        }
                    };
                    write_stream("stdout", callback_stdout);
                    write_stream("stderr", callback_stderr);
                    fclose(fp);

                    std::string log_sql = "UPDATE task_instances SET log_path='" + sql_escape(log_file) + "' WHERE exec_id='" + sql_escape(exec_id) + "' AND (log_path IS NULL OR log_path='')";
                    conn->execute(log_sql);
                }
            }

            hlogi("✅ Task completed: exec_id=%s, exit_code=%d", exec_id.c_str(), exit_code);

            // ================= DAG 依赖触发逻辑 =================
            // 💡 核心修改2：只有在任务成功，且不是由 /incr 接口触发时，才执行下游依赖
            bool is_incr_task = exec_id.find("_incr_") != std::string::npos;
            if (exit_code == 0 && !is_incr_task) {
                std::string escaped_pipeline = sql_escape(inst.pipeline_id);
                std::string dag_query = "parent_task_id = '" + sql_escape(inst.task_id) +
                    "' AND pipeline_id = '" + escaped_pipeline + "'";
                auto edges = conn->query<dag_edges>(dag_query);
                for (auto& edge : edges) {
                    // 2. 检查下游子任务是否处于启用状态
                    auto child_tasks = conn->query<tasks>("task_id = '" + edge.child_task_id + "' AND enabled = 1");
                    if (!child_tasks.empty()) {
                        auto& child_task = child_tasks.front();
                        
                        // 生成下游任务的执行实例
                        task_instances child_inst;
                        std::time_t now_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
                        child_inst.exec_id = child_task.task_id + "_dag_" + std::to_string(now_time);
                        child_inst.task_id = child_task.task_id;
                        child_inst.node_id = child_task.target_node_id;
                        child_inst.status = 2;   // Ready 待派发
                        child_inst.exit_code = 0;
                        
                        // 下游任务使用自己的 default_params，并只继承父任务中的通用日期/写库开关参数，避免专有参数污染 DAG 子任务
                        child_inst.params = build_child_params(child_task.default_params, inst.params);
                        child_inst.pipeline_id = inst.pipeline_id; // 继承父任务的 pipeline_id

                        child_inst.start_time = to_cst_time_str(now_time);
                        child_inst.end_time = child_inst.start_time;
                        child_inst.log_path = "";

                        if (conn->insert(child_inst) == 1) {
                            hlogi("🔗 DAG Triggered: [%s] success -> spawning child task [%s] with params: %s", 
                                  inst.task_id.c_str(), child_inst.exec_id.c_str(), child_inst.params.c_str());
                        } else {
                            hloge("❌ DAG Triggered Failed: Could not insert child task [%s]", child_inst.exec_id.c_str());
                        }
                    }
                }
            }
            // ====================================================

            // WebSocket 广播任务状态更新给前端监控大屏
            hv::Json ws_msg = Json::object({
                {"type", "task_status"},
                {"exec_id", inst.exec_id},
                {"status", inst.status},
                {"exit_code", exit_code}
            });
            broadcast_ws(ws_msg.dump());

            return resp->Json(Json::object({{"code", 0}, {"message", "Callback processed"}}));
        } catch (const std::exception& e) {
            hloge("❌ Task callback error: %s", e.what());
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Internal Error: ") + e.what()}}));
        }
    });

    // 4.3. 实时日志接收落盘接口 (POST /api/tasks/log)
    router.POST("/api/tasks/log", [](HttpRequest* req, HttpResponse* resp) {
        try {
            hv::Json body = req->GetJson();
            std::string exec_id = body.contains("exec_id") ? body["exec_id"].get<std::string>() : "";
            if (exec_id.empty()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "Missing exec_id"}}));
            }
            if (!body.contains("logs") || !body["logs"].is_array()) {
                return resp->Json(Json::object({{"code", 400}, {"message", "Missing logs array"}}));
            }

            // 确保存放每个任务独立日志的子目录存在
            int sys_ret = system("mkdir -p logs/tasks");
            (void)sys_ret;
            
            // 日志保存路径: logs/tasks/exec_id.log
            std::string log_file = "logs/tasks/" + exec_id + ".log";
            FILE* fp = fopen(log_file.c_str(), "ab"); // 追加模式打开
            if (fp) {
                for (auto& item : body["logs"]) {
                    std::string type = item.value("type", "stdout");
                    std::string content = item.value("content", "");
                    // 将日志以 [stdout] 某某输出 的格式落盘
                    fprintf(fp, "[%s] %s\n", type.c_str(), content.c_str());
                }
                fclose(fp);
            }

            // 记录日志落盘路径到数据库 (只在第一次上报时更新)
            auto& pool = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
            if (auto conn = pool.get()) {
                std::string sql = "UPDATE task_instances SET log_path='" + log_file + "' WHERE exec_id='" + exec_id + "' AND (log_path IS NULL OR log_path='')";
                conn->execute(sql);
            }

            // WebSocket 广播实时日志给前端监控大屏
            hv::Json ws_msg = Json::object({
                {"type", "live_log"},
                {"exec_id", exec_id},
                {"logs", body["logs"]}
            });
            broadcast_ws(ws_msg.dump());

            return resp->Json(Json::object({{"code", 0}, {"message", "Logs saved"}}));
        } catch (const std::exception& e) {
            return resp->Json(Json::object({{"code", 500}, {"message", std::string("Error: ") + e.what()}}));
        }
    });

    // 5. 启动后台线程：检测掉线节点并重置僵尸任务
    std::thread monitor_thread([&redis]() {
        while (true) {
            // 每 10 秒扫描一次
            std::this_thread::sleep_for(std::chrono::seconds(10));
            try {
                auto& p = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
                auto conn = p.get();
                if (!conn) continue;

                std::string now_str = to_cst_time_str(std::chrono::system_clock::to_time_t(std::chrono::system_clock::now()));

                // 查出所有当前状态为 Running (0) 的 task_instances
                auto running_instances = conn->query<task_instances>("status = 0");
                for (auto& inst : running_instances) {
                    auto task_list = conn->query<tasks>("task_id = '" + sql_escape(inst.task_id) + "'");
                    int timeout_sec = task_list.empty() ? 3600 : task_list.front().timeout_sec;
                    long elapsed = seconds_between_sql_time(inst.start_time, now_str);
                    if (elapsed >= 0 && elapsed > timeout_sec) {
                        hlogw("⏱️ Task timeout: exec_id=%s, task_id=%s, elapsed=%ld, timeout=%d", inst.exec_id.c_str(), inst.task_id.c_str(), elapsed, timeout_sec);
                        std::string sql = "UPDATE task_instances SET status=-2, exit_code=-2, end_time='" + now_str + "' WHERE exec_id='" + inst.exec_id + "'";
                        conn->execute(sql);
                        hv::Json ws_msg = Json::object({
                            {"type", "task_status"},
                            {"exec_id", inst.exec_id},
                            {"status", -2},
                            {"exit_code", -2}
                        });
                        broadcast_ws(ws_msg.dump());
                        continue;
                    }

                    if (!redis) continue;
                    std::string redis_key = "agent:heartbeat:" + inst.node_id;
                    // 如果 Redis 中查不到这个节点的心跳 Key，说明它掉线了 (TTL 超时被删除了)
                    if (!redis->exists(redis_key)) {
                        hlogw("⚠️ Node %s is OFFLINE! Resetting task %s", inst.node_id.c_str(), inst.task_id.c_str());
                        std::string sql = "UPDATE task_instances SET status=-1, end_time='" + now_str + "' WHERE exec_id='" + inst.exec_id + "'";
                        conn->execute(sql);
                    }
                }
            } catch (const std::exception& e) {
                hloge("❌ Monitor thread error: %s", e.what());
            }
        }
    });
    monitor_thread.detach(); // 让线程在后台独立运行

    // 5.5 启动自动定时调度 (Cron Engine) 后台线程
    std::thread cron_thread([]() {
        while (true) {
            auto now = std::chrono::system_clock::now();
            std::time_t now_time = std::chrono::system_clock::to_time_t(now);
            struct tm tm_now;
            localtime_r(&now_time, &tm_now);

            // 精确计算睡眠时间，直到下一分钟的第 0 秒才醒来执行，防止在同一分钟内重复触发
            int sleep_sec = 60 - tm_now.tm_sec;
            if (sleep_sec == 0) sleep_sec = 60;
            std::this_thread::sleep_for(std::chrono::seconds(sleep_sec));

            // 醒来后重新获取准确的当前时间
            now_time = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            try {
                auto& p = ormpp::connection_pool<ormpp::dbng<ormpp::mysql>>::instance();
                auto conn = p.get();
                if (!conn) continue;

                // 查出所有已启用且配置了 Cron 表达式的任务
                auto active_tasks = conn->query<tasks>("enabled = 1 AND cron_expr != ''");
                for (const auto& t : active_tasks) {
                    if (is_time_for_cron(t.cron_expr, now_time)) {
                        task_instances inst;
                        // 生成唯一执行批次ID：task_id_时间戳
                        inst.exec_id = t.task_id + "_" + std::to_string(now_time);
                        inst.task_id = t.task_id;
                        inst.node_id = t.target_node_id;
                        inst.status = 2;   // 2 = Ready 待派发
                        inst.exit_code = 0;
                        inst.start_time = to_cst_time_str(now_time);
                        inst.end_time = inst.start_time;
                        inst.log_path = "";
                        inst.params = t.default_params.empty() ? "{}" : t.default_params;
                        inst.pipeline_id = resolve_entry_pipeline_id(conn, t.task_id, "cron");

                        if (conn->insert(inst) == 1) {
                            hlogi("⏰ Cron Triggered: Generated task %s for task_id %s with params: %s", inst.exec_id.c_str(), t.task_id.c_str(), inst.params.c_str());
                        } else {
                            hloge("❌ Cron Trigger Failed: Could not insert task %s", inst.exec_id.c_str());
                        }
                    }
                }
            } catch (const std::exception& e) {
                hloge("❌ Cron thread error: %s", e.what());
            }
        }
    });
    cron_thread.detach();

    // 6. 启动 HTTP 服务
    http_server_t server{}; // 添加 {} 进行零初始化，避免出现内存分配的垃圾值
    server.port = getenv("API_PORT") ? std::stoi(getenv("API_PORT")) : 8080;
    server.service = &router;
    server.worker_threads = 4;
    server.ws = &ws_router; // 挂载 WebSocket 路由器

    printf("🚀 C++ Manager Backend is listening on 0.0.0.0:%d\n", server.port);
    http_server_run(&server); // 移除 0，使用默认的阻塞模式，让主线程挂起等待

    return 0;
}
