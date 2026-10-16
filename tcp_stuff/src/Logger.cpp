#include <Logger.hpp>

#include <iostream>
#include <thread>
#include <algorithm>

// CMake should define this via target_compile_definitions, e.g.:
//   target_compile_definitions(target PRIVATE DEFAULT_LOG_LEVEL="LOG_INFO")
// If not set, fall back to LOG_INFO.
#ifndef DEFAULT_LOG_LEVEL
    #define DEFAULT_LOG_LEVEL "LOG_INFO"
#endif

namespace logging {

// ── Default values ──────────────────────────────────────────────────────────

thread_local std::shared_ptr<Logger> Logger::instance;

LogLevel Logger::logLevel = Logger::parseLogLevel(DEFAULT_LOG_LEVEL);
std::string Logger::timeFormat = "%m/%d %H:%M:%S";
std::string Logger::logFileName = "";
std::ofstream Logger::logfile{};
bool Logger::colorEnabled = false;
bool Logger::logToFile = false;
size_t Logger::counter = 0;

// ── Color code table ────────────────────────────────────────────────────────

thread_local std::unordered_map<LogColor, ColorCode, LogColorHash> Logger::colorCodes_ = {
    {LogColor::BLACK,         {"black",         "\033[30m"}},
    {LogColor::GREY,          {"grey",          "\033[1;30m"}},
    {LogColor::RED,           {"red",           "\033[31m"}},
    {LogColor::GREEN,         {"green",         "\033[32m"}},
    {LogColor::YELLOW,        {"yellow",        "\033[33m"}},
    {LogColor::BLUE,          {"blue",          "\033[34m"}},
    {LogColor::MAGENTA,       {"magenta",       "\033[35m"}},
    {LogColor::CYAN,          {"cyan",          "\033[36m"}},
    {LogColor::LIGHT_GRAY,    {"light_gray",    "\033[37m"}},
    {LogColor::DARK_GRAY,     {"dark_gray",     "\033[90m"}},
    {LogColor::LIGHT_RED,     {"light_red",     "\033[91m"}},
    {LogColor::LIGHT_GREEN,   {"light_green",   "\033[92m"}},
    {LogColor::LIGHT_YELLOW,  {"light_yellow",  "\033[93m"}},
    {LogColor::LIGHT_BLUE,    {"light_blue",    "\033[94m"}},
    {LogColor::LIGHT_MAGENTA, {"light_magenta", "\033[95m"}},
    {LogColor::LIGHT_CYAN,    {"light_cyan",    "\033[1;36m"}},
    {LogColor::WHITE,         {"white",         "\033[97m"}},
    {LogColor::NOCOLOR,       {"noc",           "\033[0;39m"}}};

// ── Default format map (hardcoded colors) ───────────────────────────────────

std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash>
Logger::buildDefaultFormatMap() {
    return {
        {LogLevel::LOG_NOFORMAT, std::make_shared<LogFormat>("",        getColorCode(LogColor::NOCOLOR))},
        {LogLevel::LOG_FATAL,    std::make_shared<LogFormat>("FATAL",   getColorCode("red"))},
        {LogLevel::LOG_ERROR,    std::make_shared<LogFormat>("ERROR",   getColorCode("red"))},
        {LogLevel::LOG_CONSOLE,  std::make_shared<LogFormat>("CONSOLE", getColorCode("yellow"))},
        {LogLevel::LOG_WARNING,  std::make_shared<LogFormat>("WARNING", getColorCode("yellow"))},
        {LogLevel::LOG_INFO,     std::make_shared<LogFormat>("INFO",    getColorCode("light_green"))},
        {LogLevel::LOG_SUCCESS,  std::make_shared<LogFormat>("SUCCESS", getColorCode("green"))},
        {LogLevel::LOG_DEBUG1,   std::make_shared<LogFormat>("DEBUG 1", getColorCode("blue"))},
        {LogLevel::LOG_DEBUG2,   std::make_shared<LogFormat>("DEBUG 2", getColorCode("blue"))}};
}

thread_local std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash>
    Logger::formatMap = Logger::buildDefaultFormatMap();

thread_local std::atomic<bool> Logger::initializedAtomic = {false};

// ── Log level parsing ───────────────────────────────────────────────────────

LogLevel Logger::parseLogLevel(const std::string &s) {
    std::string lower = s;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "log_fatal")   return LogLevel::LOG_FATAL;
    if (lower == "log_error")   return LogLevel::LOG_ERROR;
    if (lower == "log_console") return LogLevel::LOG_CONSOLE;
    if (lower == "log_warning") return LogLevel::LOG_WARNING;
    if (lower == "log_info")    return LogLevel::LOG_INFO;
    if (lower == "log_success") return LogLevel::LOG_SUCCESS;
    if (lower == "log_debug1")  return LogLevel::LOG_DEBUG1;
    if (lower == "log_debug2")  return LogLevel::LOG_DEBUG2;

    return LogLevel::LOG_INFO;  // safe default
}

// ── Color code lookup ───────────────────────────────────────────────────────

std::string Logger::getColorCode(std::string color) {
    if (Logger::colorEnabled) {
        for (const auto &entry : Logger::colorCodes_) {
            if (entry.second.name == color) {
                return entry.second.value;
            }
        }
    }
    return "";
}

std::string Logger::getColorCode(LogColor color) {
    if (Logger::colorEnabled) {
        return Logger::colorCodes_.at(color).value;
    }
    return "";
}

// ── Constructors / destructor ───────────────────────────────────────────────

Logger::Logger() {}

Logger::Logger(LogLevel _logLevel) : Logger() {
    Logger::logLevel = _logLevel;
}

Logger::Logger(LogLevel _logLevel, std::string _timeFormat) : Logger(_logLevel) {
    Logger::timeFormat = _timeFormat;
}

Logger::~Logger() {
    instance = nullptr;
}

// ── Runtime configuration helpers ───────────────────────────────────────────

void Logger::setLogLevel(LogLevel level) {
    logLevel = level;
}

void Logger::setColorEnabled(bool enabled) {
    colorEnabled = enabled;
    formatMap = buildDefaultFormatMap();
}

void Logger::setLogFile(const std::string &filename) {
    if (logfile.is_open()) {
        logfile.close();
    }
    logFileName = filename;
    logToFile = true;
    logfile.open(filename, std::ios_base::app);
}

void Logger::disableFileLogging() {
    logToFile = false;
    if (logfile.is_open()) {
        logfile.close();
    }
}

// ── Core logging ────────────────────────────────────────────────────────────

Logger &Logger::flush() {
    if (currentLevel <= logLevel) {
        std::time_t t = std::time(0);
        char ft[64];
        std::strftime(ft, 64, Logger::timeFormat.c_str(), std::localtime(&t));
        std::stringstream s;

        counter++;

        if (colorEnabled) {
            s << Logger::colorCodes_.at(LogColor::NOCOLOR).value;
        }
        if (currentLevel != LogLevel::LOG_NOFORMAT) {
            s << "[" << ft << "\t" << counter << "]";
            s << "[" << formatMap.at(currentLevel)->color
              << std::setfill(' ') << std::setw(7)
              << formatMap.at(currentLevel)->level
              << (colorEnabled ? colorCodes_.at(LogColor::NOCOLOR).value : "")
              << "] ";
        }
        s << str() << std::endl;

        std::cout << s.str();

        if (logToFile) {
            if (currentLevel != LogLevel::LOG_NOFORMAT) {
                logfile << "[" << ft << "]["
                        << std::setfill(' ') << std::setw(7)
                        << formatMap.at(currentLevel)->level
                        << "] " << str() << std::endl;
            } else {
                logfile << "[" << ft << "] " << str() << std::endl;
            }
            logfile.flush();
        }
    }
    str("");
    currentLevel = LogLevel::LOG_CONSOLE;
    return *this;
}

// ── Singleton ───────────────────────────────────────────────────────────────

Logger &Logger::getInstance() {
    if (!initializedAtomic.exchange(true, std::memory_order_acquire)) {
        instance = std::make_shared<Logger>();
    }
    while (!instance) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return *instance;
}

// ── Stream operators ────────────────────────────────────────────────────────

Logger &Logger::operator<<(const LogLevel &level) {
    currentLevel = level;
    return *this;
}

Logger &Logger::operator<<(const LogColor &color) {
    if (Logger::colorEnabled) {
        (*this) << Logger::colorCodes_.at(color).value;
    }
    return *this;
}

}  // namespace logging