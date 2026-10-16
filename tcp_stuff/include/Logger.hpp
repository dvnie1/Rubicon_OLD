#ifndef LOGGER_HPP_
#define LOGGER_HPP_

#include <atomic>
#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>

namespace logging {

typedef std::ostream &(*manip1)(std::ostream &);
typedef std::basic_ios<std::ostream::char_type, std::ostream::traits_type> ios_type;
typedef ios_type &(*manip2)(ios_type &);
typedef std::ios_base &(*manip3)(std::ios_base &);

enum class LogLevel {
    LOG_NOFORMAT,
    LOG_FATAL,
    LOG_ERROR,
    LOG_CONSOLE,
    LOG_WARNING,
    LOG_INFO,
    LOG_SUCCESS,
    LOG_DEBUG1,
    LOG_DEBUG2
};

enum class LogColor {
    NOCOLOR,
    BLACK,
    GREY,
    RED,
    GREEN,
    YELLOW,
    BLUE,
    MAGENTA,
    CYAN,
    LIGHT_GRAY,
    DARK_GRAY,
    LIGHT_RED,
    LIGHT_GREEN,
    LIGHT_YELLOW,
    LIGHT_BLUE,
    LIGHT_MAGENTA,
    LIGHT_CYAN,
    WHITE
};

struct LogLevelHash {
    size_t operator()(const LogLevel &arg) const noexcept {
        return std::hash<int>{}(static_cast<int>(arg));
    }
};

struct LogColorHash {
    size_t operator()(const LogColor &arg) const noexcept {
        return std::hash<int>{}(static_cast<int>(arg));
    }
};

struct LogFormat {
    std::string level;
    std::string color;
    LogFormat(std::string _level, std::string _color) : level{_level}, color{_color} {}
};

struct ColorCode {
    std::string name;
    std::string value;
};

struct LogMessage {
    LogMessage(LogLevel _level, std::string _message) : level(_level), message(_message) {}
    LogLevel level;
    std::string message;
};

#ifdef NOLOGGING
    #define LOG_NOFORMAT(x)
    #define LOG_FATAL(x)
    #define LOG_ERROR(x)
    #define LOG_CONSOLE(x)
    #define LOG_WARNING(x)
    #define LOG_INFO(x)
    #define LOG_SUCCESS(x)
    #define LOG_DEBUG1(x)
    #define LOG_DEBUG2(x)
#else
    #define LOG_NOFORMAT(x) logging::Logger::getInstance() << logging::LogLevel::LOG_NOFORMAT << x
    #define LOG_FATAL(x) logging::Logger::getInstance() << logging::LogLevel::LOG_FATAL << x
    #define LOG_ERROR(x) logging::Logger::getInstance() << logging::LogLevel::LOG_ERROR << x
    #define LOG_CONSOLE(x) logging::Logger::getInstance() << logging::LogLevel::LOG_CONSOLE << x

    #ifndef NOINFOLOGGING
        #define LOG_WARNING(x) logging::Logger::getInstance() << logging::LogLevel::LOG_WARNING << x
        #define LOG_INFO(x) logging::Logger::getInstance() << logging::LogLevel::LOG_INFO << x
        #define LOG_SUCCESS(x) logging::Logger::getInstance() << logging::LogLevel::LOG_SUCCESS << x
    #else
        #define LOG_WARNING(x)
        #define LOG_INFO(x)
        #define LOG_SUCCESS(x)
    #endif

    #ifndef NODEBUGLOGGING
        #define LOG_DEBUG1(x) logging::Logger::getInstance() << logging::LogLevel::LOG_DEBUG1 << x
        #define LOG_DEBUG2(x) logging::Logger::getInstance() << logging::LogLevel::LOG_DEBUG2 << x
    #else
        #define LOG_DEBUG1(x)
        #define LOG_DEBUG2(x)
    #endif
#endif

/**
 * @ingroup common
 * @ingroup logger
 * @brief Thread-safe logger with optional color and file output.
 *
 * Log level is controlled at compile time via the CMake variable
 * DEFAULT_LOG_LEVEL (e.g. -DDEFAULT_LOG_LEVEL="LOG_DEBUG1").
 */
class Logger : public std::ostringstream {
   public:
    Logger();
    explicit Logger(LogLevel logLevel);
    Logger(LogLevel logLevel, std::string timeFormat);

    Logger &flush();

    Logger &operator<<(const LogLevel &level);
    Logger &operator<<(const LogColor &color);

    template <typename T>
    Logger &operator<<(const T &t) {
        (*reinterpret_cast<std::ostringstream *>(this)) << t;
        return *this;
    }

    Logger &operator<<(manip1 fp) {
        if (fp == static_cast<std::basic_ostream<char, std::char_traits<char>> &(*)(
                      std::basic_ostream<char, std::char_traits<char>> &)>(std::endl)) {
            flush();
            return *this;
        }
        (*reinterpret_cast<std::ostringstream *>(this)) << fp;
        return *this;
    }

    Logger &operator<<(manip2 fp) {
        (*reinterpret_cast<std::ostringstream *>(this)) << fp;
        return *this;
    }

    Logger &operator<<(manip3 fp) {
        (*reinterpret_cast<std::ostringstream *>(this)) << fp;
        return *this;
    }

    virtual ~Logger();

    static Logger &getInstance();

    /// Enable/disable color output at runtime
    static void setColorEnabled(bool enabled);
    /// Enable file logging at runtime
    static void setLogFile(const std::string &filename);
    /// Disable file logging
    static void disableFileLogging();
    /// Change log level at runtime
    static void setLogLevel(LogLevel level);

    static bool colorEnabled;
    static bool logToFile;
    static size_t counter;
    static std::string logFileName;
    static std::ofstream logfile;

    static std::string getColorCode(std::string color);
    static std::string getColorCode(LogColor color);

    static thread_local std::unordered_map<LogColor, ColorCode, LogColorHash> colorCodes_;
    static thread_local std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash> formatMap;

   protected:
    static LogLevel logLevel;
    static std::string timeFormat;
    LogLevel currentLevel = LogLevel::LOG_CONSOLE;
    static thread_local std::atomic<bool> initializedAtomic;

   public:
    static thread_local std::shared_ptr<Logger> instance;

   private:
    /// Parse the CMake-provided log level string
    static LogLevel parseLogLevel(const std::string &s);
    /// Build the default format map (called once per thread)
    static std::unordered_map<LogLevel, std::shared_ptr<LogFormat>, LogLevelHash> buildDefaultFormatMap();
};

}  // namespace logging

#endif /* LOGGER_HPP_ */


#ifdef VERBOSE_OUTPUT
#define VLOG(msg) std::cout << "[VERBOSE] " << msg << std::endl;
#else
#define VLOG(msg) ((void)0)
#endif