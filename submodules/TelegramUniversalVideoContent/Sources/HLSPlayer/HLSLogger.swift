import Foundation

enum LogLevel: Int {
    case verbose
    case debug
    case info
    case error
    case disabled
    
    fileprivate static let level = LogLevel.debug
}

struct HLSLogger {
    private let module: String
    
    init(module: String) {
        self.module = module
    }
    
    @inline(__always)
    func verbose(tag: String? = nil, _ text: @autoclosure () -> String) {
        log(level: .verbose, tag: tag, text())
    }
    @inline(__always)
    func debug(tag: String? = nil, _ text: @autoclosure () -> String) {
        log(level: .debug, tag: tag, text())
    }
    @inline(__always)
    func info(tag: String? = nil, _ text: @autoclosure () -> String) {
        log(level: .info, tag: tag, text())
    }
    @inline(__always)
    func error(tag: String? = nil, _ text: @autoclosure () -> String) {
        log(level: .error, tag: tag, text())
    }
    @inline(__always)
    func log(level: LogLevel, tag: String? = nil, _ text: @autoclosure () -> String) {
        #if DEBUG
        guard level.rawValue >= LogLevel.level.rawValue else { return }
        let prefix = tag.map { "[HLS.\(module).\($0)]" } ?? "[\(module)]"
        print("\(prefix) \(text())")
        #endif
    }
}
