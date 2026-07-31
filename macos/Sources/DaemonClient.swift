import Foundation

/// A client of the iroh-drop control API: newline-delimited JSON over a Unix
/// domain socket.
///
/// No FFI. The daemon is already a separate process with a language-agnostic
/// protocol, so Swift talks to it the same way the Rust CLI does. That keeps the
/// UI safe from a crash in the networking core, lets the daemon outlive the
/// window (which is the whole point — files stay available), and means an API
/// change needs no regenerated bindings.
final class DaemonClient {
    enum ClientError: LocalizedError {
        case cannotConnect(String)
        case disconnected
        case remote(code: String, message: String)

        var errorDescription: String? {
            switch self {
            case .cannotConnect(let path): return "Could not reach the helper at \(path)."
            case .disconnected: return "The helper went away."
            case .remote(_, let message): return message
            }
        }
    }

    /// A question from the daemon that needs a human answer.
    struct Ask {
        let id: UInt64
        let question: String
        let payload: [String: Any]
    }

    private let queue = DispatchQueue(label: "computer.iroh.drop.client")
    private var fd: Int32 = -1
    private var nextID: UInt64 = 1
    private var pending: [UInt64: (Result<[String: Any], Error>) -> Void] = [:]
    private var inbox = Data()

    /// Called for every event the daemon broadcasts.
    var onEvent: ((String, [String: Any]) -> Void)?
    /// Called when the daemon asks a question.
    var onAsk: ((Ask) -> Void)?
    /// Called when the connection drops.
    var onDisconnect: (() -> Void)?

    /// Handshake result, e.g. the daemon's endpoint id and method list.
    private(set) var hello: [String: Any] = [:]

    // MARK: - Connecting

    /// Where the helper listens, matching the Rust side's default.
    static func defaultSocketPath() -> String {
        if let runtime = ProcessInfo.processInfo.environment["XDG_RUNTIME_DIR"],
           FileManager.default.fileExists(atPath: runtime) {
            return runtime + "/iroh-drop/control.sock"
        }
        let home = FileManager.default.homeDirectoryForCurrentUser.path
        return home + "/.local/share/iroh-drop/control.sock"
    }

    func connect(socketPath: String, roles: [String]) throws {
        // One socket per client. A second one would duplicate every event.
        guard fd < 0 else { return }

        let descriptor = socket(AF_UNIX, SOCK_STREAM, 0)
        guard descriptor >= 0 else { throw ClientError.cannotConnect(socketPath) }

        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)
        let capacity = MemoryLayout.size(ofValue: address.sun_path)
        let pathBytes = Array(socketPath.utf8CString)
        guard pathBytes.count <= capacity else {
            close(descriptor)
            throw ClientError.cannotConnect(socketPath)
        }
        withUnsafeMutableBytes(of: &address.sun_path) { raw in
            pathBytes.withUnsafeBytes { source in
                raw.copyMemory(from: source)
            }
        }

        let length = socklen_t(MemoryLayout<sockaddr_un>.size)
        let connected = withUnsafePointer(to: &address) { pointer -> Int32 in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) { sa in
                Darwin.connect(descriptor, sa, length)
            }
        }
        guard connected == 0 else {
            close(descriptor)
            throw ClientError.cannotConnect(socketPath)
        }
        fd = descriptor

        startReading()
        // A connection is not a client until it says hello.
        hello = try callSync("hello", [
            "client": "iroh-drop.app",
            "api": 1,
            "roles": roles,
        ])
    }

    func disconnect() {
        queue.sync {
            if fd >= 0 { close(fd) }
            fd = -1
        }
    }

    // MARK: - Reading

    private func startReading() {
        let descriptor = fd
        Thread {
            var buffer = [UInt8](repeating: 0, count: 64 * 1024)
            while true {
                let count = read(descriptor, &buffer, buffer.count)
                if count <= 0 { break }
                // Copy before dispatching. Handing the shared buffer to the
                // queue would let this loop overwrite bytes the parser has not
                // read yet, which corrupts frames only under load — the worst
                // kind of bug to go looking for later.
                let chunk = Data(buffer[0..<count])
                self.queue.async {
                    self.inbox.append(chunk)
                    self.drainLines()
                }
            }
            DispatchQueue.main.async { self.onDisconnect?() }
        }.start()
    }

    private func drainLines() {
        while let newline = inbox.firstIndex(of: 0x0A) {
            let line = inbox[inbox.startIndex..<newline]
            inbox.removeSubrange(inbox.startIndex...newline)
            guard !line.isEmpty,
                  let object = try? JSONSerialization.jsonObject(with: line),
                  let frame = object as? [String: Any],
                  let kind = frame["t"] as? String
            else { continue }
            handle(kind: kind, frame: frame)
        }
    }

    private func handle(kind: String, frame: [String: Any]) {
        switch kind {
        case "res":
            guard let id = frame["id"] as? UInt64 ?? (frame["id"] as? NSNumber)?.uint64Value,
                  let reply = pending.removeValue(forKey: id) else { return }
            reply(.success(frame["p"] as? [String: Any] ?? [:]))
        case "err":
            guard let id = frame["id"] as? UInt64 ?? (frame["id"] as? NSNumber)?.uint64Value,
                  let reply = pending.removeValue(forKey: id) else { return }
            reply(.failure(ClientError.remote(
                code: frame["code"] as? String ?? "error",
                message: frame["msg"] as? String ?? "Something went wrong."
            )))
        case "ev":
            guard let name = frame["e"] as? String else { return }
            let payload = frame["p"] as? [String: Any] ?? [:]
            DispatchQueue.main.async { self.onEvent?(name, payload) }
        case "ask":
            guard let id = (frame["id"] as? NSNumber)?.uint64Value,
                  let question = frame["q"] as? String else { return }
            let payload = frame["p"] as? [String: Any] ?? [:]
            let ask = Ask(id: id, question: question, payload: payload)
            DispatchQueue.main.async { self.onAsk?(ask) }
        default:
            break
        }
    }

    // MARK: - Writing

    private func send(_ frame: [String: Any]) throws {
        guard var data = try? JSONSerialization.data(withJSONObject: frame) else {
            throw ClientError.disconnected
        }
        data.append(0x0A)
        let descriptor = fd
        guard descriptor >= 0 else { throw ClientError.disconnected }
        try data.withUnsafeBytes { raw in
            var offset = 0
            while offset < raw.count {
                let written = write(descriptor, raw.baseAddress!.advanced(by: offset), raw.count - offset)
                if written <= 0 { throw ClientError.disconnected }
                offset += written
            }
        }
    }

    /// Invoke a method, calling back on the main queue.
    func call(_ method: String,
              _ params: [String: Any] = [:],
              completion: ((Result<[String: Any], Error>) -> Void)? = nil) {
        queue.async {
            let id = self.nextID
            self.nextID += 1
            if let completion {
                self.pending[id] = { result in
                    DispatchQueue.main.async { completion(result) }
                }
            }
            do {
                try self.send(["t": "req", "id": id, "m": method, "p": params])
            } catch {
                self.pending.removeValue(forKey: id)
                if let completion {
                    DispatchQueue.main.async { completion(.failure(error)) }
                }
            }
        }
    }

    /// Answer a question. `accept: false` declines; so does never answering.
    func answer(id: UInt64, accept: Bool) {
        queue.async {
            let frame: [String: Any] = accept
                ? ["t": "res", "id": id, "p": ["accept": true]]
                : ["t": "err", "id": id, "code": "declined", "msg": "declined"]
            try? self.send(frame)
        }
    }

    /// Blocking call, for the handshake only.
    private func callSync(_ method: String, _ params: [String: Any]) throws -> [String: Any] {
        let semaphore = DispatchSemaphore(value: 0)
        var outcome: Result<[String: Any], Error> = .failure(ClientError.disconnected)
        queue.async {
            let id = self.nextID
            self.nextID += 1
            self.pending[id] = { result in
                outcome = result
                semaphore.signal()
            }
            do {
                try self.send(["t": "req", "id": id, "m": method, "p": params])
            } catch {
                self.pending.removeValue(forKey: id)
                outcome = .failure(error)
                semaphore.signal()
            }
        }
        guard semaphore.wait(timeout: .now() + 10) == .success else {
            throw ClientError.disconnected
        }
        return try outcome.get()
    }
}
