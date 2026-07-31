import AppKit
import Foundation
import ServiceManagement
import SwiftUI
import UserNotifications

/// One thing arriving or leaving.
struct Transfer: Identifiable {
    let id = UUID()
    /// Content hash: the only stable identity a transfer has. Matching on the
    /// display name would merge two different files that share one, and split
    /// one file that gets renamed.
    var hash: String
    var name: String
    /// Carried from the offer, because a small file can finish before any
    /// progress event arrives and the row would otherwise have nothing to say.
    var size: String = ""
    var done: UInt64 = 0
    var total: UInt64?
    var finished = false
    var failure: String?
    var savedTo: [String] = []

    var fraction: Double? {
        guard let total, total > 0 else { return nil }
        return min(Double(done) / Double(total), 1)
    }
}

/// A consent question waiting on a person.
struct Incoming: Identifiable {
    let id: UInt64
    let name: String
    let size: String
    let sender: String
    let expiresAt: Date
}

/// Something we are sharing.
struct SharedDrop: Identifiable {
    var id: String
    var name: String
    /// Files a person would count, not offers announced: a folder is one offer.
    var files: Int
    var size: String
    var peers: Int
    /// True when we created the drop. A drop we *joined* is also being served —
    /// that is the whole design — but presenting it as something the user is
    /// "sharing" conflates two different intentions.
    var mine: Bool
}

/// A link ready to hand over.
struct ShareInfo: Identifiable {
    let id = UUID()
    let name: String
    let link: String
}

/// The whole app's state. Views read this and nothing else.
@MainActor
final class AppModel: ObservableObject {
    @Published var connected = false
    @Published var lanOnly = false
    @Published var downloadDirectory = ""
    @Published var status = "Starting…"
    @Published var errorMessage: String?
    @Published var busy: String?

    /// Presented as a sheet, so producing a link never reflows the window.
    @Published var shareSheet: ShareInfo?

    /// Shown once, the very first time, to explain the two things you can do.
    /// Persisted, so it never nags.
    @Published var showOnboarding: Bool = !UserDefaults.standard.bool(forKey: "hasSeenOnboarding")

    func dismissOnboarding() {
        showOnboarding = false
        UserDefaults.standard.set(true, forKey: "hasSeenOnboarding")
    }

    /// Whether macOS opens the app at login. This is the system's own answer,
    /// read back so the toggle cannot drift from reality.
    @Published private(set) var launchesAtLogin = false

    func setLaunchAtLogin(_ on: Bool) {
        do {
            if on {
                try SMAppService.mainApp.register()
            } else {
                try SMAppService.mainApp.unregister()
            }
        } catch {
            errorMessage = on
                ? "Could not enable opening at login."
                : "Could not disable opening at login."
        }
        launchesAtLogin = SMAppService.mainApp.status == .enabled
    }
    @Published var incoming: [Incoming] = []
    @Published var transfers: [Transfer] = []
    @Published var shared: [SharedDrop] = []

    private let client = DaemonClient()
    private var helper: Process?
    /// Drops the user asked for, and when. Asking is consent; see `receive`.
    private var requested: [String: Date] = [:]
    private var namesByHash: [String: String] = [:]
    private var sizesByHash: [String: String] = [:]
    private var expiryTimer: Timer?
    private var started = false
    private static let receiveGrace: TimeInterval = 60

    // MARK: - Lifecycle

    func start() {
        // SwiftUI may re-run `.task` when the view is recreated (reopening the
        // window, or the menu bar scene). Connecting twice would open a second
        // socket and a second reader thread, so every event would arrive twice —
        // visible as duplicated rows for a single transfer.
        guard !started else { return }
        started = true

        let path = DaemonClient.defaultSocketPath()
        client.onEvent = { [weak self] name, payload in
            self?.apply(event: name, payload: payload)
        }
        client.onAsk = { [weak self] ask in
            self?.received(ask: ask)
        }
        client.onDisconnect = { [weak self] in
            self?.connected = false
            self?.status = "The helper stopped."
        }

        do {
            try client.connect(socketPath: path, roles: ["ui", "control"])
            finishConnecting()
        } catch {
            // No helper yet: start the one inside this bundle, then retry.
            status = "Starting the helper…"
            startHelper()
            retryConnect(path: path, attemptsLeft: 40)
        }

        expiryTimer = Timer.scheduledTimer(withTimeInterval: 1, repeats: true) { [weak self] _ in
            Task { @MainActor in self?.pruneExpired() }
        }

        UNUserNotificationCenter.current().requestAuthorization(options: [.alert, .sound]) { _, _ in }
    }

    private func retryConnect(path: String, attemptsLeft: Int) {
        guard attemptsLeft > 0 else {
            errorMessage = "The background helper did not start."
            status = "Not running"
            return
        }
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.25) { [weak self] in
            guard let self else { return }
            do {
                try self.client.connect(socketPath: path, roles: ["ui", "control"])
                self.finishConnecting()
            } catch {
                self.retryConnect(path: path, attemptsLeft: attemptsLeft - 1)
            }
        }
    }

    private func finishConnecting() {
        connected = true
        errorMessage = nil
        launchesAtLogin = SMAppService.mainApp.status == .enabled
        refresh()
    }

    /// Launch `iroh-dropd` from inside this bundle, and only from there:
    /// searching `PATH` would let anything with that name inherit the user's
    /// files. It is deliberately not a child we wait on, because the helper's
    /// job is to outlive this window.
    private func startHelper() {
        guard let executable = Bundle.main.url(forAuxiliaryExecutable: "iroh-dropd")
                ?? Bundle.main.executableURL?.deletingLastPathComponent()
                    .appendingPathComponent("iroh-dropd"),
              FileManager.default.isExecutableFile(atPath: executable.path)
        else {
            errorMessage = "The background helper is missing from the app."
            return
        }
        let process = Process()
        process.executableURL = executable
        // The whole point of the helper is that files stay reachable after the
        // window closes — so it must answer consent while no UI is attached.
        process.arguments = ["--accept-when-no-ui"]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        process.standardInput = FileHandle.nullDevice
        do {
            try process.run()
            helper = process
        } catch {
            errorMessage = "Could not start the background helper."
        }
    }

    // MARK: - Actions

    func refresh() {
        client.call("daemon.status") { [weak self] result in
            guard let self, case .success(let status) = result else { return }
            self.lanOnly = status["offline"] as? Bool ?? false
            self.downloadDirectory = status["download_dir"] as? String ?? ""
            self.status = self.lanOnly ? "Ready · this network only" : "Ready"
        }
        client.call("drop.list") { [weak self] result in
            guard let self, case .success(let listed) = result else { return }
            let rows = listed["drops"] as? [[String: Any]] ?? []
            self.shared = rows.map { row in
                let handle = row["drop"] as? String ?? "?"
                let name = row["name"] as? String
                return SharedDrop(
                    id: handle,
                    // A joined drop has no local name; saying "Received" is
                    // truer than showing a dash next to things you are sending.
                    name: name ?? "Received files",
                    files: (row["files"] as? NSNumber)?.intValue ?? 0,
                    size: row["human_size"] as? String ?? "",
                    peers: (row["peers"] as? NSNumber)?.intValue ?? 0,
                    mine: name != nil
                )
            }
        }
    }

    func send(urls: [URL]) {
        guard !urls.isEmpty else { return }
        busy = "Preparing…"
        errorMessage = nil
        let label = urls.count == 1
            ? urls[0].lastPathComponent
            : "\(urls[0].lastPathComponent) +\(urls.count - 1) more"

        client.call("drop.create", ["name": label]) { [weak self] result in
            guard let self else { return }
            switch result {
            case .failure(let error):
                self.busy = nil
                self.errorMessage = error.localizedDescription
            case .success(let created):
                guard let handle = created["drop"] as? String else { return }
                self.publish(urls: urls, index: 0, handle: handle, label: label)
            }
        }
    }

    private func publish(urls: [URL], index: Int, handle: String, label: String) {
        guard index < urls.count else {
            client.call("drop.ticket", ["drop": handle]) { [weak self] result in
                guard let self else { return }
                self.busy = nil
                switch result {
                case .failure(let error):
                    self.errorMessage = error.localizedDescription
                case .success(let ticket):
                    // A link. The word "ticket" never reaches the screen.
                    if let link = ticket["link"] as? String {
                        self.shareSheet = ShareInfo(name: label, link: link)
                    }
                    self.refresh()
                }
            }
            return
        }
        busy = urls.count > 1 ? "Preparing \(index + 1) of \(urls.count)…" : "Preparing…"
        client.call("offer.publish", ["drop": handle, "path": urls[index].path]) { [weak self] result in
            guard let self else { return }
            if case .failure(let error) = result {
                self.busy = nil
                self.errorMessage = error.localizedDescription
                return
            }
            self.publish(urls: urls, index: index + 1, handle: handle, label: label)
        }
    }

    /// Join a drop from a pasted link, and let the consent path do the fetching.
    ///
    /// Deliberately no explicit fetch: every offer already produces a question,
    /// and answering it is what starts the transfer. Fetching here as well would
    /// download twice and ignore anyone who said no.
    func receive(text: String) {
        guard let ticket = Self.ticket(in: text) else {
            errorMessage = "That does not look like an iroh-drop link."
            return
        }
        busy = "Connecting…"
        errorMessage = nil
        client.call("drop.join", ["ticket": ticket]) { [weak self] result in
            guard let self else { return }
            self.busy = nil
            switch result {
            case .failure(let error):
                self.errorMessage = error.localizedDescription
            case .success(let joined):
                if let handle = joined["drop"] as? String {
                    self.requested[handle] = Date()
                }
                self.refresh()
            }
        }
    }

    func answer(_ incoming: Incoming, accept: Bool) {
        client.answer(id: incoming.id, accept: accept)
        self.incoming.removeAll { $0.id == incoming.id }
    }

    /// Put a fresh link on the pasteboard — the menu bar has no room for a sheet.
    func copyLink(for drop: SharedDrop) {
        client.call("drop.ticket", ["drop": drop.id]) { result in
            guard case .success(let ticket) = result,
                  let link = ticket["link"] as? String else { return }
            NSPasteboard.general.clearContents()
            NSPasteboard.general.setString(link, forType: .string)
        }
    }

    /// Fetch a fresh link for a drop we are already hosting.
    func showLink(for drop: SharedDrop) {
        client.call("drop.ticket", ["drop": drop.id]) { [weak self] result in
            guard let self, case .success(let ticket) = result,
                  let link = ticket["link"] as? String else { return }
            self.shareSheet = ShareInfo(name: drop.name, link: link)
        }
    }

    /// Drops we created, which are the ones a person thinks of as "sharing".
    var sharing: [SharedDrop] { shared.filter(\.mine) }

    /// Active transfers, for the menu bar glance.
    var activeTransfers: [Transfer] { transfers.filter { !$0.finished } }

    /// A short line for the menu bar: what's happening right now.
    var menuSummary: String {
        if let busy { return busy }
        let active = activeTransfers.count
        if active > 0 { return "Receiving \(active) file\(active == 1 ? "" : "s")…" }
        if !sharing.isEmpty {
            return "Sharing \(sharing.count) drop\(sharing.count == 1 ? "" : "s")"
        }
        return connected ? "Ready" : "Not running"
    }

    func stopSharing(_ drop: SharedDrop) {
        client.call("drop.leave", ["drop": drop.id]) { [weak self] _ in self?.refresh() }
    }

    func revealDownloads() {
        guard !downloadDirectory.isEmpty else { return }
        NSWorkspace.shared.open(URL(fileURLWithPath: downloadDirectory))
    }

    func reveal(path: String) {
        NSWorkspace.shared.selectFile(path, inFileViewerRootedAtPath: "")
    }

    // MARK: - Incoming

    private func received(ask: DaemonClient.Ask) {
        let handle = ask.payload["drop"] as? String ?? ""
        if let asked = requested[handle], Date().timeIntervalSince(asked) < Self.receiveGrace {
            // They already said yes by pasting the link.
            client.answer(id: ask.id, accept: true)
            return
        }

        let ttl = (ask.payload["expires_in_ms"] as? NSNumber)?.doubleValue ?? 60_000
        let name = ask.payload["name"] as? String ?? "a file"
        let item = Incoming(
            id: ask.id,
            name: name,
            size: ask.payload["human_size"] as? String ?? "",
            sender: String((ask.payload["from"] as? String ?? "").prefix(10)),
            // Trust the daemon's deadline, less a slice for the round trip.
            expiresAt: Date().addingTimeInterval(ttl / 1000 - 2)
        )
        incoming.append(item)
        notify(name: name, size: item.size)
    }

    private func notify(name: String, size: String) {
        let content = UNMutableNotificationContent()
        content.title = "Someone wants to send you a file"
        content.body = size.isEmpty ? name : "\(name) · \(size)"
        content.sound = .default
        let request = UNNotificationRequest(
            identifier: UUID().uuidString, content: content, trigger: nil)
        UNUserNotificationCenter.current().add(request)
    }

    private func pruneExpired() {
        let now = Date()
        let before = incoming.count
        incoming.removeAll { $0.expiresAt <= now }
        if incoming.count != before {
            status = "An offer expired before anyone answered."
        }
    }

    // MARK: - Events

    private func apply(event: String, payload: [String: Any]) {
        let hash = payload["hash"] as? String ?? ""
        switch event {
        case "offer.received":
            namesByHash[hash] = payload["name"] as? String ?? "file"
            sizesByHash[hash] = payload["human_size"] as? String ?? ""
        case "fetch.progress":
            let index = slot(for: hash)
            transfers[index].done = (payload["downloaded"] as? NSNumber)?.uint64Value ?? 0
            transfers[index].total = (payload["total"] as? NSNumber)?.uint64Value
        case "fetch.materialized":
            let index = slot(for: hash)
            transfers[index].finished = true
            transfers[index].savedTo = (payload["paths"] as? [String]) ?? []
            refresh()
        case "fetch.failed":
            let index = slot(for: hash)
            transfers[index].finished = true
            transfers[index].failure = payload["error"] as? String ?? "it did not arrive"
        case "peer.joined", "drop.joined", "drop.left":
            refresh()
        default:
            break
        }
    }

    /// The row for a blob, created if this is the first we have heard of it.
    private func slot(for hash: String) -> Int {
        if let index = transfers.firstIndex(where: { $0.hash == hash }) { return index }
        transfers.append(Transfer(hash: hash,
                                  name: namesByHash[hash] ?? "file",
                                  size: sizesByHash[hash] ?? ""))
        return transfers.count - 1
    }

    // MARK: - Links

    /// Pull a ticket out of a link, or out of whatever a chat app pasted.
    static func ticket(in text: String) -> String? {
        guard let start = text.range(of: "drop1") else { return nil }
        let tail = text[start.lowerBound...]
        let allowed = tail.prefix { $0.isLowercase && $0.isASCII || $0.isNumber && $0.isASCII }
        return allowed.count > 32 ? String(allowed) : nil
    }
}
