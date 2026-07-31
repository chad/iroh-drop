import AppKit
import SwiftUI

/// Keeps the app alive when the last window closes, because the menu bar item
/// and the background helper are the product — the window is just a way in.
/// Closing the window must not stop sharing.
final class AppDelegate: NSObject, NSApplicationDelegate {
    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }

    /// Bring the app forward on launch and surface the window. A `WindowGroup`
    /// sitting next to a `MenuBarExtra` creates its window but does not order it
    /// front, so on first launch the app can look like it never opened. We find
    /// the window it made and show it.
    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApplication.shared.activate(ignoringOtherApps: true)
        DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
            NSApplication.shared.windows
                .first(where: { $0.canBecomeKey })
                .map { $0.makeKeyAndOrderFront(nil) }
        }
    }

    /// Clicking the Dock icon with no window open shows one again.
    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        if !flag {
            NSApplication.shared.windows
                .first(where: { $0.canBecomeKey })
                .map { $0.makeKeyAndOrderFront(nil) }
        }
        return true
    }
}

@main
struct IrohDropApp: App {
    @NSApplicationDelegateAdaptor(AppDelegate.self) private var appDelegate
    @StateObject private var model = AppModel()

    /// What the menu bar icon should say right now: a question beats
    /// movement, movement beats rest.
    private var menuIcon: String {
        if !model.incoming.isEmpty { return "tray.and.arrow.down.fill" }
        if !model.activeTransfers.isEmpty { return "arrow.down.circle.fill" }
        return "arrow.up.arrow.down.circle"
    }

    var body: some Scene {
        WindowGroup("Drop", id: "main") {
            ContentView()
                .environmentObject(model)
                .task { model.start() }
                // Clicking an iroh-drop:// link anywhere on the system lands
                // here. This is the reason the link is a link at all, and the
                // reason it is worth registering a scheme rather than showing
                // people a base32 blob and hoping.
                .onOpenURL { url in model.receive(text: url.absoluteString) }
        }
        .defaultSize(width: 560, height: 640)
        .windowResizability(.contentMinSize)
        .commands {
            CommandGroup(replacing: .newItem) {}
            CommandGroup(after: .newItem) {
                Button("Send Files…") { model.chooseFiles() }
                    .keyboardShortcut("o", modifiers: .command)
            }
        }

        // A menu bar item that is genuinely useful: a live summary, consent,
        // and the link you just made — without needing the window open.
        // The icon answers "is anything happening?" at a glance.
        MenuBarExtra("Drop", systemImage: menuIcon) {
            MenuBarContent().environmentObject(model)
        }
        .menuBarExtraStyle(.window)
    }
}

private struct MenuBarContent: View {
    @EnvironmentObject private var model: AppModel
    @Environment(\.openWindow) private var openWindow

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            header
            Divider().padding(.vertical, 8)

            if !model.incoming.isEmpty {
                consentSection
                Divider().padding(.vertical, 8)
            }

            if !model.available.isEmpty {
                availableSection
                Divider().padding(.vertical, 8)
            }

            if !model.sharing.isEmpty {
                sharingSection
                Divider().padding(.vertical, 8)
            }

            footer
        }
        .padding(12)
        .frame(width: 300)
    }

    private var header: some View {
        HStack(spacing: 8) {
            Circle()
                .fill(model.connected ? Color.green : Color.orange)
                .frame(width: 8, height: 8)
            Text(model.menuSummary)
                .font(.headline)
            Spacer()
            Text(model.connected ? "on" : "off")
                .font(.caption)
                .foregroundStyle(.secondary)
        }
    }

    private var consentSection: some View {
        VStack(alignment: .leading, spacing: 8) {
            Text("Incoming")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            ForEach(model.incoming) { item in
                HStack {
                    VStack(alignment: .leading, spacing: 1) {
                        Text(verbatim: "“\(item.name)”")
                            .lineLimit(1).truncationMode(.middle)
                        Text(item.size)
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("Accept") { model.answer(item, accept: true) }
                        .buttonStyle(.borderedProminent).controlSize(.small)
                    Button("Decline") { model.answer(item, accept: false) }
                        .controlSize(.small)
                }
            }
        }
    }

    /// Groups you belong to have news even when the window never opens:
    /// the menu bar is where most of the app's life happens.
    private var availableSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("New in your groups")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            ForEach(model.available.prefix(5)) { offer in
                HStack {
                    VStack(alignment: .leading, spacing: 1) {
                        Text(offer.name).lineLimit(1).truncationMode(.middle)
                        Text([offer.size, offer.groupName]
                            .filter { !$0.isEmpty }
                            .joined(separator: " · "))
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    Spacer()
                    Button("Get") { model.fetch(offer) }
                        .controlSize(.small)
                }
            }
            if model.available.count > 5 {
                Text("…and \(model.available.count - 5) more in the window")
                    .font(.caption).foregroundStyle(.tertiary)
            }
        }
    }

    private var sharingSection: some View {
        VStack(alignment: .leading, spacing: 6) {
            Text("Sharing")
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
            ForEach(model.sharing) { drop in
                HStack {
                    Text(drop.name).lineLimit(1).truncationMode(.middle)
                    Spacer()
                    Button("Copy Link") { model.copyLink(for: drop) }
                        .controlSize(.small)
                }
            }
        }
    }

    private var footer: some View {
        VStack(alignment: .leading, spacing: 8) {
            Toggle("Open at login", isOn: Binding(
                get: { model.launchesAtLogin },
                set: { model.setLaunchAtLogin($0) }
            ))
            .toggleStyle(.checkbox)
            .font(.callout)

            HStack {
                Button("Open iroh-drop") { showMainWindow() }
                    .controlSize(.small)
                Spacer()
                Button("Downloads") { model.revealDownloads() }
                    .controlSize(.small)
                Button("Quit") { NSApplication.shared.terminate(nil) }
                    .controlSize(.small)
            }
        }
    }

    /// Bring the existing window forward rather than spawning a second one.
    private func showMainWindow() {
        NSApplication.shared.activate(ignoringOtherApps: true)
        if let window = NSApplication.shared.windows.first(where: { !$0.title.isEmpty }) {
            window.makeKeyAndOrderFront(nil)
        } else {
            openWindow(id: "main")
        }
    }
}

// MARK: - First launch

/// Shown exactly once. Explains the two verbs — send and receive — and the one
/// fact that matters: files stay available while the app is around.
struct OnboardingView: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        VStack(spacing: 18) {
            Image(systemName: "arrow.up.arrow.down.circle.fill")
                .font(.system(size: 44))
                .foregroundStyle(Color.accentColor)
                .padding(.top, 6)

            VStack(spacing: 6) {
                Text("Welcome to Drop").font(.title2.weight(.semibold))
                Text("Send files to anyone, without accounts, clouds, or sign-ups.")
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .multilineTextAlignment(.center)
                    .fixedSize(horizontal: false, vertical: true)
            }

            VStack(alignment: .leading, spacing: 12) {
                FeatureRow(
                    icon: "paperplane.fill",
                    title: "To send",
                    detail: "Drag files in, get a link, hand it over. Anyone with the link can get the files."
                )
                FeatureRow(
                    icon: "tray.and.arrow.down.fill",
                    title: "To receive",
                    detail: "Click a link someone sends you, or paste it into the box."
                )
                FeatureRow(
                    icon: "lock.fill",
                    title: "Private by default",
                    detail: "Files go straight from your Mac to theirs. Nothing passes through a server you don't control."
                )
            }
            .padding(.horizontal, 4)

            Button("Get Started") { model.dismissOnboarding() }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)
                .keyboardShortcut(.defaultAction)
        }
        .padding(28)
        .frame(width: 380)
    }
}

struct FeatureRow: View {
    let icon: String
    let title: String
    let detail: String

    var body: some View {
        HStack(alignment: .top, spacing: 12) {
            Image(systemName: icon)
                .font(.system(size: 15))
                .foregroundStyle(Color.accentColor)
                .frame(width: 24, alignment: .center)
                .padding(.top, 1)
            VStack(alignment: .leading, spacing: 2) {
                Text(title).font(.callout.weight(.semibold))
                Text(detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                    .fixedSize(horizontal: false, vertical: true)
            }
        }
    }
}
