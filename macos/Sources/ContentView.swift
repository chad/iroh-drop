import AppKit
import Combine
import CoreImage.CIFilterBuiltins
import SwiftUI
import UniformTypeIdentifiers

struct ContentView: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        // Onboarding replaces the whole content on first run, rather than a
        // sheet over a window that might not be frontmost yet.
        if model.showOnboarding {
            OnboardingView()
                .environmentObject(model)
                .frame(minWidth: 480, idealWidth: 540, minHeight: 520)
        } else {
            main
        }
    }

    private var main: some View {
        VStack(spacing: 0) {
            ScrollView {
                // One centred column with a ceiling on its width. Text and
                // controls stretched across a wide window is the single thing
                // that makes a Mac app look unfinished.
                VStack(alignment: .leading, spacing: 18) {
                    ForEach(model.incoming) { IncomingCard(item: $0) }

                    if let error = model.errorMessage {
                        HStack(spacing: 8) {
                            Label(error, systemImage: "exclamationmark.triangle.fill")
                                .font(.callout)
                                .foregroundStyle(.orange)
                                .textSelection(.enabled)
                            Spacer(minLength: 4)
                            Button {
                                model.errorMessage = nil
                            } label: {
                                Image(systemName: "xmark.circle.fill")
                                    .foregroundStyle(.tertiary)
                            }
                            .buttonStyle(.plain)
                            .help("Dismiss")
                        }
                    }

                    DropZone()
                    ReceiveRow()

                    if !model.available.isEmpty {
                        AvailableSection()
                    }
                    if !model.sharing.isEmpty {
                        SharingSection()
                    }
                    if !model.transfers.isEmpty {
                        ReceivedSection()
                    }
                    if model.sharing.isEmpty && model.transfers.isEmpty {
                        EmptyHint()
                    }
                }
                .frame(maxWidth: 540, alignment: .leading)
                .frame(maxWidth: .infinity)
                .padding(.horizontal, 24)
                .padding(.vertical, 22)
            }
            Divider()
            StatusBar()
        }
        .frame(minWidth: 480, idealWidth: 580, minHeight: 520, idealHeight: 620)
        .animation(.easeInOut(duration: 0.18), value: model.incoming.count)
        .animation(.easeInOut(duration: 0.18), value: model.transfers.count)
        .sheet(item: $model.shareSheet) { info in
            ShareSheet(info: info)
        }
    }
}

// MARK: - Section furniture

/// Uppercased, secondary section headers: quieter than a `.title3`, and the
/// convention everywhere else in macOS.
private struct SectionHeader: View {
    let title: String
    var count: Int?

    var body: some View {
        HStack(spacing: 6) {
            Text(title.uppercased())
                .font(.caption.weight(.semibold))
                .foregroundStyle(.secondary)
                .kerning(0.6)
            if let count {
                Text("\(count)")
                    .font(.caption2.weight(.semibold))
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 5)
                    .padding(.vertical, 1)
                    .background(Capsule().fill(.quaternary))
            }
        }
    }
}

/// A grouped list, the way System Settings draws them.
private struct Card<Content: View>: View {
    @ViewBuilder var content: Content

    var body: some View {
        VStack(spacing: 0) { content }
            .background(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .fill(Color(nsColor: .controlBackgroundColor))
            )
            .overlay(
                RoundedRectangle(cornerRadius: 10, style: .continuous)
                    .strokeBorder(.quaternary, lineWidth: 1)
            )
    }
}

// MARK: - Sending

private struct DropZone: View {
    @EnvironmentObject private var model: AppModel
    @State private var isTargeted = false

    var body: some View {
        ZStack {
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .fill(isTargeted ? Color.accentColor.opacity(0.14) : Color(nsColor: .controlBackgroundColor))
            RoundedRectangle(cornerRadius: 14, style: .continuous)
                .strokeBorder(
                    isTargeted ? Color.accentColor : Color.secondary.opacity(0.28),
                    style: StrokeStyle(lineWidth: isTargeted ? 2 : 1.2, dash: isTargeted ? [] : [6, 5])
                )

            VStack(spacing: 10) {
                if let busy = model.busy {
                    ProgressView().controlSize(.small)
                    Text(busy).font(.callout).foregroundStyle(.secondary)
                } else {
                    Image(systemName: isTargeted ? "arrow.down.circle.fill" : "paperplane")
                        .font(.system(size: 30, weight: .light))
                        .foregroundStyle(isTargeted ? AnyShapeStyle(Color.accentColor) : AnyShapeStyle(.secondary))
                    VStack(spacing: 3) {
                        Text(isTargeted ? "Drop to send" : "Drag files here to send")
                            .font(.callout.weight(.medium))
                        Text("or")
                            .font(.caption)
                            .foregroundStyle(.tertiary)
                    }
                    Button("Choose Files…") { model.chooseFiles() }
                        .controlSize(.regular)
                        .keyboardShortcut("o", modifiers: .command)
                }
            }
            .padding(.vertical, 30)
        }
        .frame(maxWidth: .infinity)
        .animation(.easeOut(duration: 0.12), value: isTargeted)
        .onDrop(of: [.fileURL], isTargeted: $isTargeted) { providers in
            load(providers: providers)
            return true
        }
    }

    private func load(providers: [NSItemProvider]) {
        let group = DispatchGroup()
        let lock = NSLock()
        var urls: [URL] = []
        for provider in providers {
            group.enter()
            _ = provider.loadObject(ofClass: URL.self) { url, _ in
                if let url { lock.lock(); urls.append(url); lock.unlock() }
                group.leave()
            }
        }
        group.notify(queue: .main) {
            model.send(urls: urls.sorted { $0.path < $1.path })
        }
    }
}

// MARK: - Receiving

private struct ReceiveRow: View {
    @EnvironmentObject private var model: AppModel
    @State private var pasted = ""

    private var ready: Bool { AppModel.ticket(in: pasted) != nil }

    var body: some View {
        HStack(spacing: 8) {
            Image(systemName: "link")
                .foregroundStyle(.secondary)
                .font(.callout)
            TextField("Paste a link someone sent you", text: $pasted)
                .textFieldStyle(.plain)
                .onSubmit { if ready { go() } }
            if !pasted.isEmpty {
                Button {
                    pasted = ""
                } label: {
                    Image(systemName: "xmark.circle.fill").foregroundStyle(.tertiary)
                }
                .buttonStyle(.plain)
            }
            Button("Get Files") { go() }
                .buttonStyle(.borderedProminent)
                .disabled(!ready)
        }
        .padding(.horizontal, 10)
        .padding(.vertical, 7)
        .background(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .fill(Color(nsColor: .controlBackgroundColor))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 9, style: .continuous)
                .strokeBorder(.quaternary, lineWidth: 1)
        )
    }

    private func go() {
        model.receive(text: pasted)
        pasted = ""
    }
}

private struct IncomingCard: View {
    @EnvironmentObject private var model: AppModel
    let item: Incoming
    @State private var now = Date()

    private var remaining: String {
        let seconds = max(0, Int(item.expiresAt.timeIntervalSince(now)))
        return String(format: "%d:%02d", seconds / 60, seconds % 60)
    }

    var body: some View {
        HStack(alignment: .center, spacing: 13) {
            Image(systemName: "tray.and.arrow.down.fill")
                .font(.system(size: 20))
                .foregroundStyle(.white)
                .frame(width: 36, height: 36)
                .background(Circle().fill(Color.accentColor))

            VStack(alignment: .leading, spacing: 2) {
                Text("Someone wants to send you a file")
                    .font(.caption)
                    .foregroundStyle(.secondary)
                // Quoted, so a filename can never imitate our own text.
                Text(verbatim: "“\(item.name)”")
                    .font(.body.weight(.medium))
                    .lineLimit(1)
                    .truncationMode(.middle)
                Text([item.size,
                      "from \(item.sender)",
                      model.groupName(for: item.drop).map { "in \($0)" } ?? "",
                      "expires in \(remaining)"]
                        .filter { !$0.isEmpty }
                        .joined(separator: " · "))
                    .font(.caption)
                    .foregroundStyle(.tertiary)
            }

            Spacer(minLength: 6)

            VStack(spacing: 6) {
                Button("Accept") { model.answer(item, accept: true) }
                    .buttonStyle(.borderedProminent)
                Button("Decline") { model.answer(item, accept: false) }
                    .controlSize(.small)
            }
        }
        .padding(14)
        .background(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .fill(Color.accentColor.opacity(0.12))
        )
        .overlay(
            RoundedRectangle(cornerRadius: 12, style: .continuous)
                .strokeBorder(Color.accentColor.opacity(0.4), lineWidth: 1)
        )
        .onReceive(Timer.publish(every: 1, on: .main, in: .common).autoconnect()) { now = $0 }
    }
}

// MARK: - Lists

/// Offered in your groups, not yet fetched. Membership is sticky, so these
/// rows are too: they stay, fetchable, until fetched or until you leave.
private struct AvailableSection: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 7) {
            SectionHeader(title: "New in your groups", count: model.available.count)
            Card {
                ForEach(Array(model.available.enumerated()), id: \.element.id) { index, offer in
                    if index > 0 { Divider().padding(.leading, 38) }
                    AvailableRowView(offer: offer)
                }
            }
        }
    }
}

private struct AvailableRowView: View {
    @EnvironmentObject private var model: AppModel
    let offer: AvailableOffer
    /// Tapped, waiting for the transfer to show up in Received. The row
    /// leaves when the offer's status flips; until then a second tap must
    /// not ask twice.
    @State private var getting = false

    var body: some View {
        HStack(spacing: 10) {
            Image(systemName: "tray.and.arrow.down.fill")
                .foregroundStyle(.secondary)
                .font(.title3)
            VStack(alignment: .leading, spacing: 1) {
                Text(offer.name).lineLimit(1).truncationMode(.middle)
                Text([offer.size, "in \(offer.groupName)"]
                    .filter { !$0.isEmpty }
                    .joined(separator: " · "))
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: 6)
            Button(getting ? "Getting…" : "Get") {
                getting = true
                model.fetch(offer)
            }
            .controlSize(.small)
            .disabled(getting)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 9)
        .contextMenu {
            Button("Get") { model.fetch(offer) }
        }
    }
}

private struct SharingSection: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 7) {
            SectionHeader(title: "Sharing", count: model.sharing.count)
            Card {
                ForEach(Array(model.sharing.enumerated()), id: \.element.id) { index, drop in
                    if index > 0 { Divider().padding(.leading, 38) }
                    HStack(spacing: 10) {
                        Image(systemName: "arrow.up.circle.fill")
                            .foregroundStyle(.secondary)
                            .font(.title3)
                        VStack(alignment: .leading, spacing: 1) {
                            Text(drop.name).lineLimit(1).truncationMode(.middle)
                            Text(subtitle(for: drop))
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        Spacer(minLength: 6)
                        Button("Link") { model.showLink(for: drop) }
                            .controlSize(.small)
                        Button(drop.mine ? "Stop" : "Leave") { model.stopSharing(drop) }
                            .controlSize(.small)
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 9)
                    .contextMenu {
                        Button("Copy Link") { model.copyLink(for: drop) }
                        Button("Show Link & QR…") { model.showLink(for: drop) }
                        Divider()
                        Button(drop.mine ? "Stop Sharing" : "Leave Group", role: .destructive) {
                            model.stopSharing(drop)
                        }
                    }
                }
            }
        }
    }

    private func subtitle(for drop: SharedDrop) -> String {
        var parts = ["\(drop.files) file\(drop.files == 1 ? "" : "s")"]
        if !drop.size.isEmpty { parts.append(drop.size) }
        parts.append(drop.peers > 0
            ? "\(drop.peers) connected"
            : "waiting for someone")
        return parts.joined(separator: " · ")
    }
}

private struct ReceivedSection: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        VStack(alignment: .leading, spacing: 7) {
            HStack {
                SectionHeader(title: "Received")
                Spacer()
                if model.transfers.contains(where: \.finished) {
                    Button("Clear") { model.clearFinished() }
                        .buttonStyle(.plain)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
            }
            Card {
                ForEach(Array(model.transfers.reversed().prefix(10).enumerated()),
                        id: \.element.id) { index, transfer in
                    if index > 0 { Divider().padding(.leading, 38) }
                    TransferRow(transfer: transfer)
                }
            }
        }
    }
}

private struct TransferRow: View {
    @EnvironmentObject private var model: AppModel
    let transfer: Transfer

    var body: some View {
        HStack(spacing: 10) {
            icon
            VStack(alignment: .leading, spacing: 2) {
                Text(verbatim: "“\(transfer.name)”")
                    .lineLimit(1)
                    .truncationMode(.middle)
                if let failure = transfer.failure {
                    Text(failure).font(.caption).foregroundStyle(.secondary).lineLimit(1)
                } else if !transfer.finished {
                    if let fraction = transfer.fraction {
                        HStack(spacing: 6) {
                            ProgressView(value: fraction).controlSize(.small)
                            if let label = transfer.progressLabel {
                                Text(label)
                                    .font(.caption2)
                                    .foregroundStyle(.tertiary)
                                    .monospacedDigit()
                            }
                        }
                    } else {
                        Text("receiving…").font(.caption).foregroundStyle(.secondary)
                    }
                } else if !transfer.size.isEmpty {
                    Text(transfer.size).font(.caption).foregroundStyle(.secondary)
                }
            }
            Spacer(minLength: 6)
            if transfer.failure != nil, !transfer.drop.isEmpty {
                Button("Try Again") { model.retry(transfer) }
                    .controlSize(.small)
            }
            if transfer.finished, transfer.failure == nil, let path = transfer.savedTo.first {
                Button("Show") { model.reveal(path: path) }
                    .controlSize(.small)
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 9)
        .contextMenu {
            if transfer.finished, transfer.failure == nil, let path = transfer.savedTo.first {
                Button("Show in Finder") { model.reveal(path: path) }
            }
            if transfer.failure != nil, !transfer.drop.isEmpty {
                Button("Try Again") { model.retry(transfer) }
            }
        }
    }

    @ViewBuilder private var icon: some View {
        if transfer.failure != nil {
            Image(systemName: "exclamationmark.circle.fill")
                .foregroundStyle(.orange).font(.title3)
        } else if transfer.finished {
            Image(systemName: "checkmark.circle.fill")
                .foregroundStyle(.green).font(.title3)
        } else {
            ProgressView().controlSize(.small).frame(width: 20)
        }
    }
}

private struct EmptyHint: View {
    var body: some View {
        VStack(spacing: 5) {
            Text("Nothing yet")
                .font(.callout.weight(.medium))
                .foregroundStyle(.secondary)
            Text("Files you send stay available as long as iroh-drop is installed and running.")
                .font(.caption)
                .foregroundStyle(.tertiary)
                .multilineTextAlignment(.center)
                .fixedSize(horizontal: false, vertical: true)
        }
        .frame(maxWidth: .infinity)
        .padding(.top, 10)
    }
}

// MARK: - The share sheet

private struct ShareSheet: View {
    @EnvironmentObject private var model: AppModel
    @Environment(\.dismiss) private var dismiss
    let info: ShareInfo
    @State private var copied = false

    var body: some View {
        VStack(spacing: 16) {
            VStack(spacing: 4) {
                Text("Ready to send").font(.headline)
                Text(info.name)
                    .font(.callout)
                    .foregroundStyle(.secondary)
                    .lineLimit(1)
                    .truncationMode(.middle)
            }

            QRCodeView(text: info.link)
                .frame(width: 168, height: 168)
                .padding(10)
                .background(
                    RoundedRectangle(cornerRadius: 12, style: .continuous).fill(.white)
                )

            Text("Send the link, or let them point a phone camera at the code.")
                .font(.caption)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            HStack(spacing: 8) {
                Button {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(info.link, forType: .string)
                    copied = true
                    DispatchQueue.main.asyncAfter(deadline: .now() + 1.6) { copied = false }
                } label: {
                    Label(copied ? "Copied" : "Copy Link",
                          systemImage: copied ? "checkmark" : "link")
                        .frame(maxWidth: .infinity)
                }
                .buttonStyle(.borderedProminent)
                .controlSize(.large)

                ShareLink(item: info.link) {
                    Label("Share…", systemImage: "square.and.arrow.up")
                }
                .controlSize(.large)
            }

            Text("Anyone with this link can get these files.")
                .font(.caption2)
                .foregroundStyle(.tertiary)

            Button("Done") { dismiss() }
                .keyboardShortcut(.defaultAction)
        }
        .padding(22)
        .frame(width: 320)
    }
}

/// A QR code drawn by CoreImage: no dependency, crisp at any size.
struct QRCodeView: View {
    let text: String

    var body: some View {
        if let image = Self.render(text) {
            Image(nsImage: image)
                .interpolation(.none)
                .resizable()
                .aspectRatio(contentMode: .fit)
        } else {
            RoundedRectangle(cornerRadius: 6).fill(.quaternary)
        }
    }

    private static func render(_ text: String) -> NSImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(text.utf8)
        filter.correctionLevel = "L"
        guard let output = filter.outputImage else { return nil }
        let scaled = output.transformed(by: CGAffineTransform(scaleX: 10, y: 10))
        let context = CIContext()
        guard let cgImage = context.createCGImage(scaled, from: scaled.extent) else { return nil }
        return NSImage(cgImage: cgImage,
                       size: NSSize(width: scaled.extent.width, height: scaled.extent.height))
    }
}

// MARK: - Status

private struct StatusBar: View {
    @EnvironmentObject private var model: AppModel

    var body: some View {
        HStack(spacing: 7) {
            Circle()
                .fill(model.connected ? Color.green : Color.orange)
                .frame(width: 7, height: 7)
            Text(model.status).font(.caption).foregroundStyle(.secondary)
            Spacer()
            if !model.downloadDirectory.isEmpty {
                Button("Downloads") { model.revealDownloads() }
                    .buttonStyle(.link)
                    .font(.caption)
            }
        }
        .padding(.horizontal, 14)
        .padding(.vertical, 7)
    }
}

// MARK: - Small helpers

private extension View {
    /// `sheet(item:)` over an `Optional` published property.
    func sheet<Item: Identifiable, Sheet: View>(
        item: Binding<Item?>,
        @ViewBuilder content: @escaping (Item) -> Sheet
    ) -> some View {
        sheet(isPresented: Binding(
            get: { item.wrappedValue != nil },
            set: { if !$0 { item.wrappedValue = nil } }
        )) {
            if let value = item.wrappedValue { content(value) }
        }
    }
}
