//! iroh-drop, as a window.
//!
//! Deliberately small: choose files, hand over a link, accept what arrives.
//! There are no peer ids, hashes, tickets-as-jargon, or protocol words in the
//! UI — those live in the CLI. The only string a person sees is the link, and
//! the only decision they make is yes or no.

#![deny(missing_docs)]
// No console window for the shipped app; debug builds keep one for logs.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use iroh_drop_gui::{bridge, qr};

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use bridge::{Bridge, Cmd, UiState};
use eframe::egui;

fn main() -> eframe::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "iroh_drop_app=info,iroh_drop_daemon=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    let mut socket: Option<PathBuf> = None;
    let mut lan_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--socket" => socket = args.next().map(PathBuf::from),
            "--lan-only" => lan_only = true,
            _ => {}
        }
    }

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([560.0, 680.0])
            .with_min_inner_size([460.0, 480.0])
            .with_title("Drop — iroh-drop"),
        ..Default::default()
    };

    eframe::run_native(
        "iroh-drop",
        options,
        Box::new(move |cc| {
            let bridge = bridge::spawn(cc.egui_ctx.clone(), socket, lan_only);
            Ok(Box::new(App::new(bridge)))
        }),
    )
}

struct App {
    bridge: Bridge,
    link_input: String,
    qr_open: bool,
    /// When the copy button was last pressed, so its label can say so.
    copied_at: Option<Instant>,
    /// Gets already asked for ("drop/pick"), so the button cannot ask twice
    /// while the transfer is still spinning up.
    getting: HashSet<String>,
}

impl App {
    fn new(bridge: Bridge) -> Self {
        Self {
            bridge,
            link_input: String::new(),
            qr_open: false,
            copied_at: None,
            getting: HashSet::new(),
        }
    }

    fn recently_copied(&self) -> bool {
        self.copied_at
            .is_some_and(|at| at.elapsed() < Duration::from_secs(2))
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Clone what we render so the worker is never blocked by painting.
        let snapshot = {
            let state = self.bridge.state.lock().expect("state");
            Snapshot::from(&*state)
        };

        // Files dropped on the window are the fastest path to sending.
        let dropped: Vec<PathBuf> = ctx.input(|i| {
            i.raw
                .dropped_files
                .iter()
                .filter_map(|f| f.path.clone())
                .collect()
        });
        if !dropped.is_empty() {
            self.bridge.send(Cmd::Send(dropped));
        }
        let hovering = ctx.input(|i| !i.raw.hovered_files.is_empty());

        // A Get whose row has left the list (the transfer started) may go.
        self.getting.retain(|key| {
            snapshot
                .available
                .iter()
                .any(|row| format!("{}/{}", row.drop, row.pick) == *key)
        });

        // The copy label flips back after two seconds; make sure we repaint.
        if self.recently_copied() {
            ctx.request_repaint_after(Duration::from_millis(300));
        }

        egui::TopBottomPanel::top("status").show(ctx, |ui| {
            ui.horizontal(|ui| {
                let (dot, tip) = if snapshot.connected {
                    (egui::Color32::from_rgb(60, 170, 90), "Ready")
                } else {
                    (egui::Color32::from_rgb(200, 80, 80), "Not connected")
                };
                ui.colored_label(dot, "\u{25cf}");
                ui.label(tip);
                if snapshot.lan_only {
                    ui.separator();
                    ui.label("this network only");
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if !snapshot.download_dir.is_empty() && ui.button("Open folder").clicked() {
                        open_folder(&snapshot.download_dir);
                    }
                });
            });
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            egui::ScrollArea::vertical()
                .auto_shrink([false, true])
                .show(ui, |ui| {
                    if let Some(error) = &snapshot.error {
                        ui.horizontal(|ui| {
                            ui.colored_label(
                                egui::Color32::from_rgb(200, 120, 40),
                                format!("\u{26a0} {error}"),
                            );
                            if ui.small_button("\u{2715}").clicked() {
                                self.bridge.send(Cmd::DismissError);
                            }
                        });
                        ui.add_space(6.0);
                    }

                    // ── consent, first, because it is the only thing that blocks ──
                    for incoming in &snapshot.incoming {
                        egui::Frame::group(ui.style())
                            .fill(ui.visuals().extreme_bg_color)
                            .show(ui, |ui| {
                                ui.set_width(ui.available_width());
                                ui.label(egui::RichText::new("Someone wants to send you:").strong());
                                // Quoted: a filename must never be able to look
                                // like our own text.
                                ui.label(format!("{:?}   {}", incoming.name, incoming.size));
                                let remaining = incoming
                                    .expires_at
                                    .saturating_duration_since(Instant::now())
                                    .as_secs();
                                let group = snapshot
                                    .drops
                                    .iter()
                                    .find(|d| d.handle == incoming.drop)
                                    .map(|d| d.name.as_str())
                                    .unwrap_or("");
                                let context = if group.is_empty() {
                                    format!("from {}", incoming.from)
                                } else {
                                    format!("from {}  \u{2022}  in {group}", incoming.from)
                                };
                                ui.label(
                                    egui::RichText::new(format!(
                                        "{context}  \u{2022}  expires in {}:{:02}",
                                        remaining / 60,
                                        remaining % 60
                                    ))
                                    .small()
                                    .weak(),
                                );
                                ui.add_space(4.0);
                                ui.horizontal(|ui| {
                                    if ui.button("Accept").clicked() {
                                        self.bridge.send(Cmd::Answer {
                                            id: incoming.id,
                                            accept: true,
                                        });
                                    }
                                    if ui.button("No thanks").clicked() {
                                        self.bridge.send(Cmd::Answer {
                                            id: incoming.id,
                                            accept: false,
                                        });
                                    }
                                });
                            });
                        ui.add_space(8.0);
                    }

                    // ── send ──────────────────────────────────────────────
                    let send_fill = if hovering {
                        ui.visuals().selection.bg_fill.gamma_multiply(0.25)
                    } else {
                        ui.visuals().faint_bg_color
                    };
                    egui::Frame::group(ui.style())
                        .fill(send_fill)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            ui.label(egui::RichText::new("Send").strong());
                            ui.label(
                                egui::RichText::new(if hovering {
                                    "Release to send"
                                } else {
                                    "Drag files onto this window, or:"
                                })
                                .small()
                                .weak(),
                            );
                            ui.horizontal(|ui| {
                                if ui.button("Choose files…").clicked() {
                                    if let Some(paths) = rfd::FileDialog::new().pick_files() {
                                        self.bridge.send(Cmd::Send(paths));
                                    }
                                }
                                if ui.button("Choose a folder…").clicked() {
                                    if let Some(path) = rfd::FileDialog::new().pick_folder() {
                                        self.bridge.send(Cmd::Send(vec![path]));
                                    }
                                }
                            });

                            if let Some(busy) = &snapshot.busy {
                                ui.add_space(4.0);
                                ui.horizontal(|ui| {
                                    ui.spinner();
                                    ui.label(busy);
                                });
                            }

                            if let Some(link) = &snapshot.share_link {
                                ui.add_space(6.0);
                                let heading = match &snapshot.share_link_name {
                                    Some(name) => format!("Ready to send \u{2014} {name:?}"),
                                    None => "Ready to send".to_string(),
                                };
                                ui.label(egui::RichText::new(heading).strong());
                                ui.label(
                                    egui::RichText::new(
                                        "Hand over this link; anyone with it can get the files.",
                                    )
                                    .small()
                                    .weak(),
                                );
                                let mut shown = link.clone();
                                ui.add(
                                    egui::TextEdit::multiline(&mut shown)
                                        .desired_rows(2)
                                        .desired_width(f32::INFINITY)
                                        .font(egui::TextStyle::Monospace),
                                );
                                ui.horizontal(|ui| {
                                    if ui
                                        .button(if self.recently_copied() {
                                            "Copied \u{2713}"
                                        } else {
                                            "Copy link"
                                        })
                                        .clicked()
                                    {
                                        ctx.copy_text(link.clone());
                                        self.copied_at = Some(Instant::now());
                                    }
                                    if ui
                                        .button(if self.qr_open { "Hide code" } else { "Show code" })
                                        .clicked()
                                    {
                                        self.qr_open = !self.qr_open;
                                    }
                                });
                                if self.qr_open {
                                    ui.add_space(4.0);
                                    qr::show(ui, link);
                                    ui.label(
                                        egui::RichText::new("Point a phone camera at this.")
                                            .small()
                                            .weak(),
                                    );
                                }
                            }
                        });

                    ui.add_space(10.0);

                    // ── receive ───────────────────────────────────────────
                    egui::Frame::group(ui.style())
                        .fill(ui.visuals().faint_bg_color)
                        .show(ui, |ui| {
                            ui.set_width(ui.available_width());
                            ui.label(egui::RichText::new("Receive").strong());
                            ui.horizontal(|ui| {
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.link_input)
                                        .hint_text("paste a link someone sent you")
                                        .desired_width(ui.available_width() - 80.0),
                                );
                                let ready = bridge::extract_ticket(&self.link_input).is_some();
                                if ui
                                    .add_enabled(ready, egui::Button::new("Get files"))
                                    .clicked()
                                {
                                    self.bridge.send(Cmd::Receive(self.link_input.clone()));
                                    self.link_input.clear();
                                }
                            });
                        });

                    // ── transfers ─────────────────────────────────────────
                    if !snapshot.transfers.is_empty() {
                        ui.add_space(14.0);
                        ui.horizontal(|ui| {
                            ui.label(egui::RichText::new("Files").strong());
                            ui.with_layout(
                                egui::Layout::right_to_left(egui::Align::Center),
                                |ui| {
                                    let any_finished =
                                        snapshot.transfers.iter().any(|t| t.finished);
                                    if any_finished && ui.small_button("Clear").clicked() {
                                        self.bridge.send(Cmd::ClearFinished);
                                    }
                                },
                            );
                        });
                        ui.separator();
                        for transfer in snapshot.transfers.iter().rev().take(8) {
                            ui.horizontal(|ui| {
                                if let Some(error) = &transfer.failed {
                                    ui.colored_label(
                                        egui::Color32::from_rgb(200, 80, 80),
                                        format!("\u{26a0} {:?}", transfer.name),
                                    );
                                    ui.label(
                                        egui::RichText::new(shorten(error, 40)).small().weak(),
                                    );
                                    if !transfer.drop.is_empty()
                                        && ui.small_button("Try again").clicked()
                                    {
                                        self.bridge.send(Cmd::Retry {
                                            drop: transfer.drop.clone(),
                                            name: transfer.name.clone(),
                                        });
                                    }
                                } else if transfer.finished {
                                    ui.label(format!("\u{2713} {:?}", transfer.name));
                                    if let Some(first) = transfer.saved_to.first() {
                                        if ui.small_button("Show").clicked() {
                                            open_folder(parent_of(first));
                                        }
                                    }
                                } else {
                                    ui.label(format!("{:?}", transfer.name));
                                    match transfer.fraction() {
                                        Some(fraction) => {
                                            ui.add(
                                                egui::ProgressBar::new(fraction)
                                                    .desired_width(140.0),
                                            );
                                            if let Some(label) = progress_label(transfer) {
                                                ui.label(egui::RichText::new(label).small().weak());
                                            }
                                        }
                                        None => {
                                            ui.spinner();
                                        }
                                    }
                                }
                            });
                        }
                    }

                    // ── new in your groups: offered, not yet fetched ──────
                    if !snapshot.available.is_empty() {
                        ui.add_space(14.0);
                        ui.label(egui::RichText::new("New in your groups").strong());
                        ui.label(
                            egui::RichText::new(
                                "Offered while you were away, or a question that timed out. It \
                                 stays here, ready, until you get it or leave the group.",
                            )
                            .small()
                            .weak(),
                        );
                        ui.separator();
                        for row in &snapshot.available {
                            ui.horizontal(|ui| {
                                ui.label(format!(
                                    "{:?}  \u{2014} {} in {}",
                                    row.name, row.size, row.group
                                ));
                                let key = format!("{}/{}", row.drop, row.pick);
                                let pending = self.getting.contains(&key);
                                if ui
                                    .add_enabled(
                                        !pending,
                                        egui::Button::new(if pending { "Getting…" } else { "Get" })
                                            .small(),
                                    )
                                    .clicked()
                                {
                                    self.getting.insert(key);
                                    self.bridge.send(Cmd::Fetch {
                                        drop: row.drop.clone(),
                                        pick: row.pick.clone(),
                                        name: row.name.clone(),
                                    });
                                }
                            });
                        }
                    }

                    // ── your groups ───────────────────────────────────────
                    if !snapshot.drops.is_empty() {
                        ui.add_space(14.0);
                        ui.label(egui::RichText::new("Your groups").strong());
                        ui.label(
                            egui::RichText::new(
                                "You stay in these until you leave. Everything in them keeps \
                                 being served from this machine while the helper runs.",
                            )
                            .small()
                            .weak(),
                        );
                        ui.separator();
                        for row in &snapshot.drops {
                            ui.horizontal(|ui| {
                                ui.label(format!(
                                    "{}  \u{2014} {} file(s), {}",
                                    row.name,
                                    row.files,
                                    if row.peers == 0 {
                                        "waiting".to_string()
                                    } else {
                                        format!("{} connected", row.peers)
                                    }
                                ));
                                if ui.small_button("Link").clicked() {
                                    self.bridge.send(Cmd::Ticket {
                                        handle: row.handle.clone(),
                                        name: row.name.clone(),
                                    });
                                }
                                if ui
                                    .small_button(if row.mine { "Stop" } else { "Leave" })
                                    .clicked()
                                {
                                    self.bridge.send(Cmd::Forget(row.handle.clone()));
                                }
                            });
                        }
                    }

                    // ── nothing at all yet ────────────────────────────────
                    if snapshot.drops.is_empty()
                        && snapshot.transfers.is_empty()
                        && snapshot.available.is_empty()
                        && snapshot.incoming.is_empty()
                        && snapshot.share_link.is_none()
                    {
                        ui.add_space(28.0);
                        ui.vertical_centered(|ui| {
                            ui.label(
                                egui::RichText::new("Nothing yet").strong(),
                            );
                            ui.label(
                                egui::RichText::new(
                                    "Send something and hand over the link, or paste a link \
                                     someone sent you.\nGroups you join stay joined until you \
                                     leave them.",
                                )
                                .small()
                                .weak(),
                            );
                        });
                    }

                    ui.add_space(8.0);
                });
        });
    }
}

/// "1.2 of 3.4 MiB", when the total is known.
fn progress_label(transfer: &bridge::Transfer) -> Option<String> {
    transfer
        .total
        .filter(|&total| total > 0)
        .map(|total| format!("{} of {}", human_bytes(transfer.done), human_bytes(total)))
}

/// Same convention the daemon prints: binary units, one decimal.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Errors can be long; the row cannot.
fn shorten(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let taken: String = text.chars().take(max.saturating_sub(1)).collect();
    format!("{taken}…")
}

fn parent_of(path: &str) -> &str {
    match path.rfind(std::path::MAIN_SEPARATOR) {
        Some(index) => &path[..index],
        None => path,
    }
}

/// Reveal a directory in the platform's file manager.
fn open_folder(path: &str) {
    #[cfg(target_os = "macos")]
    let program = "open";
    #[cfg(target_os = "windows")]
    let program = "explorer";
    #[cfg(all(unix, not(target_os = "macos")))]
    let program = "xdg-open";
    let _ = std::process::Command::new(program).arg(path).spawn();
}

/// An immutable copy of the state, taken once per frame.
struct Snapshot {
    connected: bool,
    lan_only: bool,
    download_dir: String,
    share_link: Option<String>,
    share_link_name: Option<String>,
    busy: Option<String>,
    error: Option<String>,
    incoming: Vec<bridge::Incoming>,
    transfers: Vec<bridge::Transfer>,
    drops: Vec<bridge::DropRow>,
    available: Vec<bridge::AvailableRow>,
}

impl From<&UiState> for Snapshot {
    fn from(state: &UiState) -> Self {
        Self {
            connected: state.connected,
            lan_only: state.lan_only,
            download_dir: state.download_dir.clone(),
            share_link: state.share_link.clone(),
            share_link_name: state.share_link_name.clone(),
            busy: state.busy.clone(),
            error: state.error.clone(),
            incoming: state.incoming.clone(),
            transfers: state.transfers.clone(),
            drops: state.drops.clone(),
            available: state.available.clone(),
        }
    }
}
