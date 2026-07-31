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

use std::path::PathBuf;

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
            .with_title("iroh-drop"),
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
}

impl App {
    fn new(bridge: Bridge) -> Self {
        Self {
            bridge,
            link_input: String::new(),
            qr_open: false,
        }
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
            if let Some(error) = &snapshot.error {
                ui.colored_label(egui::Color32::from_rgb(200, 80, 80), error);
                ui.add_space(6.0);
            }

            // ── consent, first, because it is the only thing that blocks ──
            for incoming in &snapshot.incoming {
                egui::Frame::group(ui.style())
                    .fill(ui.visuals().extreme_bg_color)
                    .show(ui, |ui| {
                        ui.label(egui::RichText::new("Someone wants to send you:").strong());
                        // Quoted: a filename must never be able to look like
                        // our own text.
                        ui.label(format!("{:?}   {}", incoming.name, incoming.size));
                        let remaining = incoming
                            .expires_at
                            .saturating_duration_since(std::time::Instant::now())
                            .as_secs();
                        ui.label(
                            egui::RichText::new(format!(
                                "from {}  \u{2022}  expires in {}:{:02}",
                                incoming.from,
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

            // ── send ──────────────────────────────────────────────────────
            ui.heading("Send");
            ui.label("Drag files here, or:");
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
                ui.label("Send this link to whoever should get the files:");
                let mut shown = link.clone();
                ui.add(
                    egui::TextEdit::multiline(&mut shown)
                        .desired_rows(2)
                        .desired_width(f32::INFINITY)
                        .font(egui::TextStyle::Monospace),
                );
                ui.horizontal(|ui| {
                    if ui.button("Copy link").clicked() {
                        ctx.copy_text(link.clone());
                    }
                    if ui.button(if self.qr_open { "Hide code" } else { "Show code" }).clicked() {
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

            ui.add_space(14.0);

            // ── receive ───────────────────────────────────────────────────
            ui.heading("Receive");
            ui.horizontal(|ui| {
                ui.add(
                    egui::TextEdit::singleline(&mut self.link_input)
                        .hint_text("paste a link")
                        .desired_width(ui.available_width() - 90.0),
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

            // ── transfers ─────────────────────────────────────────────────
            if !snapshot.transfers.is_empty() {
                ui.add_space(12.0);
                ui.heading("Files");
                for transfer in snapshot.transfers.iter().rev().take(8) {
                    ui.horizontal(|ui| {
                        if let Some(error) = &transfer.failed {
                            ui.colored_label(
                                egui::Color32::from_rgb(200, 80, 80),
                                format!("{:?} failed: {error}", transfer.name),
                            );
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
                                    ui.add(egui::ProgressBar::new(fraction).desired_width(160.0));
                                }
                                None => {
                                    ui.spinner();
                                }
                            }
                        }
                    });
                }
            }

            // ── new in your groups: offered, not yet fetched ─────────────
            if !snapshot.available.is_empty() {
                ui.add_space(12.0);
                ui.heading("New in your groups");
                ui.label(
                    egui::RichText::new(
                        "You are in these groups until you leave them; anything offered stays                          here, ready to fetch.",
                    )
                    .small()
                    .weak(),
                );
                for row in &snapshot.available {
                    ui.horizontal(|ui| {
                        ui.label(format!(
                            "{:?}  \u{2014} {} in {}",
                            row.name, row.size, row.group
                        ));
                        if ui.small_button("Get").clicked() {
                            self.bridge.send(Cmd::Fetch {
                                drop: row.drop.clone(),
                                pick: row.pick.clone(),
                                name: row.name.clone(),
                            });
                        }
                    });
                }
            }

            // ── what you are still sharing ────────────────────────────────
            if !snapshot.drops.is_empty() {
                ui.add_space(12.0);
                ui.heading("Still sharing");
                ui.label(
                    egui::RichText::new(
                        "These stay available to people who have the link, even after you close \
                         this window \u{2014} as long as the background helper is running.",
                    )
                    .small()
                    .weak(),
                );
                for row in &snapshot.drops {
                    ui.horizontal(|ui| {
                        ui.label(format!(
                            "{}  \u{2014} {} file(s), {} connected",
                            row.name, row.files, row.peers
                        ));
                        if ui
                            .small_button(if row.mine { "Stop" } else { "Leave" })
                            .clicked()
                        {
                            self.bridge.send(Cmd::Forget(row.handle.clone()));
                        }
                    });
                }
            }
        });
    }
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
            busy: state.busy.clone(),
            error: state.error.clone(),
            incoming: state.incoming.clone(),
            transfers: state.transfers.clone(),
            drops: state.drops.clone(),
            available: state.available.clone(),
        }
    }
}
