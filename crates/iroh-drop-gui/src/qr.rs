//! A scannable QR code, painted as rectangles.
//!
//! Drawn rather than composed from block characters: a phone camera needs
//! crisp squares and a quiet border, and text glyphs give neither reliably.

use eframe::egui;
use qrcode::{EcLevel, QrCode};

/// Paint `data` as a QR code sized to the available width.
pub fn show(ui: &mut egui::Ui, data: &str) {
    // Low EC: a ticket is long, and the code is read from a screen at close
    // range rather than off a crumpled poster.
    let code = match QrCode::with_error_correction_level(data.as_bytes(), EcLevel::L) {
        Ok(code) => code,
        Err(_) => {
            ui.weak("This link is too long to show as a code.");
            return;
        }
    };

    let modules = code.to_colors();
    let width = code.width();
    let quiet = 2usize;
    let total = width + quiet * 2;

    let available = ui.available_width().clamp(120.0, 320.0);
    let scale = (available / total as f32).floor().max(1.0);
    let side = scale * total as f32;

    let (response, painter) =
        ui.allocate_painter(egui::vec2(side, side), egui::Sense::hover());
    let origin = response.rect.min;

    // The quiet zone must be light, so paint the whole field first.
    painter.rect_filled(response.rect, 0.0, egui::Color32::WHITE);

    for y in 0..width {
        for x in 0..width {
            if modules[y * width + x] == qrcode::Color::Dark {
                let min = origin
                    + egui::vec2((x + quiet) as f32 * scale, (y + quiet) as f32 * scale);
                painter.rect_filled(
                    egui::Rect::from_min_size(min, egui::vec2(scale, scale)),
                    0.0,
                    egui::Color32::BLACK,
                );
            }
        }
    }
}
