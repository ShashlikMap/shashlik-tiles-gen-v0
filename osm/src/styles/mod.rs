pub mod style_loader;

use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
pub struct Style {
    pub id: String,
    pub render_style: RenderStyle,
}

#[derive(Serialize, Deserialize)]
pub enum RenderStyle {
    Fill(RenderStyleColor),
    Border(RenderStyleColor, f32),
    Dashed(RenderStyleColor, RenderStyleColor),
}

#[derive(Serialize, Deserialize)]
pub struct RenderStyleColor {
    r: f32,
    g: f32,
    b: f32,
    a: f32,
}

impl RenderStyleColor {
    pub fn as_array(&self) -> [f32; 4] {
        [self.r, self.g, self.b, self.a]
    }
}
