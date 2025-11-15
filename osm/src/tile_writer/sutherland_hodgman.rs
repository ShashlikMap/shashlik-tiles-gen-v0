use geo::{coord, Coord, LineString, Rect};
fn intersect_edge(a: &Coord, b: &Coord, side: i8, rect: &Rect) -> Coord {
    if side & 8 != 0 {
        let cx = a.x + (b.x - a.x) * (rect.max().y - a.y) / (b.y - a.y);
        let cy = rect.max().y;
        return coord! { x: cx, y: cy };
    }

    if side & 4 != 0 {
        let cx = a.x + (b.x - a.x) * (rect.min().y - a.y) / (b.y - a.y);
        let cy = rect.min().y;
        return coord! { x: cx, y: cy };
    }

    if side & 2 != 0 {
        let cx = rect.max().x;
        let cy = a.y + (b.y - a.y) * (rect.max().x - a.x) / (b.x - a.x);
        return coord! { x: cx, y: cy };
    }

    if side & 1 != 0 {
        let cx = rect.min().x;
        let cy = a.y + (b.y - a.y) * (rect.min().x - a.x) / (b.x - a.x);
        return coord! { x: cx, y: cy };
    }

    panic!("Should never happen")
}

fn get_side(coord: &Coord, rect: &Rect) -> i8 {
    let mut code = 0;
    if coord.x < rect.min().x {
        code |= 1;
    } else if coord.x > rect.max().x {
        code |= 2;
    }
    if coord.y < rect.min().y {
        code |= 4;
    } else if coord.y > rect.max().y {
        code |= 8;
    }
    code
}

pub fn sutherland_hodgman_clip(subject_polygon: &LineString, rect: &Rect) -> Option<LineString> {
    let mut points = subject_polygon.0.clone();
    let mut edge = 1;
    while edge <= 8 {
        let mut result = Vec::new();
        let mut prev = *points.last().unwrap();
        let mut prev_inside = (get_side(&prev, rect) & edge) == 0;

        for p in points {
            let inside = (get_side(&p, rect) & edge) == 0;

            if inside != prev_inside {
                result.push(intersect_edge(&prev, &p, edge, rect));
            }
            if inside {
                result.push(p);
            }
            prev = p;
            prev_inside = inside;
        }
        points = result;
        if points.is_empty() {
            break;
        }
        edge *= 2;
    }

    if points.len() >= 2 {
        Some(LineString(points))
    } else {
        None
    }
}
