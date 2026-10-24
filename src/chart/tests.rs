//! Tests of the chart.

use std::sync::Arc;

use arrow_array::types::Int8Type;
use arrow_array::{ArrayRef, BinaryArray, BooleanArray, DictionaryArray, StringViewArray};
use arrow_array::{Date32Array, Float64Array, Int64Array, StringArray};
use arrow_array::{Decimal128Array, TimestampMillisecondArray, UInt64Array};
use arrow_schema::{Field, Schema};
use densa_draw::{Instance, InstanceBuffer};
use densa_text::{AtlasConfig, GlyphCache};

use super::*;

/// A result of `columns` with the key `#` after them, numbered from `first`.
fn batch(first: u64, columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    let rows = columns[0].1.len() as u64;
    let mut columns = columns;
    columns.push((
        "#",
        Arc::new(UInt64Array::from_iter_values(first..first + rows)),
    ));
    RecordBatch::try_from_iter(columns).unwrap()
}

fn chart_of(columns: Vec<(&str, ArrayRef)>) -> Plotted {
    let batch = batch(0, columns);
    let mut chart = Plotted::new(&batch.schema(), "#");
    chart.add(&batch).unwrap();
    chart.finish();
    chart
}

fn ints(values: &[i64]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

fn floats(values: &[f64]) -> ArrayRef {
    Arc::new(Float64Array::from(values.to_vec()))
}

fn texts(values: &[&str]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

/// Pane `pane`'s points, by `x`.
fn points(chart: &Plotted, pane: usize) -> Vec<(f64, f64)> {
    let (xs, ys) = chart.points(pane).unwrap();
    xs.iter().copied().zip(ys.iter().copied()).collect()
}

#[test]
fn the_first_column_goes_across_and_each_number_column_gets_a_pane() {
    let chart = chart_of(vec![
        ("hour", ints(&[1, 2, 3])),
        ("service", texts(&["a", "b", "c"])),
        ("count", ints(&[10, 20, 30])),
        ("mean", floats(&[1.5, 2.5, 3.5])),
    ]);
    assert_eq!(chart.panes(), 2, "the text column is left out");
    assert_eq!(chart.pane_title(0), "count");
    assert_eq!(chart.pane_title(1), "mean");
    assert_eq!(chart.pane_title(2), "");
    assert_eq!(chart.caption(), "hour across; count, mean below");
    assert!(chart.describe().ends_with(chart.caption()));
    assert_eq!(chart.x_kind(), AxisKind::Number);
    assert_eq!(chart.x_extent(), Some((1.0, 3.0)));
    assert_eq!(points(&chart, 1), [(1.0, 1.5), (2.0, 2.5), (3.0, 3.5)]);
    let y = chart.y_axis(1, &Axis::number(0.0, 10.0)).unwrap();
    assert!(y.start() < 1.5 && y.end() > 3.5, "{y:?}");
    assert!(chart.y_axis(2, &Axis::number(0.0, 10.0)).is_none());
}

#[test]
fn the_row_numbers_can_be_anywhere() {
    let numbers: ArrayRef = Arc::new(UInt64Array::from(vec![7, 8]));
    let rows =
        RecordBatch::try_from_iter([("#", numbers), ("x", ints(&[1, 2])), ("y", ints(&[3, 4]))])
            .unwrap();
    let mut chart = Plotted::new(&rows.schema(), "#");
    chart.add(&rows).unwrap();
    assert_eq!(chart.caption(), "x across; y below");
    assert_eq!(points(&chart, 0), [(1.0, 3.0), (2.0, 4.0)]);
    assert_eq!(chart.mark_row(Some(8)), Some(2.0));
}

#[test]
fn times_dates_and_decimals_read_as_numbers() {
    let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_500, 2_000]));
    let chart = chart_of(vec![("at", times), ("n", ints(&[1, 2]))]);
    assert_eq!(chart.x_kind(), AxisKind::Time);
    assert_eq!(chart.x_extent(), Some((1.5, 2.0)), "seconds");
    let days: ArrayRef = Arc::new(Date32Array::from(vec![1, 2]));
    let money = Decimal128Array::from(vec![150, -25]).with_precision_and_scale(10, 2);
    let money: ArrayRef = Arc::new(money.unwrap());
    let chart = chart_of(vec![("day", days), ("money", money)]);
    assert_eq!(chart.x_kind(), AxisKind::Time);
    assert_eq!(points(&chart, 0), [(86_400.0, 1.5), (172_800.0, -0.25)]);
}

#[test]
fn rows_in_order_show_at_once_and_others_once_sorted_in() {
    let first = batch(0, vec![("x", ints(&[1, 3])), ("y", ints(&[10, 30]))]);
    let mut chart = Plotted::new(&first.schema(), "#");
    chart.add(&first).unwrap();
    let next = batch(2, vec![("x", ints(&[3, 4])), ("y", ints(&[31, 40]))]);
    chart.add(&next).unwrap();
    assert_eq!(points(&chart, 0).len(), 4, "in order: shown at once");

    let early = batch(4, vec![("x", ints(&[2, 0])), ("y", ints(&[20, 0]))]);
    chart.add(&early).unwrap();
    chart.prepare();
    assert_eq!(points(&chart, 0).len(), 4, "sorted in a while later");
    chart.finish();
    let expected = [
        (0.0, 0.0),
        (1.0, 10.0),
        (2.0, 20.0),
        (3.0, 30.0),
        (3.0, 31.0),
        (4.0, 40.0),
    ];
    assert_eq!(
        points(&chart, 0),
        expected,
        "same x: in the order they came"
    );
    assert_eq!(chart.x_extent(), Some((0.0, 4.0)));

    // A row after one still waiting waits too, so the order holds.
    chart
        .add(&batch(6, vec![("x", ints(&[-1])), ("y", ints(&[1]))]))
        .unwrap();
    chart
        .add(&batch(7, vec![("x", ints(&[9])), ("y", ints(&[1]))]))
        .unwrap();
    assert_eq!(chart.x_extent(), Some((0.0, 4.0)));
    chart.charted.as_mut().unwrap().sorted_at -= SORT_EVERY;
    chart.prepare();
    assert_eq!(
        chart.x_extent(),
        Some((-1.0, 9.0)),
        "once it has been a while"
    );
}

#[test]
fn many_rows_of_the_same_x_keep_the_order_they_came_in() {
    let first = batch(0, vec![("x", ints(&[5])), ("y", ints(&[0]))]);
    let mut chart = Plotted::new(&first.schema(), "#");
    chart.add(&first).unwrap();
    let x: Vec<i64> = (0..2_000).map(|i| i % 3).collect();
    let y: Vec<i64> = (1..=2_000).collect();
    chart
        .add(&batch(1, vec![("x", ints(&x)), ("y", ints(&y))]))
        .unwrap();
    chart.finish();
    let points = points(&chart, 0);
    assert!(points
        .windows(2)
        .all(|w| w[0].0 < w[1].0 || w[0].1 < w[1].1));
}

#[test]
fn rows_out_of_order_after_a_while_are_sorted_in_as_they_come() {
    let first = batch(0, vec![("x", ints(&[5])), ("y", ints(&[0]))]);
    let mut chart = Plotted::new(&first.schema(), "#");
    chart.add(&first).unwrap();
    chart.charted.as_mut().unwrap().sorted_at -= SORT_EVERY;
    chart
        .add(&batch(1, vec![("x", ints(&[1])), ("y", ints(&[0]))]))
        .unwrap();
    assert_eq!(chart.x_extent(), Some((1.0, 5.0)), "no paint needed");
}

#[test]
fn rows_without_a_place_across_are_left_out() {
    let x = floats(&[f64::NAN, 2.0, f64::INFINITY, f64::NEG_INFINITY, 1.0]);
    let chart = chart_of(vec![("x", x), ("y", ints(&[1, 2, 3, 4, 5]))]);
    assert_eq!(points(&chart, 0), [(1.0, 5.0), (2.0, 2.0)]);
    let nulls: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(4)]));
    let chart = chart_of(vec![("x", nulls), ("y", ints(&[1, 2]))]);
    assert_eq!(points(&chart, 0), [(4.0, 2.0)]);
    let gaps: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let chart = chart_of(vec![("x", ints(&[1, 2])), ("y", gaps)]);
    assert!(points(&chart, 0)[1].1.is_nan(), "a missing value is a gap");
}

#[test]
fn a_result_that_cannot_be_charted_says_why() {
    let bytes: ArrayRef = Arc::new(BinaryArray::from_vec(vec![b"a"]));
    let bytes_first = chart_of(vec![("blob", bytes), ("n", ints(&[1]))]);
    assert_eq!(bytes_first.panes(), 1, "one empty pane");
    assert_eq!(
        bytes_first.caption(),
        "Not charted: the first column, blob, is not a number, date, timestamp or text"
    );
    assert_eq!(bytes_first.x_extent(), None);
    assert_eq!(bytes_first.pane_title(0), "");
    assert_eq!(bytes_first.category(0), None);

    let no_numbers = chart_of(vec![("x", ints(&[1])), ("name", texts(&["a"]))]);
    assert_eq!(
        no_numbers.caption(),
        "Not charted: no number column after x"
    );
    let times: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1]));
    let times_after = chart_of(vec![("x", ints(&[1])), ("at", times)]);
    assert_eq!(
        times_after.caption(),
        "Not charted: no number column after x"
    );
    let alone = chart_of(vec![("x", ints(&[1]))]);
    assert!(alone.caption().contains("no number column"));

    let schema = Arc::new(Schema::new(vec![Field::new("#", DataType::UInt64, false)]));
    assert_eq!(
        Plotted::new(&schema, "#").caption(),
        "The result has no columns to chart"
    );
    assert_eq!(
        Plotted::new(&schema, "row").caption(),
        "The result has no row numbers"
    );
    assert_eq!(Plotted::waiting().caption(), WAITING);
}

#[test]
fn a_result_without_rows_to_chart_says_so_once_it_has_arrived() {
    let empty = batch(0, vec![("x", ints(&[])), ("y", ints(&[]))]);
    let mut chart = Plotted::new(&empty.schema(), "#");
    chart.add(&empty).unwrap();
    assert_eq!(chart.caption(), "x across; y below", "more may come");
    chart.finish();
    assert_eq!(chart.caption(), "Not charted: no row has a value for x");
    let mut waiting = Plotted::waiting();
    waiting.finish();
    assert_eq!(waiting.caption(), WAITING);
}

#[test]
fn panes_stop_at_the_most_shown_and_the_caption_says_so() {
    let columns: Vec<(&str, ArrayRef)> = ["x", "a", "b", "c", "d", "e"]
        .into_iter()
        .map(|name| (name, ints(&[1, 2])))
        .collect();
    let chart = chart_of(columns);
    assert_eq!(chart.panes(), MAX_PANES);
    assert_eq!(chart.pane_title(3), "d");
    assert_eq!(
        chart.caption(),
        "x across; a, b, c, d below (the first 4 of 5 number columns)"
    );
}

#[test]
fn four_number_columns_are_all_shown() {
    let columns: Vec<(&str, ArrayRef)> = ["x", "a", "b", "c", "d"]
        .into_iter()
        .map(|name| (name, ints(&[1, 2])))
        .collect();
    assert_eq!(chart_of(columns).caption(), "x across; a, b, c, d below");
}

#[test]
fn a_row_is_marked_where_it_is_across() {
    let mut chart = chart_of(vec![
        ("x", floats(&[7.0, 2.0, f64::NAN])),
        ("y", ints(&[1, 2, 3])),
    ]);
    assert_eq!(chart.mark_row(Some(0)), Some(7.0));
    assert_eq!(chart.mark, Some(7.0));
    assert_eq!(chart.mark_row(Some(1)), Some(2.0));
    assert_eq!(chart.mark_row(None), None, "no row selected");
    assert_eq!(chart.mark, None);
    assert_eq!(chart.mark_row(Some(1)), Some(2.0));
    assert_eq!(chart.mark_row(Some(2)), None, "no place across");
    assert_eq!(chart.mark, None);
    assert_eq!(chart.mark_row(Some(99)), None, "no such row");
    let mut waiting = Plotted::waiting();
    assert_eq!(waiting.mark_row(Some(0)), None);

    chart
        .add(&batch(3, vec![("x", floats(&[0.5])), ("y", ints(&[1]))]))
        .unwrap();
    assert_eq!(
        chart.mark_row(Some(3)),
        Some(0.5),
        "a row waiting is sorted in"
    );
}

#[test]
fn the_y_extent_is_found_again_when_rows_or_the_view_change() {
    let first = batch(0, vec![("x", ints(&[1, 2])), ("y", ints(&[10, 20]))]);
    let mut chart = Plotted::new(&first.schema(), "#");
    chart.add(&first).unwrap();
    let wide = Axis::number(0.0, 10.0);
    let range = |chart: &Plotted, x: &Axis| {
        let y = chart.y_axis(0, x).unwrap();
        (y.start(), y.end())
    };
    let before = range(&chart, &wide);
    assert_eq!(range(&chart, &wide), before, "the same again");
    chart
        .add(&batch(2, vec![("x", ints(&[3])), ("y", ints(&[90]))]))
        .unwrap();
    assert!(range(&chart, &wide).1 > 90.0, "a row came: found again");
    assert!(
        range(&chart, &Axis::number(0.0, 1.5)).1 < 20.0,
        "a narrower view"
    );
}

#[test]
fn rows_for_a_chart_that_shows_nothing_are_ignored() {
    let bytes: ArrayRef = Arc::new(BinaryArray::from_vec(vec![b"a"]));
    let rows = batch(0, vec![("blob", bytes), ("n", ints(&[1]))]);
    let mut chart = Plotted::new(&rows.schema(), "#");
    assert_eq!(chart.add(&rows), Ok(()));
}

#[test]
fn rows_unlike_the_columns_are_refused() {
    let rows = batch(0, vec![("x", ints(&[1])), ("y", ints(&[1]))]);
    let mut chart = Plotted::new(&rows.schema(), "#");
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![0]));
    let signed =
        RecordBatch::try_from_iter([("x", ints(&[1])), ("y", ints(&[1])), ("#", keys)]).unwrap();
    assert!(chart.add(&signed).is_err());
}

/// What pane 0 of `chart` draws across 0..`x_end` on a 400 x 200 plot.
fn drawn(chart: &Plotted, x_end: f64) -> Vec<Instance> {
    let mut cache = GlyphCache::new(AtlasConfig::default());
    let mut out = InstanceBuffer::with_capacity(65_536);
    let mut canvas = Canvas::begin(&mut cache, &mut out);
    let plot = Plot::new(
        Rect::new(0.0, 0.0, 400.0, 200.0),
        Axis::number(0.0, x_end),
        Axis::number(0.0, 10.0),
    );
    chart.paint(0, &plot, &mut canvas, &ChartPalette::new());
    out.as_slice().to_vec()
}

/// The squares drawn `DOT` wide.
fn dots(instances: &[Instance]) -> usize {
    let dot = |i: &&Instance| i.rect[2] == DOT && i.rect[3] == DOT;
    instances.iter().filter(dot).count()
}

#[test]
fn a_few_points_are_dots_on_a_line_and_many_a_line_alone() {
    let few = chart_of(vec![("x", ints(&[1, 2, 3])), ("y", ints(&[2, 5, 8]))]);
    let drawn_few = drawn(&few, 4.0);
    assert_eq!(dots(&drawn_few), 3);
    assert!(drawn_few.len() > 3, "and a line");

    let x: Vec<i64> = (0..300).collect();
    let y: Vec<i64> = x.iter().map(|x| x % 7).collect();
    let many = chart_of(vec![("x", ints(&x)), ("y", ints(&y))]);
    assert_eq!(dots(&drawn(&many, 300.0)), 0, "300 points in sight");
    assert_eq!(dots(&drawn(&many, 199.0)), 200, "200 in sight: 0 to 199");
    assert_eq!(dots(&drawn(&many, 200.0)), 0, "201 in sight");

    let off = chart_of(vec![("x", ints(&[1, 2])), ("y", ints(&[50, 5]))]);
    assert_eq!(
        dots(&drawn(&off, 4.0)),
        1,
        "a point above the plot has no dot"
    );
}

#[test]
fn the_marked_row_is_a_band_across_the_pane() {
    let band = |instances: &[Instance]| {
        let color = ChartPalette::new().line.with_alpha(0.3).to_array();
        instances
            .iter()
            .filter(|i| i.color == color && i.rect[3] == 200.0)
            .map(|i| i.rect)
            .collect::<Vec<_>>()
    };
    let mut chart = chart_of(vec![("x", ints(&[1, 2, 3])), ("y", ints(&[2, 5, 8]))]);
    assert!(band(&drawn(&chart, 4.0)).is_empty());
    chart.mark_row(Some(1));
    assert_eq!(
        band(&drawn(&chart, 4.0)),
        [[199.0, 0.0, 3.0, 200.0]],
        "x 2 of 0..4"
    );
    assert!(band(&drawn(&chart, 1.5)).is_empty(), "out of sight");

    let mut waiting = Plotted::waiting();
    waiting.mark = Some(1.0);
    assert_eq!(
        band(&drawn(&waiting, 4.0)).len(),
        1,
        "drawn before any series"
    );
}

#[test]
#[ignore = "a measurement: cargo test --release frame_of -- --ignored --nocapture"]
fn a_frame_of_a_million_rows_in_four_panes() {
    let rows = 1_000_000_i64;
    let mut chart = None;
    let started = Instant::now();
    for start in (0..rows).step_by(8_192) {
        let x: Vec<i64> = (start..(start + 8_192).min(rows)).collect();
        let y = |k: i64| ints(&x.iter().map(|v| v * k % 977).collect::<Vec<_>>());
        let columns = vec![
            ("x", ints(&x)),
            ("a", y(3)),
            ("b", y(5)),
            ("c", y(7)),
            ("d", y(11)),
        ];
        let rows = batch(start as u64, columns);
        let chart = chart.get_or_insert_with(|| Plotted::new(&rows.schema(), "#"));
        chart.add(&rows).unwrap();
    }
    let chart = chart.unwrap();
    println!("taking the rows in: {:?}", started.elapsed());
    let mut cache = GlyphCache::new(AtlasConfig::default());
    let mut out = InstanceBuffer::with_capacity(200_000);
    let x = Axis::number(0.0, rows as f64);
    for _ in 0..3 {
        let started = Instant::now();
        let mut canvas = Canvas::begin(&mut cache, &mut out);
        for pane in 0..4 {
            let y = chart.y_axis(pane, &x).unwrap();
            let top = pane as f32 * 200.0;
            let plot = Plot::new(Rect::new(0.0, top, 1_600.0, 200.0), x, y);
            chart.paint(pane, &plot, &mut canvas, &ChartPalette::new());
        }
        println!("a frame: {:?}", started.elapsed());
    }
}

#[test]
fn a_text_first_column_makes_a_bar_for_each_row_in_the_results_order() {
    let mut chart = chart_of(vec![
        ("service", texts(&["web", "db", "web"])),
        ("count", ints(&[30, 10, 20])),
        ("note", texts(&["x", "y", "z"])),
        ("mean", floats(&[1.5, 2.5, 3.5])),
    ]);
    assert_eq!(chart.x_kind(), AxisKind::Category);
    assert_eq!(
        chart.caption(),
        "Bars of count, mean, a bar for each service"
    );
    assert_eq!(chart.panes(), 2);
    assert_eq!(points(&chart, 0), [(0.0, 30.0), (1.0, 10.0), (2.0, 20.0)]);
    assert_eq!(points(&chart, 1)[2], (2.0, 3.5));
    let names: Vec<_> = (0..4).map(|i| chart.category(i)).collect();
    assert_eq!(
        names,
        [Some("web"), Some("db"), Some("web"), None],
        "repeats keep their bar"
    );
    assert_eq!(
        chart.x_extent(),
        Some((-0.5, 2.5)),
        "each bar's room in sight"
    );
    assert_eq!(chart.x_step(), Some(1.0));
    assert_eq!(chart.mark_row(Some(1)), Some(1.0));

    chart
        .add(&batch(
            3,
            vec![
                ("service", texts(&["cache"])),
                ("count", ints(&[5])),
                ("note", texts(&["w"])),
                ("mean", floats(&[0.5])),
            ],
        ))
        .unwrap();
    assert_eq!(
        chart.category(3),
        Some("cache"),
        "more rows, more bars after"
    );
    assert_eq!(points(&chart, 0)[3], (3.0, 5.0));
}

#[test]
fn bars_can_be_named_by_any_text_true_or_false_and_nulls() {
    let names = |column: ArrayRef| {
        let rows = column.len();
        let chart = chart_of(vec![("name", column), ("n", ints(&vec![1; rows]))]);
        assert_eq!(chart.x_kind(), AxisKind::Category);
        (0..rows)
            .map(|i| chart.category(i).unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    let flags: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)]));
    assert_eq!(names(flags), ["true", "null", "false"]);
    let views: ArrayRef = Arc::new(StringViewArray::from(vec!["a long name past twelve", "b"]));
    assert_eq!(names(views), ["a long name past twelve", "b"]);
    let coded: DictionaryArray<Int8Type> = vec!["east", "west", "east"].into_iter().collect();
    assert_eq!(names(Arc::new(coded)), ["east", "west", "east"]);
    let gaps: ArrayRef = Arc::new(StringArray::from(vec![None, Some("b")]));
    assert_eq!(names(gaps), ["null", "b"]);
}

#[test]
fn rows_without_a_row_number_get_no_bar() {
    let keys: ArrayRef = Arc::new(UInt64Array::from(vec![Some(0), None, Some(2)]));
    let rows = RecordBatch::try_from_iter([
        ("name", texts(&["a", "b", "c"])),
        ("n", ints(&[1, 2, 3])),
        ("#", keys),
    ])
    .unwrap();
    let mut chart = Plotted::new(&rows.schema(), "#");
    chart.add(&rows).unwrap();
    assert_eq!(points(&chart, 0), [(0.0, 1.0), (1.0, 3.0)]);
    assert_eq!(chart.category(1), Some("c"));
}

#[test]
fn bars_rise_from_zero_and_their_scale_shows_it() {
    let chart = chart_of(vec![("name", texts(&["a", "b"])), ("n", ints(&[40, 50]))]);
    let y = chart.y_axis(0, &Axis::category(-0.5, 1.5)).unwrap();
    assert!(y.start() < 0.0 && y.end() > 50.0, "{y:?}");
    let below = chart_of(vec![("name", texts(&["a", "b"])), ("n", ints(&[-40, -50]))]);
    let y = below.y_axis(0, &Axis::category(-0.5, 1.5)).unwrap();
    assert!(y.start() < -50.0 && y.end() > 0.0, "{y:?}");
    let line = chart_of(vec![("x", ints(&[1, 2])), ("n", ints(&[40, 50]))]);
    let y = line.y_axis(0, &Axis::number(0.0, 3.0)).unwrap();
    assert!(y.start() > 30.0, "a line keeps to its values: {y:?}");
    assert_eq!(line.x_step(), None);
    assert_eq!(line.x_extent(), Some((1.0, 2.0)));
}

#[test]
fn bars_are_drawn_as_columns_without_dots_or_a_line() {
    let chart = chart_of(vec![
        ("name", texts(&["a", "b", "c"])),
        ("n", ints(&[2, 5, 8])),
    ]);
    let mut cache = GlyphCache::new(AtlasConfig::default());
    let mut out = InstanceBuffer::with_capacity(1_024);
    let mut canvas = Canvas::begin(&mut cache, &mut out);
    let plot = Plot::new(
        Rect::new(0.0, 0.0, 300.0, 200.0),
        Axis::category(-0.5, 2.5),
        Axis::number(0.0, 10.0),
    );
    chart.paint(0, &plot, &mut canvas, &ChartPalette::new());
    let bars: Vec<[f32; 4]> = out.as_slice().iter().map(|i| i.rect).collect();
    assert_eq!(bars.len(), 3, "{bars:?}");
    // A bar 70% of its 100 pixels wide, from zero (the bottom) to its value.
    assert_eq!(bars[2][2], 70.0);
    assert_eq!((bars[2][1], bars[2][3]), (40.0, 160.0));
}

#[test]
fn the_marked_bar_has_its_whole_place_shaded_behind_it() {
    let mut chart = chart_of(vec![
        ("name", texts(&["a", "b", "c"])),
        ("n", ints(&[2, 5, 8])),
    ]);
    chart.mark_row(Some(1));
    let mut cache = GlyphCache::new(AtlasConfig::default());
    let mut out = InstanceBuffer::with_capacity(1_024);
    let mut canvas = Canvas::begin(&mut cache, &mut out);
    let plot = Plot::new(
        Rect::new(0.0, 0.0, 300.0, 200.0),
        Axis::category(-0.5, 2.5),
        Axis::number(0.0, 10.0),
    );
    chart.paint(0, &plot, &mut canvas, &ChartPalette::new());
    let shade = ChartPalette::new().line.with_alpha(0.3).to_array();
    let first = out.as_slice()[0];
    assert_eq!(first.color, shade, "under the bars");
    assert_eq!(first.rect, [100.0, 0.0, 100.0, 200.0]);
}

#[test]
fn a_long_name_is_kept_to_what_could_show() {
    let long = "é".repeat(NAME_CHARS + 50);
    let chart = chart_of(vec![("name", texts(&[&long, "b"])), ("n", ints(&[1, 2]))]);
    assert_eq!(chart.category(0).unwrap().chars().count(), NAME_CHARS);
    let short = "é".repeat(NAME_CHARS);
    let chart = chart_of(vec![("name", texts(&[&short])), ("n", ints(&[1]))]);
    assert_eq!(chart.category(0), Some(short.as_str()));
}

#[test]
fn a_marked_bar_half_in_sight_is_shaded_within_the_plot() {
    let mut chart = chart_of(vec![
        ("name", texts(&["a", "b", "c"])),
        ("n", ints(&[2, 5, 8])),
    ]);
    chart.mark_row(Some(2));
    let mut cache = GlyphCache::new(AtlasConfig::default());
    let mut out = InstanceBuffer::with_capacity(1_024);
    let mut canvas = Canvas::begin(&mut cache, &mut out);
    // Bar 2's place runs 1.5..2.5: its left part is in sight of 0..1.8.
    let plot = Plot::new(
        Rect::new(0.0, 0.0, 180.0, 200.0),
        Axis::category(0.0, 1.8),
        Axis::number(0.0, 10.0),
    );
    chart.paint(0, &plot, &mut canvas, &ChartPalette::new());
    let shade = ChartPalette::new().line.with_alpha(0.3).to_array();
    let band: Vec<[f32; 4]> = out
        .as_slice()
        .iter()
        .filter(|i| i.color == shade)
        .map(|i| i.rect)
        .collect();
    assert_eq!(band, [[150.0, 0.0, 30.0, 200.0]], "cut at the plot's edge");
}
