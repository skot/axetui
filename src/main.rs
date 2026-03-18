use std::{
    collections::VecDeque,
    env,
    io,
    process,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    prelude::*,
    symbols,
    widgets::{Block, BorderType, Borders, Clear, Gauge, Paragraph, Sparkline, Wrap},
};
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;
use tungstenite::{Message, connect};

const UI_TICK_RATE: Duration = Duration::from_millis(200);
const POLL_INTERVAL: Duration = Duration::from_secs(3);
const HISTORY_CAP: usize = 120;

fn main() -> io::Result<()> {
    let config = Config::from_args();
    let mut terminal = setup_terminal()?;
    let app_result = run_app(&mut terminal, config);
    restore_terminal(&mut terminal)?;
    app_result
}

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    config: Config,
) -> io::Result<()> {
    let (poll_tx, poll_rx) = mpsc::channel();
    let (control_tx, control_rx) = mpsc::channel();
    spawn_polling_thread(config.clone(), poll_tx.clone(), control_rx);
    spawn_log_thread(config.clone(), poll_tx.clone());

    let mut app = App::new(config);
    let mut last_tick = Instant::now();

    loop {
        while let Ok(message) = poll_rx.try_recv() {
            app.handle_poll_message(message);
        }

        terminal.draw(|frame| draw(frame, &app))?;

        let timeout = UI_TICK_RATE.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => return Ok(()),
                        KeyCode::Char('r') => app.request_refresh(&control_tx),
                        _ => {}
                    }
                }
            }
        }

        if last_tick.elapsed() >= UI_TICK_RATE {
            app.on_tick();
            last_tick = Instant::now();
        }
    }
}

fn spawn_log_thread(config: Config, tx: Sender<PollMessage>) {
    thread::spawn(move || {
        loop {
            let ws_url = config.websocket_url();
            match connect(ws_url.as_str()) {
                Ok((mut socket, _)) => {
                    if tx
                        .send(PollMessage::LogStatus(format!(
                            "ws connected to {}",
                            config.websocket_url()
                        )))
                        .is_err()
                    {
                        break;
                    }

                    loop {
                        match socket.read() {
                            Ok(Message::Text(text)) => {
                                if tx.send(PollMessage::LogLine(text.to_string())).is_err() {
                                    return;
                                }
                            }
                            Ok(Message::Binary(bytes)) => {
                                let line = String::from_utf8_lossy(&bytes).trim().to_string();
                                if !line.is_empty() && tx.send(PollMessage::LogLine(line)).is_err() {
                                    return;
                                }
                            }
                            Ok(Message::Close(_)) => {
                                let _ = tx.send(PollMessage::LogStatus(
                                    "ws closed by Bitaxe, retrying".to_string(),
                                ));
                                break;
                            }
                            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Frame(_)) => {}
                            Err(error) => {
                                let _ = tx.send(PollMessage::LogStatus(format!(
                                    "ws error: {error}; reconnecting"
                                )));
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    if tx
                        .send(PollMessage::LogStatus(format!(
                            "ws connect failed: {error}; retrying"
                        )))
                        .is_err()
                    {
                        break;
                    }
                }
            }

            thread::sleep(Duration::from_secs(2));
        }
    });
}

fn spawn_polling_thread(
    config: Config,
    tx: Sender<PollMessage>,
    control_rx: Receiver<ControlMessage>,
) {
    thread::spawn(move || {
        let client = match Client::builder()
            .timeout(Duration::from_secs(4))
            .user_agent("axetui/0.1")
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                let _ = tx.send(PollMessage::Error(format!("HTTP client setup failed: {error}")));
                return;
            }
        };

        let mut cached_asic: Option<AsicInfo> = None;

        loop {
            match fetch_snapshot(&client, &config, cached_asic.clone()) {
                Ok((snapshot, asic)) => {
                    cached_asic = Some(asic);
                    if tx.send(PollMessage::Snapshot(snapshot)).is_err() {
                        break;
                    }
                }
                Err(error) => {
                    if tx.send(PollMessage::Error(error)).is_err() {
                        break;
                    }
                }
            }

            match control_rx.recv_timeout(POLL_INTERVAL) {
                Ok(ControlMessage::Refresh) => continue,
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
    });
}

fn fetch_snapshot(
    client: &Client,
    config: &Config,
    cached_asic: Option<AsicInfo>,
) -> Result<(BitaxeSnapshot, AsicInfo), String> {
    let system_info: SystemInfo = client
        .get(format!("{}/api/system/info", config.base_url))
        .send()
        .map_err(|error| format!("system info request failed: {error}"))?
        .error_for_status()
        .map_err(|error| format!("system info response failed: {error}"))?
        .json()
        .map_err(|error| format!("system info JSON failed: {error}"))?;

    let asic = match cached_asic {
        Some(asic) => asic,
        None => client
            .get(format!("{}/api/system/asic", config.base_url))
            .send()
            .map_err(|error| format!("ASIC request failed: {error}"))?
            .error_for_status()
            .map_err(|error| format!("ASIC response failed: {error}"))?
            .json()
            .map_err(|error| format!("ASIC JSON failed: {error}"))?,
    };

    Ok((BitaxeSnapshot::from_api(system_info, &asic), asic))
}

fn draw(frame: &mut Frame, app: &App) {
    let theme = Theme::default();
    let area = frame.area();

    let vertical = Layout::vertical([
        Constraint::Length(3),
        Constraint::Min(20),
        Constraint::Length(12),
        Constraint::Length(4),
    ])
    .split(area);

    render_header(frame, vertical[0], app, theme);
    render_main(frame, vertical[1], app, theme);
    render_log_pane(frame, vertical[2], app, theme);
    render_footer(frame, vertical[3], app, theme);
}

fn render_header(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let layout = Layout::horizontal([Constraint::Min(32), Constraint::Length(48)]).split(area);

    let title = Paragraph::new(Line::from(vec![
        " BITAXE ".fg(theme.accent).bold(),
        "live terminal dashboard".fg(theme.text),
        "  ".into(),
        app.connection_label().fg(theme.info),
    ]))
    .block(panel("Node", theme))
    .wrap(Wrap { trim: true });
    frame.render_widget(title, layout[0]);

    let session_line = compact_session_line(app, layout[1].width.saturating_sub(2) as usize);
    let status = Paragraph::new(session_line)
        .alignment(Alignment::Left)
        .block(panel("Session", theme))
        .wrap(Wrap { trim: true });
    frame.render_widget(status, layout[1]);
}

fn render_main(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let rows = Layout::vertical([Constraint::Min(12), Constraint::Length(7)])
        .spacing(1)
        .split(area);

    let top = Layout::horizontal([
        Constraint::Percentage(48),
        Constraint::Percentage(52),
    ])
    .spacing(1)
    .split(rows[0]);

    let bottom = Layout::horizontal([
        Constraint::Percentage(48),
        Constraint::Percentage(52),
    ])
    .spacing(1)
    .split(rows[1]);

    render_hashrate_chart(frame, top[0], app, theme);
    render_meter_panels(frame, top[1], app, theme);
    render_hashrate_stats(frame, bottom[0], app, theme);
    render_status_pane(frame, bottom[1], app, theme);
}

fn render_hashrate_chart(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let spark = Sparkline::default()
        .block(panel("Hashrate", theme))
        .data(&app.hashrate_spark)
        .max(app.hashrate_scale_max())
        .style(Style::default().fg(theme.accent))
        .bar_set(symbols::bar::NINE_LEVELS);
    frame.render_widget(spark, area);
}

fn render_hashrate_stats(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let stats = vec![
        Line::from(vec![
            "Current".fg(theme.muted),
            "  ".into(),
            format!("{:.1} GH/s", app.snapshot.hashrate).fg(theme.text).bold(),
        ]),
        Line::from(vec![
            "Best Diff".fg(theme.muted),
            "  ".into(),
            app.snapshot.best_diff.as_str().fg(theme.text),
        ]),
        Line::from(vec![
            "Network Diff".fg(theme.muted),
            "  ".into(),
            app.snapshot.network_difficulty.as_str().fg(theme.text),
        ]),
    ];

    frame.render_widget(
        Paragraph::new(stats)
            .block(panel("Hashrate Stats", theme))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_meter_panels(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let top = Layout::horizontal([
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
        Constraint::Ratio(1, 3),
    ])
    .spacing(1)
    .split(area);

    render_meter_group(
        frame,
        top[0],
        theme,
        "Power",
        &[
            MeterRow::new(
                "Power",
                format!("{:.1} W", app.snapshot.power),
                app.snapshot.power / app.snapshot.max_power.max(1.0),
            ),
            MeterRow::new(
                "Input Voltage",
                format!("{:.2} V", app.snapshot.input_voltage),
                app.snapshot.input_voltage / 5.5,
            ),
            MeterRow::new(
                "ASIC Freq",
                format!("{:.0} MHz", app.snapshot.frequency),
                app.snapshot.frequency / app.snapshot.max_frequency.max(1.0),
            ),
            MeterRow::new(
                "Current",
                format!("{:.2} A", app.snapshot.current_amps),
                app.snapshot.current_amps / 6.0,
            ),
            MeterRow::new(
                "ASIC Voltage",
                format!("{:.2} V", app.snapshot.core_voltage_actual / 1000.0),
                app.snapshot.core_voltage_actual / 1500.0,
            ),
        ],
    );

    render_meter_group(
        frame,
        top[1],
        theme,
        "Heat",
        &[
            MeterRow::new(
                "ASIC Temp",
                format!("{:.0} C", app.snapshot.temp),
                app.snapshot.temp / 90.0,
            ),
            MeterRow::new(
                "VRM Temp",
                format!("{:.0} C", app.snapshot.vr_temp),
                app.snapshot.vr_temp / 100.0,
            ),
            MeterRow::new(
                "Temp Target",
                format!("{:.0} C", app.snapshot.temp_target),
                app.snapshot.temp_target / 90.0,
            ),
            MeterRow::new(
                "Error Rate",
                format!("{:.2} %", app.snapshot.error_percentage),
                app.snapshot.error_percentage / 5.0,
            ),
        ],
    );

    render_meter_group(
        frame,
        top[2],
        theme,
        "Fan",
        &[
            MeterRow::new(
                "Fan Speed",
                format!("{:.1} %", app.snapshot.fan_percent),
                app.snapshot.fan_percent / 100.0,
            ),
            MeterRow::new(
                "Fan RPM",
                format!("{:.0} RPM", app.snapshot.fan_rpm),
                app.snapshot.fan_rpm / 7000.0,
            ),
        ],
    );

}

fn render_status_pane(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let note = vec![
        Line::from(vec![
            "Pool".fg(theme.muted),
            "  ".into(),
            app.snapshot.pool_label().fg(theme.text).bold(),
        ]),
        Line::from(vec![
            "Worker".fg(theme.muted),
            "  ".into(),
            app.snapshot.stratum_user.as_str().fg(theme.text),
        ]),
        Line::from(vec![
            "Latency".fg(theme.muted),
            "  ".into(),
            format!("{:.0} ms", app.snapshot.response_time_ms).fg(theme.accent_soft),
        ]),
        Line::from(vec![
            "WiFi".fg(theme.muted),
            "  ".into(),
            format!("{} | {} dBm", app.snapshot.wifi_status, app.snapshot.wifi_rssi).fg(theme.text),
        ]),
        Line::from(vec![
            "Shares".fg(theme.muted),
            "  ".into(),
            format!(
                "{} accepted / {} rejected / {} stale",
                app.snapshot.shares_accepted,
                app.snapshot.shares_rejected,
                app.snapshot.stale_shares()
            )
            .fg(theme.text),
        ]),
        Line::from(vec![
            "Heap".fg(theme.muted),
            "  ".into(),
            format!("{:.1} MB free", app.snapshot.free_heap_mb).fg(theme.text),
        ]),
        Line::from(""),
        Line::from(app.message.as_str().fg(theme.text)),
    ];
    frame.render_widget(
        Paragraph::new(note)
            .block(panel("Bitaxe Status", theme))
            .wrap(Wrap { trim: true }),
        area,
    );
}

fn render_log_pane(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    frame.render_widget(Clear, area);

    let content_width = area.width.saturating_sub(2) as usize;
    let visible_lines = area.height.saturating_sub(2) as usize;
    let start = app.logs.len().saturating_sub(visible_lines);
    let log_lines: Vec<Line<'_>> = app
        .logs
        .iter()
        .skip(start)
        .map(|line| {
            let clipped = clip_text(line, content_width);
            let color = if line.contains("accept")
                || line.contains("share")
                || line.contains("found block")
                || line.contains("connected")
            {
                theme.ok
            } else if line.contains("pool")
                || line.contains("stratum")
                || line.contains("ws ")
                || line.contains("subscribed")
            {
                theme.info
            } else if line.contains("warn") || line.contains("error") {
                theme.warn
            } else {
                theme.muted
            };
            Line::from(clipped.fg(color))
        })
        .collect();

    frame.render_widget(
        Paragraph::new(log_lines)
            .block(panel("Log", theme))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn render_footer(frame: &mut Frame, area: Rect, app: &App, theme: Theme) {
    let controls = Line::from(vec![
        " q ".fg(theme.bg).bg(theme.accent).bold(),
        " quit ".fg(theme.muted),
        " r ".fg(theme.bg).bg(theme.info).bold(),
        " refresh now ".fg(theme.muted),
        " | ".fg(theme.muted),
        app.connection_detail().fg(theme.text),
    ]);

    frame.render_widget(
        Paragraph::new(controls)
            .block(panel("Controls", theme))
            .alignment(Alignment::Center),
        area,
    );
}

fn panel<'a>(title: &'a str, theme: Theme) -> Block<'a> {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(Style::default().fg(theme.border))
        .style(Style::default().bg(theme.bg));

    if title.is_empty() {
        block
    } else {
        block.title(Line::from(format!(" {} ", title)).fg(theme.muted))
    }
}

fn render_meter_group(
    frame: &mut Frame,
    area: Rect,
    theme: Theme,
    title: &str,
    rows: &[MeterRow],
) {
    frame.render_widget(panel(title, theme), area);

    let inner = Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    };

    let constraints: Vec<Constraint> = rows
        .iter()
        .flat_map(|_| [Constraint::Length(1), Constraint::Length(1)])
        .collect();
    let chunks = Layout::vertical(constraints).split(inner);
    let value_width = meter_value_width(rows, inner.width);

    for (index, row) in rows.iter().enumerate() {
        let base = index * 2;
        let label_row = Layout::horizontal([
            Constraint::Min(8),
            Constraint::Length(value_width),
        ])
        .split(chunks[base]);
        let label_width = label_row[0].width.saturating_sub(1) as usize;
        frame.render_widget(
            Paragraph::new(fit_label(&row.label, label_width).fg(theme.text)),
            label_row[0],
        );
        frame.render_widget(
            Paragraph::new(row.value.as_str().fg(theme.text).bold()).alignment(Alignment::Right),
            label_row[1],
        );

        let gauge = Gauge::default()
            .style(Style::default().bg(theme.bg))
            .gauge_style(Style::default().fg(theme.warn).bg(Color::Rgb(78, 85, 96)))
            .ratio(row.ratio.clamp(0.0, 1.0) as f64)
            .label("");
        frame.render_widget(gauge, chunks[base + 1]);
    }
}

struct MeterRow {
    label: String,
    value: String,
    ratio: f32,
}

impl MeterRow {
    fn new(label: &str, value: String, ratio: f32) -> Self {
        Self {
            label: label.to_string(),
            value,
            ratio,
        }
    }
}

fn fit_label(label: &str, max_chars: usize) -> String {
    if label.chars().count() <= max_chars {
        return label.to_string();
    }

    if max_chars <= 1 {
        return String::new();
    }

    let truncated: String = label.chars().take(max_chars - 1).collect();
    format!("{truncated}…")
}

fn sanitize_log_line(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\u{1b}' {
            if matches!(chars.peek(), Some('[')) {
                chars.next();
                for next in chars.by_ref() {
                    if ('@'..='~').contains(&next) {
                        break;
                    }
                }
            }
            continue;
        }

        if ch == '\r' || ch == '\n' {
            continue;
        }

        if ch.is_control() && ch != '\t' {
            continue;
        }

        if ch == '\t' {
            out.push(' ');
            out.push(' ');
        } else {
            out.push(ch);
        }
    }

    out.trim().to_string()
}

fn clip_text(input: &str, max_chars: usize) -> String {
    if input.chars().count() <= max_chars {
        return input.to_string();
    }

    if max_chars <= 1 {
        return String::new();
    }

    let clipped: String = input.chars().take(max_chars - 1).collect();
    format!("{clipped}…")
}

fn meter_value_width(rows: &[MeterRow], available_width: u16) -> u16 {
    let longest = rows
        .iter()
        .map(|row| row.value.chars().count() as u16)
        .max()
        .unwrap_or(8)
        .saturating_add(1);

    let max_allowed = available_width.saturating_sub(8).max(8);
    longest.min(max_allowed)
}

fn format_metric_value(value: &Value) -> String {
    match value {
        Value::Null => "n/a".to_string(),
        Value::Number(number) => {
            if let Some(v) = number.as_f64() {
                format_large_number(v)
            } else {
                number.to_string()
            }
        }
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn format_large_number(value: f64) -> String {
    if value >= 1_000_000_000_000_000_000.0 {
        format!("{:.2}E", value / 1_000_000_000_000_000_000.0)
    } else if value >= 1_000_000_000_000_000.0 {
        format!("{:.2}P", value / 1_000_000_000_000_000.0)
    } else if value >= 1_000_000_000_000.0 {
        format!("{:.2}T", value / 1_000_000_000_000.0)
    } else if value >= 1_000_000_000.0 {
        format!("{:.2}G", value / 1_000_000_000.0)
    } else if value >= 1_000_000.0 {
        format!("{:.2}M", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.1}k", value / 1_000.0)
    } else {
        format!("{value:.0}")
    }
}

fn compact_session_line(app: &App, max_chars: usize) -> Line<'static> {
    let endpoint = app
        .base_url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let text = format!("{endpoint} | {} | {}", app.status_text(), app.last_update_text());

    Line::from(clip_text(&text, max_chars)).fg(Color::Rgb(255, 204, 128))
}

#[derive(Clone)]
struct Config {
    base_url: String,
}

impl Config {
    fn from_args() -> Self {
        let mut args = env::args().skip(1);
        let first = args.next();

        if let Some(arg) = first.as_deref() {
            if arg == "--help" || arg == "-h" {
                print_help_and_exit();
            }
        }

        let base_url = first
            .or_else(|| env::var("BITAXE_URL").ok())
            .unwrap_or_else(|| "http://bitaxe.local".to_string());

        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
        }
    }

    fn websocket_url(&self) -> String {
        if let Some(rest) = self.base_url.strip_prefix("https://") {
            format!("wss://{rest}/api/ws")
        } else if let Some(rest) = self.base_url.strip_prefix("http://") {
            format!("ws://{rest}/api/ws")
        } else if self.base_url.starts_with("ws://") || self.base_url.starts_with("wss://") {
            format!("{}/api/ws", self.base_url)
        } else {
            format!("ws://{}/api/ws", self.base_url)
        }
    }
}

fn print_help_and_exit() -> ! {
    println!("axetui - Bitaxe Ratatui dashboard");
    println!();
    println!("Usage:");
    println!("  axetui [BITAXE_URL]");
    println!();
    println!("Examples:");
    println!("  axetui http://bitaxe.local");
    println!("  BITAXE_URL=http://192.168.1.77 cargo run");
    println!();
    println!("Keys:");
    println!("  q    quit");
    println!("  r    refresh immediately");
    process::exit(0);
}

#[derive(Clone, Copy)]
struct Theme {
    bg: Color,
    border: Color,
    text: Color,
    muted: Color,
    accent: Color,
    accent_soft: Color,
    ok: Color,
    warn: Color,
    info: Color,
}

impl Default for Theme {
    fn default() -> Self {
        Self {
            bg: Color::Black,
            border: Color::Rgb(46, 66, 87),
            text: Color::Rgb(228, 233, 240),
            muted: Color::Rgb(129, 151, 171),
            accent: Color::Rgb(255, 168, 76),
            accent_soft: Color::Rgb(255, 204, 128),
            ok: Color::Rgb(99, 210, 140),
            warn: Color::Rgb(255, 122, 89),
            info: Color::Rgb(82, 177, 255),
        }
    }
}

struct App {
    base_url: String,
    started_at: Instant,
    last_success: Option<Instant>,
    last_error: Option<String>,
    hashrate_spark: VecDeque<u64>,
    logs: VecDeque<String>,
    snapshot: BitaxeSnapshot,
    message: String,
}

impl App {
    fn new(config: Config) -> Self {
        Self {
            base_url: config.base_url,
            started_at: Instant::now(),
            last_success: None,
            last_error: None,
            hashrate_spark: VecDeque::from(vec![0]),
            logs: VecDeque::from(vec!["waiting for Bitaxe websocket log stream...".to_string()]),
            snapshot: BitaxeSnapshot::placeholder(),
            message: "Connecting to Bitaxe API and waiting for telemetry.".to_string(),
        }
    }

    fn on_tick(&mut self) {
        if self.last_success.is_none() && self.started_at.elapsed() > Duration::from_secs(5) {
            self.message = format!("Still waiting for {} to answer /api/system/info.", self.base_url);
        }
    }

    fn request_refresh(&mut self, control_tx: &Sender<ControlMessage>) {
        match control_tx.send(ControlMessage::Refresh) {
            Ok(()) => {
                self.push_log("manual refresh requested".to_string());
                self.message = "Manual refresh sent to the Bitaxe poller.".to_string();
            }
            Err(_) => {
                self.push_log("warn refresh request failed; poller offline".to_string());
                self.message = "Refresh failed because the poller is no longer running.".to_string();
            }
        }
    }

    fn handle_poll_message(&mut self, message: PollMessage) {
        match message {
            PollMessage::Snapshot(snapshot) => {
                self.last_success = Some(Instant::now());
                self.last_error = None;
                self.message = snapshot.summary_message();

                self.hashrate_spark
                    .push_back(snapshot.hashrate.max(0.0).round() as u64);
                if self.hashrate_spark.len() > HISTORY_CAP {
                    self.hashrate_spark.pop_front();
                }

                self.snapshot = snapshot;
            }
            PollMessage::Error(error) => {
                self.last_error = Some(error.clone());
                self.message = format!("API poll failed: {error}");
                self.push_log(format!("api error: {error}"));
            }
            PollMessage::LogLine(line) => {
                self.push_log(sanitize_log_line(&line));
            }
            PollMessage::LogStatus(status) => {
                self.push_log(sanitize_log_line(&status));
            }
        }
    }

    fn push_log(&mut self, line: String) {
        self.logs.push_back(line);
        while self.logs.len() > 14 {
            self.logs.pop_front();
        }
    }

    fn connection_label(&self) -> &'static str {
        if self.last_error.is_some() {
            "degraded"
        } else if self.last_success.is_some() {
            "connected"
        } else {
            "connecting"
        }
    }

    fn connection_detail(&self) -> String {
        if let Some(error) = &self.last_error {
            format!("last error: {error}")
        } else if let Some(last) = self.last_success {
            format!("last poll {}s ago", last.elapsed().as_secs())
        } else {
            "waiting for first successful poll".to_string()
        }
    }

    fn last_update_text(&self) -> String {
        self.last_success
            .map(|last| format!("updated {}s ago", last.elapsed().as_secs()))
            .unwrap_or_else(|| "no data yet".to_string())
    }

    fn status_text(&self) -> String {
        if self.snapshot.temp > self.snapshot.temp_target + 8.0 {
            "Hot".to_string()
        } else if self.snapshot.is_using_fallback_stratum {
            "Fallback Pool".to_string()
        } else if self.last_error.is_some() {
            "API Error".to_string()
        } else {
            "Mining".to_string()
        }
    }

    fn hashrate_scale_max(&self) -> u64 {
        let peak = self.hashrate_spark.iter().copied().max().unwrap_or(1);
        let padded = (peak as f64 * 1.8).ceil() as u64;
        padded.max(1)
    }
}

enum PollMessage {
    Snapshot(BitaxeSnapshot),
    Error(String),
    LogLine(String),
    LogStatus(String),
}

enum ControlMessage {
    Refresh,
}

#[derive(Clone, Debug)]
struct BitaxeSnapshot {
    power: f32,
    max_power: f32,
    input_voltage: f32,
    current_amps: f32,
    temp: f32,
    vr_temp: f32,
    temp_target: f32,
    hashrate: f32,
    expected_hashrate: f32,
    error_percentage: f32,
    best_diff: String,
    network_difficulty: String,
    shares_accepted: u32,
    shares_rejected: u32,
    share_reasons: Vec<RejectedReason>,
    response_time_ms: f32,
    frequency: f32,
    max_frequency: f32,
    core_voltage_actual: f32,
    fan_percent: f32,
    fan_rpm: f32,
    wifi_status: String,
    wifi_rssi: i32,
    stratum_url: String,
    stratum_user: String,
    is_using_fallback_stratum: bool,
    free_heap_mb: f32,
}

impl BitaxeSnapshot {
    fn placeholder() -> Self {
        Self {
            power: 0.0,
            max_power: 40.0,
            input_voltage: 0.0,
            current_amps: 0.0,
            temp: 0.0,
            vr_temp: 0.0,
            temp_target: 60.0,
            hashrate: 0.0,
            expected_hashrate: 1.0,
            error_percentage: 0.0,
            best_diff: "n/a".to_string(),
            network_difficulty: "n/a".to_string(),
            shares_accepted: 0,
            shares_rejected: 0,
            share_reasons: Vec::new(),
            response_time_ms: 0.0,
            frequency: 0.0,
            max_frequency: 1.0,
            core_voltage_actual: 0.0,
            fan_percent: 0.0,
            fan_rpm: 0.0,
            wifi_status: "unknown".to_string(),
            wifi_rssi: 0,
            stratum_url: String::new(),
            stratum_user: "n/a".to_string(),
            is_using_fallback_stratum: false,
            free_heap_mb: 0.0,
        }
    }

    fn from_api(info: SystemInfo, asic: &AsicInfo) -> Self {
        let max_frequency = asic
            .frequency_options
            .iter()
            .copied()
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(info.frequency);

        Self {
            power: info.power,
            max_power: info.max_power.max(1.0),
            input_voltage: info.voltage / 1000.0,
            current_amps: info.current / 1000.0,
            temp: info.temp.max(0.0),
            vr_temp: info.vr_temp.max(0.0),
            temp_target: info.temptarget.max(1.0),
            hashrate: info.hash_rate.max(0.0),
            expected_hashrate: info.expected_hashrate.max(1.0),
            error_percentage: info.error_percentage.max(0.0),
            best_diff: format_metric_value(&info.best_diff),
            network_difficulty: format_metric_value(&info.network_difficulty),
            shares_accepted: info.shares_accepted,
            shares_rejected: info.shares_rejected,
            share_reasons: info.shares_rejected_reasons,
            response_time_ms: info.response_time.max(0.0),
            frequency: info.frequency.max(0.0),
            max_frequency: max_frequency.max(1.0),
            core_voltage_actual: info.core_voltage_actual.max(0.0),
            fan_percent: info.fanspeed.clamp(0.0, 100.0),
            fan_rpm: info.fanrpm.max(0.0),
            wifi_status: info.wifi_status,
            wifi_rssi: info.wifi_rssi,
            stratum_url: info.stratum_url,
            stratum_user: info.stratum_user,
            is_using_fallback_stratum: info.is_using_fallback_stratum != 0,
            free_heap_mb: info.free_heap as f32 / 1_048_576.0,
        }
    }

    fn stale_shares(&self) -> u32 {
        self.share_reasons
            .iter()
            .find(|reason| reason.message.eq_ignore_ascii_case("stale"))
            .map(|reason| reason.count)
            .unwrap_or(0)
    }

    fn pool_label(&self) -> String {
        if self.stratum_url.is_empty() {
            "n/a".to_string()
        } else if self.is_using_fallback_stratum {
            format!("{} (fallback)", self.stratum_url)
        } else {
            self.stratum_url.clone()
        }
    }

    fn summary_message(&self) -> String {
        if self.temp > self.temp_target + 8.0 {
            format!(
                "ASIC is running hot at {:.1}C, above the {:.0}C target.",
                self.temp, self.temp_target
            )
        } else if self.is_using_fallback_stratum {
            format!(
                "Primary pool unavailable, mining on fallback {}.",
                self.stratum_url
            )
        } else if self.hashrate < self.expected_hashrate * 0.85 {
            format!(
                "Hashrate {:.1} GH/s is below expected {:.1} GH/s.",
                self.hashrate, self.expected_hashrate
            )
        } else {
            format!(
                "Mining normally at {:.1} GH/s with {:.0} ms pool latency.",
                self.hashrate, self.response_time_ms
            )
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct RejectedReason {
    #[serde(default)]
    message: String,
    #[serde(default)]
    count: u32,
}

#[derive(Clone, Debug, Deserialize)]
struct AsicInfo {
    #[serde(rename = "frequencyOptions", default)]
    frequency_options: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct SystemInfo {
    #[serde(default)]
    power: f32,
    #[serde(default)]
    voltage: f32,
    #[serde(default)]
    current: f32,
    #[serde(default)]
    temp: f32,
    #[serde(rename = "vrTemp", default)]
    vr_temp: f32,
    #[serde(rename = "maxPower", default = "default_max_power")]
    max_power: f32,
    #[serde(rename = "hashRate", default)]
    hash_rate: f32,
    #[serde(rename = "expectedHashrate", default)]
    expected_hashrate: f32,
    #[serde(rename = "errorPercentage", default)]
    error_percentage: f32,
    #[serde(rename = "bestDiff", default)]
    best_diff: Value,
    #[serde(rename = "networkDifficulty", default)]
    network_difficulty: Value,
    #[serde(rename = "isUsingFallbackStratum", default)]
    is_using_fallback_stratum: u8,
    #[serde(rename = "coreVoltageActual", default)]
    core_voltage_actual: f32,
    #[serde(default)]
    frequency: f32,
    #[serde(rename = "wifiStatus", default)]
    wifi_status: String,
    #[serde(rename = "wifiRSSI", default)]
    wifi_rssi: i32,
    #[serde(rename = "sharesAccepted", default)]
    shares_accepted: u32,
    #[serde(rename = "sharesRejected", default)]
    shares_rejected: u32,
    #[serde(rename = "sharesRejectedReasons", default)]
    shares_rejected_reasons: Vec<RejectedReason>,
    #[serde(rename = "stratumURL", default)]
    stratum_url: String,
    #[serde(rename = "stratumUser", default)]
    stratum_user: String,
    #[serde(rename = "responseTime", default)]
    response_time: f32,
    #[serde(rename = "temptarget", default = "default_temp_target")]
    temptarget: f32,
    #[serde(default)]
    fanspeed: f32,
    #[serde(default)]
    fanrpm: f32,
    #[serde(rename = "freeHeap", default)]
    free_heap: u64,
}

fn default_max_power() -> f32 {
    40.0
}

fn default_temp_target() -> f32 {
    60.0
}
