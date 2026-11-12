// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! Configuration types for the Prometheus scrape receiver.

use prometheus_scrape_manager::expfmt;
use prometheus_scrape_manager::relabel::Config as RelabelConfig;
use prometheus_scrape_manager::scrape::{SchedulingMode, ScrapeConfig};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::num::NonZeroUsize;
use std::time::Duration;

const fn default_scrape_interval() -> Duration {
    Duration::from_secs(15)
}

const fn default_scrape_timeout() -> Duration {
    Duration::from_secs(10)
}

const fn default_max_body_bytes() -> usize {
    16 * 1024 * 1024
}

const fn default_max_data_points_per_batch() -> usize {
    expfmt::arrow::DEFAULT_MAX_DATA_POINTS_PER_BATCH
}

fn default_job_name() -> String {
    "prometheus".to_owned()
}

fn default_metrics_path() -> String {
    "/metrics".to_owned()
}

const fn default_true() -> bool {
    true
}

const fn default_concurrent_in_flight() -> usize {
    8
}

const fn default_concurrent_buffered_bodies() -> usize {
    2
}

const fn default_concurrent_decoders() -> usize {
    1
}

/// Target scheduling strategy for [`SchedulingConfig`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulingModeKind {
    /// Scrapes all targets sequentially on a job-wide interval tick.
    #[default]
    Sequential,
    /// Schedules each target at its deterministic `hash(target) % scrape_interval` offset while
    /// running at most one scrape at a time.
    SequentialJittered,
    /// Schedules each target at its deterministic jittered offset with bounded concurrent HTTP
    /// requests, pooled body buffers, and pooled Arrow decoders.
    Concurrent,
}

/// Scheduler and scratch-pool bounds for `prometheus_scrape_manager::scrape::ScrapeManager`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SchedulingConfig {
    /// Scheduling cadence and concurrency mode.
    #[serde(default)]
    pub mode: SchedulingModeKind,

    /// Maximum concurrent in-flight HTTP scrape requests when `mode = "concurrent"`.
    #[serde(default = "default_concurrent_in_flight")]
    pub max_in_flight: usize,

    /// Maximum HTTP response bodies buffered simultaneously when `mode = "concurrent"`.
    #[serde(default = "default_concurrent_buffered_bodies")]
    pub max_buffered_bodies: usize,

    /// Maximum reusable `ArrowDecoder` instances pooled simultaneously when `mode = "concurrent"`.
    #[serde(default = "default_concurrent_decoders")]
    pub max_decoders: usize,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            mode: SchedulingModeKind::Sequential,
            max_in_flight: default_concurrent_in_flight(),
            max_buffered_bodies: default_concurrent_buffered_bodies(),
            max_decoders: default_concurrent_decoders(),
        }
    }
}

impl SchedulingConfig {
    /// Converts [`SchedulingConfig`] into a validated [`SchedulingMode`].
    pub(crate) fn to_scheduling_mode(
        self,
    ) -> Result<SchedulingMode, otel_arrow_dfe_config::error::Error> {
        match self.mode {
            SchedulingModeKind::Sequential => Ok(SchedulingMode::sequential()),
            SchedulingModeKind::SequentialJittered => Ok(SchedulingMode::sequential_jittered()),
            SchedulingModeKind::Concurrent => {
                let max_in_flight = NonZeroUsize::new(self.max_in_flight).ok_or_else(|| {
                    otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: "scheduling.max_in_flight must be greater than zero".to_owned(),
                    }
                })?;
                let max_buffered_bodies =
                    NonZeroUsize::new(self.max_buffered_bodies).ok_or_else(|| {
                        otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                            error: "scheduling.max_buffered_bodies must be greater than zero"
                                .to_owned(),
                        }
                    })?;
                let max_decoders = NonZeroUsize::new(self.max_decoders).ok_or_else(|| {
                    otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: "scheduling.max_decoders must be greater than zero".to_owned(),
                    }
                })?;
                if max_buffered_bodies > max_in_flight {
                    return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: format!(
                            "scheduling.max_buffered_bodies ({max_buffered_bodies}) must not exceed scheduling.max_in_flight ({max_in_flight})"
                        ),
                    });
                }
                if max_decoders > max_in_flight {
                    return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                        error: format!(
                            "scheduling.max_decoders ({max_decoders}) must not exceed scheduling.max_in_flight ({max_in_flight})"
                        ),
                    });
                }
                Ok(SchedulingMode::concurrent_with_pools(
                    max_in_flight,
                    max_buffered_bodies,
                    max_decoders,
                ))
            }
        }
    }
}

/// Configuration for the Prometheus scrape receiver (`urn:otel:receiver:prometheus`).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Logical job name associated with scraped targets.
    #[serde(default = "default_job_name")]
    pub job_name: String,

    /// Interval between scrape ticks.
    #[serde(default = "default_scrape_interval", with = "humantime_serde")]
    pub scrape_interval: Duration,

    /// Per-target HTTP request timeout. Must not exceed `scrape_interval`.
    #[serde(default = "default_scrape_timeout", with = "humantime_serde")]
    pub scrape_timeout: Duration,

    /// Optional delay before the first scrape tick.
    #[serde(default, with = "humantime_serde")]
    pub initial_delay: Duration,

    /// Default HTTP path appended to bare `host:port` targets.
    #[serde(default = "default_metrics_path")]
    pub metrics_path: String,

    /// Whether explicit sample timestamps in the payload are preserved.
    #[serde(default = "default_true")]
    pub honor_timestamps: bool,

    /// Whether sample labels colliding with target labels take precedence (`true`) or are renamed
    /// with an `exported_` prefix (`false`).
    #[serde(default)]
    pub honor_labels: bool,

    /// Whether per-target active series are tracked across scrapes to emit
    /// `FLAG_NO_RECORDED_VALUE` staleness markers when series disappear, scrapes fail, or targets
    /// are removed.
    #[serde(default)]
    pub track_staleness: bool,

    /// Whether to advertise OpenMetrics 1.0 in the HTTP `Accept` header.
    #[serde(default = "default_true")]
    pub enable_open_metrics: bool,

    /// Whether to negotiate `Accept-Encoding: gzip` on target HTTP scrapes.
    #[serde(default = "default_true")]
    pub enable_compression: bool,

    /// Deterministic seed mixed into per-target FNV-1a phase offset hashing.
    #[serde(default)]
    pub jitter_seed: u64,

    /// Scrape scheduler cadence, concurrency, and buffer pool settings.
    #[serde(default)]
    pub scheduling: SchedulingConfig,

    /// Maximum HTTP response body size in bytes per target scrape (default 16 MiB).
    #[serde(default = "default_max_body_bytes")]
    pub max_body_bytes: usize,

    /// Maximum number of data points per emitted OTAP batch (default `16384`; `0` means unchunked).
    #[serde(default = "default_max_data_points_per_batch")]
    pub max_data_points_per_batch: usize,

    /// Prometheus target relabeling rules (`relabel_configs`) applied during discovery.
    #[serde(default)]
    pub relabel_configs: Vec<RelabelConfig>,

    /// Prometheus sample relabeling rules (`metric_relabel_configs`) applied after decoding.
    #[serde(default)]
    pub metric_relabel_configs: Vec<RelabelConfig>,

    /// Static scrape target endpoints (`host:port` or full `http(s)://...` URLs).
    #[serde(default)]
    pub targets: Vec<String>,

    /// Headless DNS service endpoints (`host:port`) resolved to pod/host IPs on each scrape tick.
    #[serde(default)]
    pub dns_sd_targets: Vec<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            job_name: default_job_name(),
            scrape_interval: default_scrape_interval(),
            scrape_timeout: default_scrape_timeout(),
            initial_delay: Duration::ZERO,
            metrics_path: default_metrics_path(),
            honor_timestamps: true,
            honor_labels: false,
            track_staleness: false,
            enable_open_metrics: true,
            enable_compression: true,
            jitter_seed: 0,
            scheduling: SchedulingConfig::default(),
            max_body_bytes: default_max_body_bytes(),
            max_data_points_per_batch: default_max_data_points_per_batch(),
            relabel_configs: Vec::new(),
            metric_relabel_configs: Vec::new(),
            targets: Vec::new(),
            dns_sd_targets: Vec::new(),
        }
    }
}

impl Config {
    /// Builds a validated [`ScrapeConfig`] for `prometheus_scrape_manager`.
    pub(crate) fn to_scrape_config(
        &self,
    ) -> Result<ScrapeConfig, otel_arrow_dfe_config::error::Error> {
        let scheduling = self.scheduling.to_scheduling_mode()?;
        let mut scrape_cfg = ScrapeConfig::new(self.job_name.clone())
            .with_scrape_interval(self.scrape_interval)
            .with_scrape_timeout(self.scrape_timeout)
            .with_initial_delay(self.initial_delay)
            .with_jitter_seed(self.jitter_seed)
            .with_scheduling(scheduling)
            .with_enable_open_metrics(self.enable_open_metrics)
            .with_enable_compression(self.enable_compression)
            .with_honor_labels(self.honor_labels)
            .with_track_staleness(self.track_staleness)
            .with_max_body_bytes(self.max_body_bytes)
            .with_max_data_points_per_batch(self.max_data_points_per_batch)
            .with_relabel_configs(self.relabel_configs.clone())
            .with_metric_relabel_configs(self.metric_relabel_configs.clone());
        scrape_cfg.honor_timestamps = self.honor_timestamps;
        scrape_cfg.escaping_scheme = expfmt::format::EscapingScheme::AllowUtf8;
        scrape_cfg.validate().map_err(|err| {
            otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: err.to_string(),
            }
        })?;
        Ok(scrape_cfg)
    }
}

/// Normalizes a static target (`host:port` or full URL) into an HTTP(S) URL.
pub(crate) fn normalize_target_url(
    raw_target: &str,
    default_metrics_path: &str,
) -> Result<Url, otel_arrow_dfe_config::error::Error> {
    let trimmed = raw_target.trim();
    if trimmed.is_empty() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "target endpoint must not be empty".to_owned(),
        });
    }

    let candidate = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_owned()
    } else {
        format!("http://{trimmed}{default_metrics_path}")
    };

    let parsed = Url::parse(&candidate).map_err(|err| {
        otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: format!("invalid scrape target `{trimmed}`: {err}"),
        }
    })?;

    if parsed.scheme() != "http" && parsed.scheme() != "https" {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: format!(
                "unsupported scheme `{}` in scrape target `{trimmed}`",
                parsed.scheme()
            ),
        });
    }
    if parsed.host_str().is_none() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: format!("scrape target `{trimmed}` is missing a host"),
        });
    }

    Ok(parsed)
}

/// Validates a `prometheus_receiver` configuration.
pub(crate) fn validate_config(config: &Config) -> Result<(), otel_arrow_dfe_config::error::Error> {
    if config.job_name.trim().is_empty() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "job_name must not be empty".to_owned(),
        });
    }
    if config.scrape_interval.is_zero() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "scrape_interval must be greater than zero".to_owned(),
        });
    }
    if config.scrape_timeout.is_zero() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "scrape_timeout must be greater than zero".to_owned(),
        });
    }
    if config.scrape_timeout > config.scrape_interval {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "scrape_timeout must not exceed scrape_interval".to_owned(),
        });
    }
    if config.max_body_bytes == 0 {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "max_body_bytes must be greater than zero".to_owned(),
        });
    }
    if !config.metrics_path.starts_with('/') {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "metrics_path must start with '/'".to_owned(),
        });
    }
    if config.targets.is_empty() && config.dns_sd_targets.is_empty() {
        return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: "at least one entry in targets or dns_sd_targets must be configured".to_owned(),
        });
    }
    for target in &config.targets {
        let _ = normalize_target_url(target, &config.metrics_path)?;
    }
    for dns_target in &config.dns_sd_targets {
        let trimmed = dns_target.trim();
        if trimmed.is_empty() || !trimmed.contains(':') {
            return Err(otel_arrow_dfe_config::error::Error::InvalidUserConfig {
                error: format!("dns_sd_targets entry `{trimmed}` must be in `host:port` format"),
            });
        }
    }
    let _ = config.to_scrape_config()?;
    Ok(())
}
