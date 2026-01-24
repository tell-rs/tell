//! TUI module for Tell interactive mode.
//!
//! Provides an interactive terminal interface with:
//! - `/init` wizard for setup
//! - `/sources`, `/sinks`, `/plugins` management
//! - `/tail` live data view
//! - `/query` SQL interface

// Allow dead code for scaffolded TUI components not yet wired up
#![allow(dead_code)]

mod action;
mod api;
mod app;
mod commands;
mod component;
mod event;
#[cfg(unix)]
mod tap;
mod theme;
mod ui;

mod components;

pub use app::App;
