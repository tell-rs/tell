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
mod app;
mod component;
mod event;
mod theme;

mod components;

pub use app::App;
