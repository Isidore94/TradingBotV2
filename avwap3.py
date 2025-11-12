#!/usr/bin/env python3
"""Compatibility wrapper for the combined AVWAP orchestration."""
from __future__ import annotations

import logging

import combined_avwap_runner


def run_once() -> None:
    logging.info("Delegating to combined_avwap_runner.run_once()")
    combined_avwap_runner.run_once()


if __name__ == "__main__":
    combined_avwap_runner.main_loop()
