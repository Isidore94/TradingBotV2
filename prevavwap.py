#!/usr/bin/env python3
"""Compatibility wrapper for previous-anchor AVWAP runner.

The dedicated previous-earnings workflow has been folded into
:mod:`combined_avwap_runner`.  This stub remains so existing automation can
continue to invoke ``prevavwap.py`` without change.
"""
from __future__ import annotations

import logging

import combined_avwap_runner


def run_once() -> None:
    logging.info("Delegating to combined_avwap_runner.run_once()")
    combined_avwap_runner.run_once()


if __name__ == "__main__":
    combined_avwap_runner.main_loop()
