"""
╔══════════════════════════════════════════════════════════════════════════════╗
║  DIGITS BOT  v3  —  1HZ25V  —  Distribution Thinking                        ║
║                                                                              ║
║  PHILOSOPHY                                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  "Win consistently with controlled risk."                                    ║
║                                                                              ║
║  This bot does NOT bet blindly on Even or Odd.                               ║
║  It asks one question per tick:                                              ║
║                                                                              ║
║    "Is the recent digit parity distribution biased enough that              ║
║     betting on the dominant parity has positive expected value?"            ║
║                                                                              ║
║  If yes, it sizes the bet and places it. If no, it waits.                  ║
║                                                                              ║
║  CONTRACT                                                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Symbol   : 1HZ25V (Volatility 25 Index)                                    ║
║  Type     : DIGITODD / DIGITEVEN (bet on last digit parity at expiry)       ║
║  Duration : 5 ticks                                                          ║
║  Payout   : ~95% (near-even payout, so edge must come from bias)            ║
║                                                                              ║
║  CHANGES FROM v2                                                             ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  ✅ LiveRecalibrator — replaces one-shot SigmaCalibrator                    ║
║     - Initial calibration at warmup (tick 60), same as before               ║
║     - Re-calibrates every RECAL_INTERVAL ticks (default 300)               ║
║     - Uses rolling RECAL_WINDOW of recent σ samples (default 300)          ║
║     - Thresholds drift with the market — no more permanent ACTIVE blocks    ║
║     - Logs every recalibration event with before/after drift                ║
║     - Env overrides: RECAL_INTERVAL, RECAL_WINDOW, CALM_FACTOR,            ║
║                      NORMAL_FACTOR                                          ║
║                                                                              ║
║  DISTRIBUTION MODEL                                                          ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Last digit of 1HZ25V price cycles 0-9. In a uniform distribution,         ║
║  P(even) = P(odd) = 0.50 exactly.                                           ║
║                                                                              ║
║  We track short-term parity bias with an EWMA:                              ║
║    bias = EWMA of recent outcomes (1=even, 0=odd)                           ║
║    If bias drifts significantly from 0.5, we bet the dominant side.        ║
║                                                                              ║
║  ONLINE ESTIMATOR (learns while running)                                    ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Primary signal  : EWMA parity bias (α = 0.15, faster than EXPIRYRANGE)    ║
║  Confirmation    : Raw win rate over last N_WINDOW outcomes                 ║
║  Volatility gate : σ of tick moves — we avoid betting in ACTIVE regimes    ║
║    (high volatility = erratic digit endings, less predictable)              ║
║                                                                              ║
║  LIVE RECALIBRATING REGIME THRESHOLDS  (new in v3)                         ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  LiveRecalibrator maintains a rolling window of σ samples.                 ║
║  At warmup and every RECAL_INTERVAL ticks thereafter it sets:              ║
║    SIGMA_CALM   = median(window σ) × CALM_FACTOR   (default 0.85)         ║
║    SIGMA_NORMAL = median(window σ) × NORMAL_FACTOR (default 1.25)         ║
║  Thresholds adapt continuously — quiet sessions unlock trading,            ║
║  volatile sessions tighten the gate automatically.                         ║
║                                                                              ║
║  ENTRY GATE (all must pass)                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  1. Estimator warmed up (≥ MIN_WARMUP_TICKS) + thresholds calibrated       ║
║  2. Regime is CALM or NORMAL (block ACTIVE)                                 ║
║  3. Bias signal strong enough: |bias - 0.5| ≥ MIN_BIAS_EDGE                ║
║  4. EV > MIN_EV (default 0.005 — tight edge, near-even payout)             ║
║  5. Kelly fraction f > 0                                                    ║
║  6. No trade in progress                                                    ║
║  7. Cooldown since last trade                                               ║
║                                                                              ║
║  MARTINGALE                                                                  ║
║  ──────────────────────────────────────────────────────────────────────────  ║
║  Kicks in after 2 consecutive losses. Factor: 1.75×                        ║
║  Base stake: $0.35. Max steps before forced reset: 4                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import math
import os
import sys
import time
import traceback
import threading
from collections import deque
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Optional

try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed, ConnectionClosedError, ConnectionClosedOK,
    )
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def _env(key, default):
    v = os.environ.get(key)
    if v is None:
        return default
    if isinstance(default, bool):
        return v.lower() in ("1", "true", "yes")
    if isinstance(default, float):
        return float(v)
    if isinstance(default, int):
        return int(v)
    return v


# Deriv connection
API_TOKEN  = _env("DERIV_API_TOKEN", "3nMoTkW49VHJqhH")
APP_ID     = _env("DERIV_APP_ID",    1089)
SYMBOL     = _env("SYMBOL",          "R_100")

# Contract
DURATION_TICKS = 5       # 5-tick expiry

# Digit bias estimator
BIAS_ALPHA       = _env("BIAS_ALPHA",       0.15)   # EWMA α — faster adaptation
N_WINDOW         = _env("N_WINDOW",         30)     # rolling window for raw win rate
MIN_WARMUP_TICKS = _env("MIN_WARMUP_TICKS", 60)     # ticks before trading starts

# Volatility gating — thresholds are SET DYNAMICALLY after warmup by LiveRecalibrator.
# These factors are multiplied against the median warmup σ to derive CALM / NORMAL bounds.
EWMA_ALPHA      = _env("EWMA_ALPHA",      0.05)
CALM_FACTOR     = _env("CALM_FACTOR",     0.85)   # median σ × 0.85 = CALM ceiling
NORMAL_FACTOR   = _env("NORMAL_FACTOR",   1.25)   # median σ × 1.25 = NORMAL ceiling
RECAL_INTERVAL  = _env("RECAL_INTERVAL",  300)    # recalibrate every N ticks
RECAL_WINDOW    = _env("RECAL_WINDOW",    300)    # rolling window size for σ samples
# Actual SIGMA_CALM / SIGMA_NORMAL are written by LiveRecalibrator at runtime.
# Sentinel values — will be overwritten before any trade is evaluated.
SIGMA_CALM   = float("inf")
SIGMA_NORMAL = float("inf")

# Entry gate
MIN_BIAS_EDGE = _env("MIN_BIAS_EDGE", 0.04)   # |bias - 0.50| must exceed this
MIN_EV        = _env("MIN_EV",        0.005)  # EV per dollar staked
ALLOW_REGIMES = {"CALM", "NORMAL"}

# Kelly sizing
KELLY_FRACTION = _env("KELLY_FRACTION", 0.20)
KELLY_MAX_PCT  = _env("KELLY_MAX_PCT",  0.04)

# Payout for DIGITODD / DIGITEVEN on 1HZ25V
# Deriv pays ~95 cents per $1 stake on win (i.e. ROI ≈ -0.05 on a fair coin).
# Edge must overcome this. Seeded low; bot learns actual ROI live.
SEED_ROI = _env("SEED_ROI", 0.90)  # will be updated live from proposals

# ── Martingale config ────────────────────────────────────────────────────────
MARTINGALE_BASE_STAKE = _env("MARTINGALE_BASE_STAKE", 0.35)
MARTINGALE_FACTOR     = _env("MARTINGALE_FACTOR",     1.15)
MARTINGALE_MAX_STEPS  = _env("MARTINGALE_MAX_STEPS",  4)
MIN_STAKE             = MARTINGALE_BASE_STAKE

# Session limits
TARGET_PROFIT  = _env("TARGET_PROFIT",  50.0)
STOP_LOSS      = _env("STOP_LOSS",      20.0)
TRADE_COOLDOWN = _env("TRADE_COOLDOWN", 15)     # seconds — 5-tick at ~1Hz = ~5s per trade

# Resilience
RECONNECT_MIN  = _env("RECONNECT_MIN",  2)
RECONNECT_MAX  = _env("RECONNECT_MAX",  60)
WS_PING        = _env("WS_PING",        30)
BUY_RETRIES    = _env("BUY_RETRIES",    8)
LOCK_TIMEOUT   = _env("LOCK_TIMEOUT",   60)    # shorter — 5-tick settles fast

# Settlement poller
POLL_INTERVAL  = _env("POLL_INTERVAL",  3)     # more aggressive for fast contracts

PORT = _env("PORT", 8081)   # different port from EXPIRYRANGE bot


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def info(msg):  print(f"[{_ts()}] [INFO ] {msg}", flush=True)
def warn(msg):  print(f"[{_ts()}] [WARN ] {msg}", flush=True)
def err(msg):   print(f"[{_ts()}] [ERROR] {msg}", flush=True)
def tlog(msg):  print(f"[{_ts()}] [TRADE] {msg}", flush=True)
def slog(msg):  print(f"[{_ts()}] [SETTL] {msg}", flush=True)
def mlog(msg):  print(f"[{_ts()}] [MARTI] {msg}", flush=True)
def dlog(msg):  print(f"[{_ts()}] [DIGIT] {msg}", flush=True)   # digit-specific
def jlog(obj):  print(json.dumps(obj), flush=True)


# ─────────────────────────────────────────────────────────────────────────────
# LIVE RECALIBRATOR  — continuous adaptive regime thresholds
# ─────────────────────────────────────────────────────────────────────────────

class LiveRecalibrator:
    """
    Keeps SIGMA_CALM and SIGMA_NORMAL in sync with the market as volatility
    conditions evolve throughout the trading day.

    How it works
    ────────────
    Maintains a rolling deque of the most recent RECAL_WINDOW |Δprice| samples.
    Every RECAL_INTERVAL ticks (after initial warmup), it:
      1. Takes the median of the rolling window  (robust to spikes)
      2. Sets SIGMA_CALM   = median × CALM_FACTOR
             SIGMA_NORMAL  = median × NORMAL_FACTOR
      3. Writes the values into module globals so DigitBiasEstimator.regime()
         picks them up on the very next tick — no restart required.

    The rolling window naturally de-weights old market conditions. When a calm
    session turns choppy the NORMAL ceiling rises, allowing trades to continue
    when appropriate. When volatility cools, the gate tightens again.

    Calibration history is kept so you can see threshold drift in the logs.
    """

    def __init__(self):
        self._samples: deque = deque(maxlen=int(RECAL_WINDOW))
        self._tick_count = 0
        self.calibrated   = False
        self.sigma_calm   = float("inf")
        self.sigma_normal = float("inf")
        self._history: list = []

    def record(self, move: float):
        """Feed every |Δprice| here. Handles initial + periodic recalibration."""
        if move > 0:
            self._samples.append(move)
        self._tick_count += 1

        if not self.calibrated and self._tick_count >= MIN_WARMUP_TICKS:
            self._calibrate(reason="initial-warmup")
        elif self.calibrated and self._tick_count % int(RECAL_INTERVAL) == 0:
            self._calibrate(reason=f"scheduled (tick {self._tick_count})")

    def _calibrate(self, reason: str = ""):
        if len(self._samples) < 10:
            warn("LiveRecalibrator: <10 samples — skipping")
            return

        sorted_s      = sorted(self._samples)
        n             = len(sorted_s)
        median_s      = sorted_s[n // 2]
        new_calm      = round(median_s * CALM_FACTOR,   6)
        new_normal    = round(median_s * NORMAL_FACTOR, 6)

        prev_calm    = self.sigma_calm
        prev_normal  = self.sigma_normal
        drift_calm   = abs(new_calm   - prev_calm)   if self.calibrated else 0.0
        drift_normal = abs(new_normal - prev_normal) if self.calibrated else 0.0

        self.sigma_calm   = new_calm
        self.sigma_normal = new_normal
        self.calibrated   = True

        global SIGMA_CALM, SIGMA_NORMAL
        SIGMA_CALM   = self.sigma_calm
        SIGMA_NORMAL = self.sigma_normal

        self._history.append({
            "tick":         self._tick_count,
            "reason":       reason,
            "samples":      n,
            "median_sigma": round(median_s, 6),
            "sigma_calm":   new_calm,
            "sigma_normal": new_normal,
            "drift_calm":   round(drift_calm,   6),
            "drift_normal": round(drift_normal, 6),
        })

        info("─" * 62)
        info(f"[DIGIT][RECAL] {reason}")
        info(f"  Tick          : {self._tick_count}")
        info(f"  Samples       : {n}  (window={int(RECAL_WINDOW)})")
        info(f"  Median σ      : {median_s:.6f}")
        info(f"  SIGMA_CALM    : {new_calm:.6f}  (×{CALM_FACTOR})"
             + (f"  Δ={drift_calm:+.6f}" if len(self._history) > 1 else ""))
        info(f"  SIGMA_NORMAL  : {new_normal:.6f}  (×{NORMAL_FACTOR})"
             + (f"  Δ={drift_normal:+.6f}" if len(self._history) > 1 else ""))
        info("─" * 62)

    def summary(self) -> str:
        if not self._history:
            return "No calibrations yet"
        last = self._history[-1]
        return (f"recals={len(self._history)}  last_tick={last['tick']}  "
                f"CALM={self.sigma_calm:.5f}  NORMAL={self.sigma_normal:.5f}")


# ─────────────────────────────────────────────────────────────────────────────
# DIGIT PARITY BIAS ESTIMATOR
# ─────────────────────────────────────────────────────────────────────────────

class DigitBiasEstimator:
    """
    Learns the parity distribution of the last digit of 1HZ25V prices.

    Two complementary signals:

    1. EWMA bias  — exponentially weighted mean of recent parity outcomes
                    (1 = even digit, 0 = odd digit). Fast α = 0.15 so it
                    adapts quickly to short-term runs.

    2. Raw win rate — simple proportion of even outcomes in last N_WINDOW ticks.
                     Used as confirmation / regime filter.

    Volatility σ — EWMA of |Δprice| per tick, used to gate out ACTIVE regimes
                   where digit endings are most erratic.

    Trade direction: whichever parity the bias favours.
    P(win) estimate: the bias itself (e.g. 0.57 → bet even, P(win) ≈ 0.57).
    EV per dollar: P(win) × ROI - (1 - P(win))
                   where ROI is tracked live from Deriv proposals.
    """

    def __init__(self):
        self.n              = 0
        self.last_price: Optional[float] = None

        # Continuous adaptive regime calibrator (replaces one-shot SigmaCalibrator)
        self.calibrator = LiveRecalibrator()

        # Digit bias signal
        self.ema_bias: Optional[float]  = None   # EWMA of even outcomes
        self.raw_window: deque          = deque(maxlen=int(N_WINDOW))

        # Volatility signal (mirrors EXPIRYRANGE bot)
        self.ema_move: Optional[float]  = None
        self.ema_sq:   Optional[float]  = None

        # Live ROI tracker (updated from Deriv proposals)
        self.ema_roi    = SEED_ROI
        self.roi_n      = 0

        # Outcome tracking
        self.total_trades = 0
        self.total_wins   = 0
        self.session_pnl  = 0.0

        self.regime_stats = {
            "CALM":   {"trades": 0, "wins": 0},
            "NORMAL": {"trades": 0, "wins": 0},
            "ACTIVE": {"trades": 0, "wins": 0},
        }

    def ingest(self, price: float):
        """
        Called on every tick.
        Updates:
          - digit parity EWMA
          - raw parity window
          - volatility EWMA
          - sigma calibrator (during warmup)
        """
        self.n += 1

        # ── Last digit parity ────────────────────────────────────────────────
        # Extract last digit: convert to string, take final character
        price_str   = f"{price:.5f}".rstrip('0') or "0"
        last_digit  = int(price_str[-1]) if price_str[-1].isdigit() else 0
        is_even     = 1 if last_digit % 2 == 0 else 0

        self.raw_window.append(is_even)

        if self.ema_bias is None:
            self.ema_bias = float(is_even)
        else:
            self.ema_bias = BIAS_ALPHA * is_even + (1 - BIAS_ALPHA) * self.ema_bias

        # ── Volatility ───────────────────────────────────────────────────────
        if self.last_price is not None:
            move = abs(price - self.last_price)
            if self.ema_move is None:
                self.ema_move = move
                self.ema_sq   = move * move
            else:
                self.ema_move = EWMA_ALPHA * move      + (1 - EWMA_ALPHA) * self.ema_move
                self.ema_sq   = EWMA_ALPHA * move ** 2 + (1 - EWMA_ALPHA) * self.ema_sq

            # LiveRecalibrator handles initial calibration + periodic recalibration
            self.calibrator.record(move)

        self.last_price = price

        # n tracks all ticks, calibrator tracks its own tick count internally

    def record_roi(self, ask: float, payout: float):
        if ask > 0:
            roi = payout / ask   # e.g. 0.95 for near-even payout
            self.roi_n   += 1
            self.ema_roi  = 0.10 * roi + 0.90 * self.ema_roi

    def record_outcome(self, won: bool, profit: float):
        regime = self.regime()
        self.total_trades += 1
        self.session_pnl  += profit
        if won:
            self.total_wins += 1
        if regime in self.regime_stats:
            self.regime_stats[regime]["trades"] += 1
            if won:
                self.regime_stats[regime]["wins"] += 1

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def sigma(self) -> float:
        return self.ema_move if self.ema_move is not None else 0.0

    def regime(self) -> str:
        s = self.sigma
        if s < SIGMA_CALM:   return "CALM"
        if s < SIGMA_NORMAL: return "NORMAL"
        return "ACTIVE"

    @property
    def bias(self) -> float:
        """EWMA estimate of P(even digit). 0.5 = no bias."""
        return self.ema_bias if self.ema_bias is not None else 0.5

    @property
    def raw_even_rate(self) -> float:
        """Raw proportion of even outcomes in the last N_WINDOW ticks."""
        if not self.raw_window:
            return 0.5
        return sum(self.raw_window) / len(self.raw_window)

    @property
    def bias_edge(self) -> float:
        """Distance of bias from 0.5 — how strong the signal is."""
        return abs(self.bias - 0.5)

    def trade_direction(self) -> str:
        """Which parity to bet based on current bias."""
        return "EVEN" if self.bias >= 0.5 else "ODD"

    def p_win(self) -> float:
        """
        Estimated P(win) = the bias toward the dominant parity.
        e.g. bias=0.57 → bet EVEN → P(win)=0.57
             bias=0.43 → bet ODD  → P(win)=0.57 (= 1 - 0.43)
        """
        b = self.bias
        return b if b >= 0.5 else (1.0 - b)

    def ev_per_dollar(self) -> float:
        """
        EV = P(win) × ROI - (1 - P(win))
        ROI here is the win_payout/stake ratio (e.g. 0.95 means
        $1 stake returns $0.95 profit on a win).
        """
        pw  = self.p_win()
        roi = self.ema_roi
        return pw * roi - (1.0 - pw)

    def ready(self) -> bool:
        return (self.n >= MIN_WARMUP_TICKS
                and self.ema_bias is not None
                and self.calibrator.calibrated)

    def status(self) -> dict:
        pw   = self.p_win()
        ev   = self.ev_per_dollar()
        reg  = self.regime()
        tot  = self.total_trades
        wr   = self.total_wins / tot if tot > 0 else 0.0
        return {
            "ticks":         self.n,
            "sigma":         round(self.sigma, 6),
            "sigma_calm":    round(SIGMA_CALM, 5),
            "sigma_normal":  round(SIGMA_NORMAL, 5),
            "calibrated":    self.calibrator.calibrated,
            "recals":        len(self.calibrator._history),
            "regime":        reg,
            "ema_bias":      round(self.bias, 6),
            "raw_even_rate": round(self.raw_even_rate, 4),
            "bias_edge":     round(self.bias_edge, 6),
            "direction":     self.trade_direction(),
            "p_win":         round(pw, 6),
            "ema_roi":       round(self.ema_roi, 4),
            "ev_per_$":      round(ev, 6),
            "ready":         self.ready(),
            "trades":        tot,
            "wins":          self.total_wins,
            "wr":            round(wr, 4),
            "session_pnl":   round(self.session_pnl, 4),
            "regime_stats":  self.regime_stats,
        }

    def log_status(self):
        s = self.status()
        dlog(f"σ={s['sigma']:.5f}  regime={s['regime']}  "
             f"bias={s['ema_bias']:.4f}  raw_even={s['raw_even_rate']:.3f}  "
             f"edge={s['bias_edge']:.4f}  dir={s['direction']}  "
             f"P(win)={s['p_win']:.4f}  EV={s['ev_per_$']:+.4f}  "
             f"roi={s['ema_roi']:.3f}  trades={s['trades']}  WR={s['wr']*100:.1f}%")


# ─────────────────────────────────────────────────────────────────────────────
# MARTINGALE STAKE MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class MartingaleManager:
    """
    1.75× martingale kicks in after 2 consecutive losses.

    Step 0  → base stake ($0.35)  [loss 1 stays here]
    Step 1  → $0.35 × 1.75¹ ≈ $0.61  (activates on loss 2)
    Step 2  → $0.35 × 1.75² ≈ $1.07
    Step 3  → $0.35 × 1.75³ ≈ $1.87
    Step 4  → $0.35 × 1.75⁴ ≈ $3.27

    Any win resets to step 0. After max steps, force reset (risk cap).
    Kelly is overridden while step > 0.
    """

    def __init__(self):
        self.step = 0
        self.consecutive_losses = 0

    def compute_stake(self, kelly_stake: float) -> float:
        if self.step == 0:
            return max(MARTINGALE_BASE_STAKE, kelly_stake)
        else:
            raw   = MARTINGALE_BASE_STAKE * (MARTINGALE_FACTOR ** self.step)
            stake = math.floor(raw * 100) / 100
            return max(MARTINGALE_BASE_STAKE, stake)

    def record_win(self):
        if self.step > 0:
            mlog(f"WIN at step {self.step} → RESET to step 0")
        self.step = 0
        self.consecutive_losses = 0

    def record_loss(self):
        self.consecutive_losses += 1
        if self.consecutive_losses < 2:
            mlog(f"LOSS #{self.consecutive_losses} → step {self.step} "
                 f"(martingale not yet active)")
            return
        if self.step < MARTINGALE_MAX_STEPS:
            self.step += 1
            next_stake = MARTINGALE_BASE_STAKE * (MARTINGALE_FACTOR ** self.step)
            mlog(f"LOSS #{self.consecutive_losses} → step {self.step}  "
                 f"next_stake≈${next_stake:.2f}")
        else:
            mlog(f"LOSS at MAX step {self.step} — FORCE RESET (risk cap)")
            self.step = 0
            self.consecutive_losses = 0

    @property
    def is_recovering(self) -> bool:
        return self.step > 0

    def describe(self) -> str:
        if self.step == 0:
            return f"step=0 (base)  streak={self.consecutive_losses}"
        stake = MARTINGALE_BASE_STAKE * (MARTINGALE_FACTOR ** self.step)
        return (f"step={self.step}/{MARTINGALE_MAX_STEPS}  "
                f"streak={self.consecutive_losses}  "
                f"stake≈${stake:.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# KELLY STAKE SIZER
# ─────────────────────────────────────────────────────────────────────────────

class KellySizer:
    """
    Kelly for near-even payout bets.
    f* = (p × b - (1-p)) / b   where b = ROI (win_payout / stake).
    """

    def compute(self, p_win: float, roi: float,
                balance: float) -> tuple:
        q = 1.0 - p_win
        b = roi
        if b <= 0:
            return MIN_STAKE, 0.0, False

        f_full = (p_win * b - q) / b
        if f_full <= 0:
            return MIN_STAKE, f_full, False

        f_frac    = KELLY_FRACTION * f_full
        raw_stake = f_frac * balance
        capped    = min(raw_stake, balance * KELLY_MAX_PCT)
        floored   = max(MIN_STAKE, capped)
        stake     = math.floor(floored * 100) / 100
        return stake, f_full, True

    def explain(self, p_win: float, roi: float, balance: float) -> str:
        stake, f, allow = self.compute(p_win, roi, balance)
        ev = p_win * roi - (1 - p_win)
        return (f"P(win)={p_win:.4f}  ROI={roi:.3f}  "
                f"Kelly_f={f:.4f}  kelly_stake=${stake:.2f}  "
                f"EV={ev:+.4f}  allow={allow}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY GATE
# ─────────────────────────────────────────────────────────────────────────────

class EntryGate:
    """All conditions must pass before a trade is placed."""

    def evaluate(self, est: DigitBiasEstimator,
                 balance: float, sizer: KellySizer) -> tuple:
        diag = est.status()

        if not est.ready():
            if not est.calibrator.calibrated:
                return False, f"warmup+calibration ({est.n}/{MIN_WARMUP_TICKS})", diag
            return False, f"warmup ({est.n}/{MIN_WARMUP_TICKS})", diag

        regime = est.regime()
        if regime not in ALLOW_REGIMES:
            return False, f"regime={regime} (blocked)", diag

        if est.bias_edge < MIN_BIAS_EDGE:
            return False, (f"bias_edge={est.bias_edge:.4f} < {MIN_BIAS_EDGE} "
                           f"(no signal)"), diag

        pw = est.p_win()
        ev = est.ev_per_dollar()
        if ev < MIN_EV:
            return False, f"EV={ev:.4f} < {MIN_EV}", diag

        roi = est.ema_roi
        stake, f, has_edge = sizer.compute(pw, roi, balance)
        if not has_edge:
            return False, f"Kelly_f={f:.4f} ≤ 0 (no edge)", diag

        diag["kelly_stake"] = stake
        diag["kelly_f"]     = round(f, 6)
        return True, "OK", diag


# ─────────────────────────────────────────────────────────────────────────────
# SESSION RISK MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class SessionRisk:
    def __init__(self):
        self.session_pnl = 0.0
        self.wins        = 0
        self.losses      = 0

    def record_win(self, profit: float):
        self.wins        += 1
        self.session_pnl += profit
        tlog(f"WIN  +${profit:.4f}  |  P&L ${self.session_pnl:+.4f}")
        self._stats()

    def record_loss(self, amount: float):
        self.losses      += 1
        self.session_pnl -= amount
        tlog(f"LOSS -${amount:.2f}  |  P&L ${self.session_pnl:+.4f}")
        self._stats()

    def can_trade(self) -> bool:
        if self.session_pnl >= TARGET_PROFIT:
            info(f"Target profit ${TARGET_PROFIT} reached — stopping")
            return False
        if self.session_pnl <= -STOP_LOSS:
            warn(f"Stop-loss -${STOP_LOSS} hit — stopping")
            return False
        return True

    def _stats(self):
        total = self.wins + self.losses
        wr    = self.wins / total * 100 if total else 0
        info(f"Trades:{total}  W:{self.wins}  L:{self.losses}  "
             f"WR:{wr:.1f}%  P&L:${self.session_pnl:.4f}")


# ─────────────────────────────────────────────────────────────────────────────
# DERIV CLIENT
# ─────────────────────────────────────────────────────────────────────────────

class DerivClient:
    def __init__(self):
        self.endpoint   = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"
        self.ws         = None
        self._send_q    = None
        self._inbox     = None
        self._send_task = None
        self._recv_task = None

    async def connect(self) -> bool:
        try:
            info(f"Connecting → {self.endpoint}")
            self.ws = await websockets.connect(
                self.endpoint,
                ping_interval=WS_PING,
                ping_timeout=20,
                close_timeout=10,
            )
            self._send_q = asyncio.Queue()
            self._inbox  = asyncio.Queue()
            self._start_io()
            await asyncio.sleep(0)
            await self._send({"authorize": API_TOKEN})
            resp = await self._recv_type("authorize", timeout=15)
            if not resp or "error" in resp:
                msg = (resp or {}).get("error", {}).get("message", "timeout")
                err(f"Auth failed: {msg}")
                return False
            auth = resp.get("authorize", {})
            info(f"Auth OK  |  {auth.get('loginid')}  |  "
                 f"Balance: ${auth.get('balance', 0):.2f}")
            return True
        except Exception as e:
            err(f"Connect error: {e}")
            traceback.print_exc(file=sys.stdout)
            return False

    def _start_io(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        self._send_task = asyncio.create_task(self._send_pump(), name="send")
        self._recv_task = asyncio.create_task(self._recv_pump(), name="recv")

    async def _send_pump(self):
        while True:
            data, fut = await self._send_q.get()
            try:
                await self.ws.send(json.dumps(data))
                if fut and not fut.done():
                    fut.set_result(True)
            except Exception as exc:
                if fut and not fut.done():
                    fut.set_exception(exc)
            finally:
                self._send_q.task_done()

    async def _recv_pump(self):
        try:
            async for raw in self.ws:
                try:
                    await self._inbox.put(json.loads(raw))
                except json.JSONDecodeError:
                    pass
        except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
            await self._inbox.put({"__disconnect__": True})
        except Exception as exc:
            err(f"Recv pump error: {exc}")
            await self._inbox.put({"__disconnect__": True})

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def _send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_q.put((data, fut))
        await fut

    async def receive(self, timeout: float = 60) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def _recv_type(self, msg_type: str, timeout: float = 10) -> Optional[dict]:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if "__disconnect__" in msg:
                await self._inbox.put(msg)
                return None
            if msg_type == "proposal" and "proposal_open_contract" in msg:
                continue
            if msg_type in msg or "error" in msg:
                return msg
            await self._inbox.put(msg)

    async def fetch_balance(self) -> Optional[float]:
        try:
            await self._send({"balance": 1})
            resp = await self._recv_type("balance", timeout=10)
            if resp and "balance" in resp:
                return float(resp["balance"]["balance"])
        except Exception as e:
            warn(f"Balance fetch: {e}")
        return None

    async def subscribe_ticks(self) -> bool:
        await self._send({"ticks": SYMBOL, "subscribe": 1})
        resp = await self._recv_type("tick", timeout=10)
        if not resp or "error" in resp:
            msg = (resp or {}).get("error", {}).get("message", "timeout")
            err(f"Tick subscribe failed: {msg}")
            return False
        info(f"Subscribed to {SYMBOL}")
        return True

    async def place_trade(self, stake: float, direction: str,
                          est: DigitBiasEstimator) -> Optional[int]:
        """
        Place a DIGITEVEN or DIGITODD contract for DURATION_TICKS ticks on 1HZ25V.
        direction: "EVEN" or "ODD"
        """
        contract_type = f"DIGIT{'EVEN' if direction == 'EVEN' else 'ODD'}"

        await self._send({
            "proposal":      1,
            "amount":        stake,
            "basis":         "stake",
            "contract_type": contract_type,
            "currency":      "USD",
            "duration":      DURATION_TICKS,
            "duration_unit": "t",
            "symbol":        SYMBOL,
        })
        proposal = await self._recv_type("proposal", timeout=20)
        if not proposal or "error" in proposal:
            msg = (proposal or {}).get("error", {}).get("message", "timeout")
            err(f"Proposal error: {msg}")
            return None

        prop   = proposal.get("proposal", {})
        pid    = prop.get("id")
        ask    = float(prop.get("ask_price", stake))
        payout = float(prop.get("payout", 0))

        if not pid:
            err("No proposal ID")
            return None

        est.record_roi(ask, payout)
        roi = payout / ask if ask > 0 else 0
        info(f"Proposal OK  |  {contract_type}  ask=${ask:.2f}  "
             f"payout=${payout:.2f}  ROI={roi*100:.1f}%  ema_roi={est.ema_roi:.4f}")

        buy_ts      = time.time()
        contract_id = None
        await self._send({"buy": pid, "price": ask})

        for attempt in range(BUY_RETRIES):
            resp = await self._recv_type("buy", timeout=8)
            if resp is None:
                warn(f"Buy no response attempt {attempt + 1}")
                continue
            if "error" in resp:
                err(f"Buy error: {resp['error'].get('message', '')}")
                return None
            contract_id = resp.get("buy", {}).get("contract_id")
            if contract_id:
                break

        if not contract_id:
            warn("No contract_id — orphan recovery")
            for _ in range(4):
                await asyncio.sleep(3)
                await self._send({"profit_table": 1, "description": 1,
                                  "sort": "DESC", "limit": 5})
                resp = await self._recv_type("profit_table", timeout=10)
                if resp and "profit_table" in resp:
                    for tx in resp["profit_table"].get("transactions", []):
                        if (abs(float(tx.get("buy_price", 0)) - stake) < 0.01 and
                                float(tx.get("purchase_time", 0)) >= buy_ts - 10):
                            contract_id = tx.get("contract_id")
                            info(f"Orphan recovered → {contract_id}")
                            break
                if contract_id:
                    break
            if not contract_id:
                err("Orphan recovery failed")
                return None

        # Subscribe by contract ID after buy
        try:
            await self._send({
                "proposal_open_contract": 1,
                "contract_id":            contract_id,
                "subscribe":              1,
            })
            slog(f"Post-buy subscription to contract {contract_id} sent")
        except Exception:
            pass

        tlog(f"Placed  |  contract={contract_id}  |  "
             f"{contract_type}  ${ask:.2f}  {DURATION_TICKS}t  symbol={SYMBOL}")
        return contract_id

    async def poll_contract(self, contract_id) -> Optional[dict]:
        try:
            slog(f"Polling contract {contract_id} ...")
            await self._send({
                "proposal_open_contract": 1,
                "contract_id":            int(contract_id),
            })
            resp = await self._recv_type("proposal_open_contract", timeout=10)
            if resp and "proposal_open_contract" in resp:
                data = resp["proposal_open_contract"]
                slog(f"Poll result: status={data.get('status')}  "
                     f"is_settled={data.get('is_settled')}  "
                     f"profit={data.get('profit')}")
                return data
            else:
                slog(f"Poll: no proposal_open_contract in response: "
                     f"{list((resp or {}).keys())}")
        except Exception as e:
            warn(f"Poll error: {e}")
        return None

    @staticmethod
    def contract_ids_match(cid_a, cid_b) -> bool:
        return str(cid_a) == str(cid_b)

    @staticmethod
    def is_settled(data: dict) -> bool:
        if data.get("is_settled"):
            return True
        if data.get("is_sold"):
            return True
        status = data.get("status", "").lower()
        return status in ("sold", "won", "lost")


# ─────────────────────────────────────────────────────────────────────────────
# HEALTH SERVER
# ─────────────────────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    bot_ref = None

    def do_GET(self):
        body = b"OK"
        if self.path == "/status" and self.bot_ref:
            b = self.bot_ref
            s = b.session_risk
            m = b.martingale
            body = json.dumps({
                "status":         "running",
                "symbol":         SYMBOL,
                "locked":         b.waiting_for_result,
                "session_wins":   s.wins,
                "session_losses": s.losses,
                "session_pnl":    round(s.session_pnl, 4),
                "balance":        b.balance,
                "martingale":     {
                    "step":      m.step,
                    "streak":    m.consecutive_losses,
                    "max_steps": MARTINGALE_MAX_STEPS,
                    "factor":    MARTINGALE_FACTOR,
                },
                "estimator":      b.est.status(),
            }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


def start_health_server(bot):
    HealthHandler.bot_ref = bot
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    info(f"Health server :{PORT}  →  GET /  or  GET /status")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BOT
# ─────────────────────────────────────────────────────────────────────────────

class DigitsBot:
    def __init__(self):
        self.client       = DerivClient()
        self.est          = DigitBiasEstimator()
        self.gate         = EntryGate()
        self.sizer        = KellySizer()
        self.session_risk = SessionRisk()
        self.martingale   = MartingaleManager()

        self.waiting_for_result: bool            = False
        self._evaluating:        bool            = False
        self.current_trade:      Optional[dict]  = None
        self.lock_since:         Optional[float] = None
        self._stop:              bool            = False
        self._bal_before:        Optional[float] = None
        self.balance:            float           = 1000.0

        self.tick_count:      int   = 0
        self.signal_count:    int   = 0
        self.last_trade_time: float = 0.0
        self.skip_log_count:  int   = 0

        self._poller_task: Optional[asyncio.Task] = None

    def _unlock(self, reason: str = "manual"):
        if self.waiting_for_result:
            cid = (self.current_trade or {}).get("id", "?")
            info(f"Unlock: contract={cid}  reason={reason}")
        self.waiting_for_result = False
        self.current_trade      = None
        self.lock_since         = None
        self._evaluating        = False
        if self._poller_task and not self._poller_task.done():
            self._poller_task.cancel()
            self._poller_task = None

    # ── Settlement Poller ─────────────────────────────────────────────────────

    async def _settlement_poller(self, contract_id, expected_expiry: float):
        """
        5-tick contracts settle in ~5s. Poll aggressively near expiry.
        """
        wait_before_poll = max(0, expected_expiry - time.time() - 2)
        slog(f"Poller started for {contract_id}  "
             f"(waiting {wait_before_poll:.1f}s before first poll)")

        try:
            if wait_before_poll > 0:
                await asyncio.sleep(wait_before_poll)

            deadline = time.time() + 60   # 1-minute poll window
            poll_n   = 0

            while self.waiting_for_result and time.time() < deadline:
                poll_n += 1
                data = await self.client.poll_contract(contract_id)

                if data is None:
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                slog(f"Poll #{poll_n}: status={data.get('status')!r}  "
                     f"is_settled={data.get('is_settled')!r}  "
                     f"profit={data.get('profit')!r}")

                if DerivClient.is_settled(data):
                    slog(f"Poll #{poll_n}: CONTRACT SETTLED  profit={data.get('profit')}")
                    ok = await self.handle_settlement(data)
                    if not ok:
                        self._stop = True
                    return
                else:
                    await asyncio.sleep(POLL_INTERVAL)

            if self.waiting_for_result:
                warn(f"Poller timed out for {contract_id} — forcing unlock")
                self._unlock("poller_timeout")

        except asyncio.CancelledError:
            slog(f"Poller for {contract_id} cancelled (settlement via WS)")
        except Exception as e:
            err(f"Poller exception: {e}")
            traceback.print_exc(file=sys.stdout)

    def _start_poller(self, contract_id, expiry_time: float):
        if self._poller_task and not self._poller_task.done():
            self._poller_task.cancel()
        self._poller_task = asyncio.create_task(
            self._settlement_poller(contract_id, expiry_time),
            name=f"poller_{contract_id}",
        )

    # ── Lock timeout (last resort) ────────────────────────────────────────────

    def _check_lock_timeout(self):
        if not self.waiting_for_result or self.lock_since is None:
            return
        elapsed = time.monotonic() - self.lock_since
        if elapsed >= LOCK_TIMEOUT:
            warn(f"Hard lock timeout ({LOCK_TIMEOUT}s) — force unlock.")
            self._unlock("hard_timeout")

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def on_tick(self, price: float):
        self.tick_count += 1
        self.est.ingest(price)
        self._check_lock_timeout()

        if self.tick_count % 30 == 0:
            if not self.est.ready():
                warmup_left = MIN_WARMUP_TICKS - self.est.n
                cal_status  = "CALIBRATED" if self.est.calibrator.calibrated else "pending"
                info(f"tick={self.tick_count}  price={price:.5f}  "
                     f"WARMUP({warmup_left} ticks remaining)  "
                     f"calibration={cal_status}  "
                     f"σ={self.est.sigma:.5f}  bias={self.est.bias:.4f}")
            else:
                locked  = "LOCKED" if self.waiting_for_result else "READY"
                mgstate = (f"  marti=[{self.martingale.describe()}]"
                           if self.martingale.is_recovering else "")
                info(f"tick={self.tick_count}  price={price:.5f}  {locked}  "
                     f"σ={self.est.sigma:.5f}  regime={self.est.regime()}  "
                     f"bias={self.est.bias:.4f}({self.est.trade_direction()})  "
                     f"edge={self.est.bias_edge:.4f}  "
                     f"P(win)={self.est.p_win():.4f}  "
                     f"EV={self.est.ev_per_dollar():+.4f}"
                     f"{mgstate}")

        if self.waiting_for_result or self._evaluating:
            return
        if not self.session_risk.can_trade():
            self._stop = True
            return
        if time.time() - self.last_trade_time < TRADE_COOLDOWN:
            return

        self._evaluating = True
        try:
            await self._evaluate(price)
        finally:
            self._evaluating = False

    # ── Distribution evaluation ───────────────────────────────────────────────

    async def _evaluate(self, price: float):
        if self.waiting_for_result:
            return

        allow, reason, diag = self.gate.evaluate(
            self.est, self.balance, self.sizer)

        if not allow:
            self.skip_log_count += 1
            if self.skip_log_count % 60 == 1:
                dlog(f"SKIP: {reason}  "
                     f"(σ={self.est.sigma:.5f}  "
                     f"bias={self.est.bias:.4f}  "
                     f"edge={self.est.bias_edge:.4f}  "
                     f"EV={self.est.ev_per_dollar():+.4f})")
            return

        self.skip_log_count = 0

        direction   = self.est.trade_direction()
        pw          = self.est.p_win()
        roi         = self.est.ema_roi
        kelly_stake = diag.get("kelly_stake", MIN_STAKE)
        f           = diag.get("kelly_f", 0.0)
        ev          = self.est.ev_per_dollar()
        final_stake = self.martingale.compute_stake(kelly_stake)

        self.signal_count += 1
        info("=" * 62)
        info(f"TRADE #{self.signal_count}  |  tick={self.tick_count}  "
             f"|  symbol={SYMBOL}  |  direction={direction}")
        info(f"  σ={self.est.sigma:.6f}  regime={self.est.regime()}")
        info(f"  bias={self.est.bias:.6f}  edge={self.est.bias_edge:.6f}  "
             f"raw_even={self.est.raw_even_rate:.4f}")
        info(f"  P(win)={pw:.6f}  ROI={roi:.4f}  EV={ev:+.6f}")
        info(f"  Kelly_f={f:.6f}  kelly_stake=${kelly_stake:.2f}")
        info(f"  Martingale: {self.martingale.describe()}")
        info(f"  FINAL stake=${final_stake:.2f}  balance=${self.balance:.2f}")
        info("=" * 62)

        self._bal_before = await self.client.fetch_balance()
        if self._bal_before is not None:
            self.balance = self._bal_before
            info(f"Pre-trade balance: ${self._bal_before:.2f}")

        # Estimate expiry: 1HZ25V ticks ~25× per second nominally,
        # but the API delivers one tick/s to clients. 5 ticks ≈ 5s wall time.
        expiry_time = time.time() + (DURATION_TICKS * 1.5)  # small buffer

        contract_id = await self.client.place_trade(final_stake, direction, self.est)

        if contract_id:
            self.current_trade = {
                "id":         contract_id,
                "stake":      final_stake,
                "direction":  direction,
                "p_win":      pw,
                "roi":        roi,
                "ev":         ev,
                "kelly_f":    f,
                "sigma":      self.est.sigma,
                "bias":       self.est.bias,
                "bias_edge":  self.est.bias_edge,
                "regime":     self.est.regime(),
                "marti_step": self.martingale.step,
            }
            self.waiting_for_result = True
            self.lock_since         = time.monotonic()
            self.last_trade_time    = time.time()

            self._start_poller(contract_id, expiry_time)

            jlog({
                "type":        "trade",
                "cid":         contract_id,
                "direction":   direction,
                "stake":       final_stake,
                "kelly_stake": kelly_stake,
                "marti_step":  self.martingale.step,
                "p_win":       round(pw, 6),
                "roi":         round(roi, 4),
                "ev":          round(ev, 6),
                "kelly_f":     round(f, 6),
                "sigma":       round(self.est.sigma, 6),
                "bias":        round(self.est.bias, 6),
                "bias_edge":   round(self.est.bias_edge, 6),
                "regime":      self.est.regime(),
                "ts":          _ts(),
            })
        else:
            self._bal_before = None
            warn("Placement failed — ready for next evaluation")
            self.last_trade_time = time.time()

    # ── Settlement handler ────────────────────────────────────────────────────

    async def handle_settlement(self, data: dict) -> bool:
        cid = data.get("contract_id")

        slog(f"handle_settlement: incoming_cid={cid!r}  "
             f"waiting={self.waiting_for_result}  "
             f"current_id={self.current_trade.get('id') if self.current_trade else None!r}")

        if not self.waiting_for_result:
            slog("handle_settlement: not waiting — ignoring")
            return True

        if not self.current_trade:
            slog("handle_settlement: no current trade — ignoring")
            return True

        expected_cid = self.current_trade["id"]
        if not DerivClient.contract_ids_match(cid, expected_cid):
            slog(f"handle_settlement: CID mismatch "
                 f"(got {cid!r}  expected {expected_cid!r}) — ignoring")
            return True

        if not DerivClient.is_settled(data):
            slog(f"handle_settlement: CID matched but NOT yet settled  "
                 f"status={data.get('status')!r}")
            return True

        slog(f"handle_settlement: CONFIRMED SETTLED  "
             f"cid={cid}  status={data.get('status')}  profit={data.get('profit')}")

        profit = float(data.get("profit", 0))
        status = data.get("status", "unknown")
        stake  = self.current_trade["stake"]

        bal_after = await self.client.fetch_balance()
        if bal_after is not None and self._bal_before is not None:
            actual = round(bal_after - self._bal_before, 4)
            self.balance = bal_after
        else:
            actual = profit

        tlog(f"SETTLED  |  contract={cid}  |  "
             f"status={status}  |  direction={self.current_trade['direction']}  |  "
             f"profit=${actual:+.4f}")

        won = actual > 0

        if won:
            self.martingale.record_win()
        else:
            self.martingale.record_loss()

        mlog(f"After settlement: {self.martingale.describe()}")

        self.est.record_outcome(won, actual)

        if won:
            self.session_risk.record_win(actual)
        else:
            self.session_risk.record_loss(stake)

        self.est.log_status()

        jlog({
            "type":            "result",
            "cid":             cid,
            "status":          status,
            "direction":       self.current_trade["direction"],
            "profit":          actual,
            "pnl":             round(self.session_risk.session_pnl, 4),
            "wins":            self.session_risk.wins,
            "losses":          self.session_risk.losses,
            "sigma":           round(self.est.sigma, 6),
            "bias_was":        round(self.current_trade.get("bias", 0.5), 6),
            "p_win_was":       round(self.current_trade.get("p_win", 0), 6),
            "ev_was":          round(self.current_trade.get("ev", 0), 6),
            "marti_step_was":  self.current_trade.get("marti_step", 0),
            "marti_step_next": self.martingale.step,
            "ts":              _ts(),
        })

        self._bal_before = None
        self._unlock("settlement")
        info("Ready for next evaluation")
        return self.session_risk.can_trade()

    # ── Reconnect ─────────────────────────────────────────────────────────────

    async def _reconnect(self) -> bool:
        delay   = RECONNECT_MIN
        attempt = 0
        while not self._stop:
            attempt += 1
            warn(f"Reconnect attempt {attempt} in {delay}s ...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, RECONNECT_MAX)
            await self.client.close()
            self.client = DerivClient()
            try:
                if not await self.client.connect():
                    continue
                if not await self.client.subscribe_ticks():
                    continue
                if self.waiting_for_result and self.current_trade:
                    cid  = self.current_trade["id"]
                    info(f"Re-attaching contract {cid}")
                    data = await self.client.poll_contract(cid)
                    if data:
                        await self.handle_settlement(data)
                    if self.waiting_for_result:
                        await self.client._send({
                            "proposal_open_contract": 1,
                            "contract_id":            cid,
                            "subscribe":              1,
                        })
                info("Reconnect OK")
                return True
            except Exception as e:
                err(f"Reconnect error: {e}")
        return False

    # ── Main run loop ─────────────────────────────────────────────────────────

    async def run(self):
        info("=" * 62)
        info("DIGITS BOT  v3  —  1HZ25V  —  Distribution Thinking")
        info(f"Symbol     : {SYMBOL}")
        info(f"Contract   : DIGITEVEN / DIGITODD  {DURATION_TICKS} ticks")
        info(f"Model      : EWMA parity bias  α={BIAS_ALPHA}  window={N_WINDOW}")
        info(f"Calibration: live-recal every {int(RECAL_INTERVAL)} ticks  "
             f"window={int(RECAL_WINDOW)}  CALM×{CALM_FACTOR}  NORMAL×{NORMAL_FACTOR}")
        info(f"Gate       : bias_edge≥{MIN_BIAS_EDGE}  EV≥{MIN_EV}  "
             f"regime∈{{CALM,NORMAL}}")
        info(f"Martingale : base=${MARTINGALE_BASE_STAKE}  "
             f"factor={MARTINGALE_FACTOR}×  max_steps={MARTINGALE_MAX_STEPS}  "
             f"kicks_in_after=2_losses")
        info(f"Session    : target=+${TARGET_PROFIT}  stop=-${STOP_LOSS}")
        info(f"Poller     : every {POLL_INTERVAL}s")
        info("=" * 62)

        if API_TOKEN in ("REPLACE_WITH_YOUR_TOKEN", ""):
            err("Set DERIV_API_TOKEN before running")
            return

        if not await self.client.connect():
            return

        bal = await self.client.fetch_balance()
        if bal:
            self.balance = bal
            info(f"Starting balance: ${self.balance:.2f}")

        if not await self.client.subscribe_ticks():
            return

        info(f"Live — warming up estimator ({MIN_WARMUP_TICKS} ticks) ...")

        try:
            while not self._stop:
                response = await self.client.receive(timeout=60)

                if "__disconnect__" in response:
                    warn("WS disconnected — reconnecting")
                    if not await self._reconnect():
                        break
                    continue

                if not response:
                    try:
                        await self.client.ws.ping()
                    except Exception:
                        warn("Ping failed — reconnecting")
                        if not await self._reconnect():
                            break
                    continue

                if "tick" in response:
                    quote = response["tick"].get("quote")
                    if quote is not None:
                        await self.on_tick(float(quote))

                if "balance" in response:
                    bal = response["balance"].get("balance")
                    if bal is not None:
                        self.balance = float(bal)

                if "proposal_open_contract" in response:
                    poc = response["proposal_open_contract"]
                    slog(f"WS proposal_open_contract: cid={poc.get('contract_id')}  "
                         f"status={poc.get('status')}  "
                         f"is_settled={poc.get('is_settled')}")
                    ok = await self.handle_settlement(poc)
                    if not ok:
                        self._stop = True

                if "buy" in response:
                    ok = await self.handle_settlement(response["buy"])
                    if not ok:
                        self._stop = True

                if "transaction" in response:
                    tx = response["transaction"]
                    if "contract_id" in tx:
                        slog(f"WS transaction: action={tx.get('action')}  "
                             f"cid={tx.get('contract_id')}  "
                             f"profit={tx.get('profit')}")
                        ok = await self.handle_settlement({
                            "contract_id": tx.get("contract_id"),
                            "profit":      tx.get("profit", 0),
                            "status":      tx.get("action", "sold"),
                            "is_settled":  True,
                        })
                        if not ok:
                            self._stop = True

        except KeyboardInterrupt:
            info("Stopped by user")
        except Exception as e:
            err(f"Fatal: {e}")
            traceback.print_exc(file=sys.stdout)
        finally:
            self._print_final()
            await self.client.close()
            info("Bot exited.")

    def _print_final(self):
        s     = self.session_risk
        m     = self.martingale
        total = s.wins + s.losses
        wr    = s.wins / total * 100 if total else 0
        info("=" * 62)
        info("FINAL SESSION STATS")
        info(f"Live ticks    : {self.tick_count}")
        info(f"Trades placed : {self.signal_count}")
        info(f"W:{s.wins}  L:{s.losses}  WR:{wr:.1f}%  P&L:${s.session_pnl:.4f}")
        info(f"Final σ       : {self.est.sigma:.6f}")
        info(f"Recalibration : {self.est.calibrator.summary()}")
        info(f"Calibrated σ  : CALM={self.est.calibrator.sigma_calm:.5f}  "
             f"NORMAL={self.est.calibrator.sigma_normal:.5f}")
        info(f"Final regime  : {self.est.regime()}")
        info(f"Final bias    : {self.est.bias:.6f}  "
             f"edge={self.est.bias_edge:.6f}  "
             f"dir={self.est.trade_direction()}")
        info(f"Final P(win)  : {self.est.p_win():.6f}")
        info(f"Final EV/$    : {self.est.ev_per_dollar():+.6f}")
        info(f"ROI tracked   : {self.est.ema_roi:.4f}")
        info(f"Martingale    : step={m.step}  streak={m.consecutive_losses}")
        for r, st in self.est.regime_stats.items():
            if st["trades"] > 0:
                rwr = st["wins"] / st["trades"] * 100
                info(f"  {r}: {st['trades']} trades  WR:{rwr:.1f}%")
        info("=" * 62)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    bot = DigitsBot()
    start_health_server(bot)
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
