# SuperClaw 30.0 — APEX + Humanizer + Flywheel Edition

> An autonomous, human-emulating content engine for Binance Square — powered by a multi-layer signal stack, narrative detection, archetype-bound swarm nodes, and a self-tuning flywheel.

SuperClaw 30.0 is a research-grade trading-content agent that ingests real-time market data, on-chain whale activity, X/Twitter firehoses, and RSS news, then publishes humanized posts to Binance Square (with optional Discord mirroring). It learns from its own engagement, adapts its cadence via elastic limits, and coordinates with peer swarm nodes for cross-engagement.

---

## Disclaimer

This software automates publishing to a live social platform using a real browser session. **Use at your own risk.**

- It is designed to **emulate human behavior** (typing cadence, mouse movement, jitter). This may violate the **Terms of Service** of Binance, X/Twitter, Discord, or any other platform it touches.
- Publishing financial content may be regulated in your jurisdiction. **You are responsible for compliance** with all applicable laws (MiCA, SEC, FCA, MAS, etc.).
- **Nothing this bot produces is financial advice.** Every generated signal is heuristic and may be wrong.
- The author(s) assume **no liability** for account bans, financial loss, legal exposure, or reputational damage.

If you are not comfortable with any of the above, **do not run this code.**

---

## Feature Overview

| Layer | Capability |
|-------|-----------|
| **L1** | Real-time **X firehose** + **on-chain whale tracker** + **narrative engine** |
| **L2** | Persistent **archetype binding** per swarm node (A1–A6) |
| **L3** | **Local human-vs-AI discriminator** (sklearn logistic regression) |
| **L4** | **Swarm cross-engagement** — peer nodes reply with staggered delays |
| **L5** | **Language arbitrage** with timezone-peak scheduling (`vi`, `ar`, `zh`) |
| **L6** | **Elastic limits** modulated by VIX + Fear & Greed |
| **RAG** | Few-shot injection from top-performing recent posts |
| **A/B** | Hook/format testing with engagement-weighted winner selection |
| **Bayes** | 1-D Bayesian tuning of the signal threshold |
| **Paper** | Binance **testnet** execution bridge with PnL checkpoints |
| **SVG** | Branded gainers/liquidation infographics rendered on the fly |
| **Meme** | VLM-generated localized crypto memes for `vi` / `tr` |
| **Humanizer** | Bezier mouse paths, WPM-typed text, typo injection, burst pauses |

---

## How It Works (High Level)

```
 ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
 │  Binance     │   │  X Firehose  │   │  On-Chain    │
 │  REST API    │   │  (RSS)       │   │  Whales (RPC)│
 └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
        │                  │                  │
        └──────────┬───────┴──────────────────┘
                   ▼
          ┌────────────────────┐
          │  Narrative Engine  │◄──── trending_hijacker / feed_learner
          └─────────┬──────────┘
                    ▼
          ┌────────────────────┐
          │  Content Scheduler │◄──── elastic multiplier (VIX + F&G)
          └─────────┬──────────┘
                    ▼
          ┌────────────────────┐
          │  Post Pipeline     │◄──── Trust Engine + L3 Discriminator
          └─────────┬──────────┘
                    ▼
          ┌────────────────────┐
          │  Humanizer +       │
          │  Playwright (CDP)  │────► Binance Square
          └────────────────────┘
```

Post-publish, the agent registers an **engagement watch**, schedules **language variants**, queues **swarm cross-replies**, opens a **paper trade**, and logs everything to SQLite for the learning loop.

---

## Requirements

- **Python 3.10+**
- **Playwright** + Chromium
- A Chromium-based browser (Edge / Chrome / Chromium) with a **logged-in Binance Square session**
- Optional: `sentence-transformers` for embedding dedup (`all-MiniLM-L6-v2`)
- Optional: `scikit-learn` for the L3 discriminator
- Optional: `scipy` for Bayesian tuning

### Python Dependencies

```bash
pip install requests openai flask playwright
pip install scikit-learn sentence-transformers scipy   # optional but recommended
playwright install chromium
```

---

## Installation

```bash
git clone https://github.com/antbag-dev/superclaw.git
cd superclaw
python -m venv .venv && source .venv/bin/activate   # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
playwright install chromium
```

Then create your key files (see **Configuration** below) and run:

```bash
python superclaw.py
```

On first run, a browser window will open to `https://www.binance.com/en/square`. **Log in manually** and complete any CAPTCHA. The session is persisted to `.superclaw-browser-profile/`.

---

## Configuration

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `DISCORD_WEBHOOK_URL` | Mirror posts to a Discord channel |
| `BINANCE_SQUARE_OPENAPI_KEY` | Optional Square OpenAPI key (fallback publisher) |
| `GROQ_API_KEY` / `GROQ_API_KEYS` | Groq API key(s) — comma/semicolon separated |
| `CEREBRAS_API_KEY` / `CEREBRAS_API_KEYS` | Cerebras API key(s) |
| `OPENROUTER_API_KEY` / `OPENROUTER_API_KEYS` | OpenRouter API key(s) |
| `SWARM_NODE_ID` | Unique node identifier (default `node-alpha`) |
| `SWARM_PEER_NODES` | Comma-separated peer node URLs/IDs for cross-engagement |
| `ONCHAIN_RPC_URL` | Ethereum JSON-RPC endpoint for whale tracking |
| `BINANCE_TESTNET_KEY` / `BINANCE_TESTNET_SECRET` | Testnet credentials for paper trading |

### Key Files

You may also drop keys into plain text files in the project root (one per line, `#` for comments):

- `groq_keys.txt`
- `cerebras_keys.txt`
- `openrouter_keys.txt`

### Generated State

| File / Dir | Purpose |
|------------|---------|
| `bot_state.json` | Full agent state (counters, active signals, tune params) |
| `analytics.db` | SQLite: posts, narratives, trust scores, A/B tests, etc. |
| `archetype_binding_<node>.json` | Persistent archetype binding per node |
| `discriminator_<node>.pkl` | Trained L3 classifier |
| `svg_out/` | Generated SVG infographics and meme cards |
| `.superclaw-browser-profile/` | Playwright browser profile (session cookies) |

> **Never commit these to Git.** Add them to `.gitignore`.

---

## Dashboard

A lightweight Flask dashboard runs at **http://localhost:8080** with live metrics:

- 7-day views / posts
- Browser vs API publish counts
- X firehose hits, whale events, swarm replies
- Active narratives and recent posts
- Humanizer action log

Quick-action endpoints:

| Endpoint | Action |
|----------|--------|
| `/api/stats` | Dump raw metrics |
| `/api/analytics` | Full analytics summary |
| `/api/narratives` | Top narratives |
| `/api/feed-now` | Trigger a Square feed scan |
| `/api/learn` | Relearn weights from analytics |
| `/api/tune` | Run adaptive tuner |
| `/api/bayes` | Run Bayesian threshold tune |
| `/api/discriminator` | Retrain L3 classifier |
| `/api/ab` | Decide A/B winners |

---

## Project Structure

The script is intentionally monolithic (~3,900 lines) and organized into numbered sections:

| Section | Contents |
|---------|----------|
| 1 | Configuration |
| 2 | AI provider layer (Groq / Cerebras / OpenRouter with key rotation) |
| 3 | HTTP + utilities |
| 4 | Analytics DB schema |
| 5 | State management |
| 6 | L1 — X firehose, on-chain whales, narrative engine |
| 7 | L2 — Swarm node archetype binding |
| 8 | L3 — Trust engine + local discriminator |
| 9 | L4 — Engagement optimizer + swarm cross-engagement |
| 10 | L5 — Language arbitrage (timezone peak) |
| 11 | L6 — Content scheduler + elastic limits |
| 12 | Humanizer (mouse, typing, pauses) |
| 13 | Browser launcher (Edge/Chrome via CDP) |
| 14–17 | Binance API, indicators, sentiment, personas, quality scoring |
| 18 | Learned weights from analytics |
| 19 | RAG few-shot injection + embedding dedup |
| 20–21 | A/B testing, Bayesian tuning |
| 22 | AI content generation |
| 23 | Paper trading bridge |
| 24 | SVG renderer |
| 25 | Meme engineer |
| 26–28 | Post pipeline, Square posting, tracking |
| 29–31 | Signal building, news layer, result publishing |
| 32–34 | Dashboard, analytics summary, adaptive tuner |
| 35–37 | Flywheel jobs, main cycle, startup |

---

## 🔁 Operating Loop

Each `SCAN_INTERVAL_SECONDS` (default **45s**):

1. **Background work** — sentiment, active signal checks, engagement tick, language variants, swarm replies, flywheel jobs
2. **Fast lane** — if a fresh, high-category news item drops, publish immediately
3. **Budget gate** — respect `DAILY_POST_TARGET × elastic_mult`
4. **Slot gate** — enforce `MIN_POST_INTERVAL_SECONDS` + jitter
5. **Plan** — score all content types by learned weight × narrative boost × trust bias
6. **Cascade** — try top plan items until one publishes

---

## 🧪 Ethics & Responsible Use

This project sits at the intersection of **trading automation** and **social platform automation**. If you publish it:

- Keep the disclaimer at the top of the README.
- Do **not** ship it with real API keys, webhooks, or wallet addresses hardcoded.
- Consider gating it behind a `--dry-run` flag before any real publishing (currently not implemented).
- Do **not** use it to shill, pump, or manipulate markets. The code includes referral-link and pump-phrase filters — respect their spirit.

If you're not sure whether running this is legal in your jurisdiction, **assume it isn't** and don't run it.

---

## Roadmap / Known Gaps

- [ ] `--dry-run` mode that logs drafts without publishing
- [ ] Refactor monolith into modules (`superclaw/` package)
- [ ] Unit tests for indicators, quality scoring, trust engine
- [ ] Docker image with preinstalled Chromium
- [ ] Proper config file (`config.toml`) instead of 200+ constants
- [ ] Web UI for editing persona prompts and daily targets
- [ ] Real embedding store (FAISS / Chroma) instead of JSON blobs in SQLite

Pull requests welcome.

---

## License

MIT License — see `LICENSE` for details.

> **Not affiliated with Binance, X Corp, Discord, or any exchange.** All trademarks belong to their respective owners.

---

## Acknowledgements

- [Playwright](https://playwright.dev/) for browser automation
- [Groq](https://groq.com/) / [Cerebras](https://cerebras.ai/) / [OpenRouter](https://openrouter.ai/) for fast LLM inference
- [sentence-transformers](https://www.sbert.net/) for embedding-based dedup
- The open-source crypto data community (Alternative.me, CoinDesk, Cointelegraph RSS)

---

**Built for research and content-ops experimentation. Trade safe, publish responsibly.**
