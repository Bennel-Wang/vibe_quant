# Copilot instructions for this repository

## Setup and commands

- Install dependencies with `pip install -r requirements.txt`.
- Create `config.yaml` from `config.yaml.example`, or point `QUANT_CONFIG` at an external config file. Most `quant_system` imports load config at import time and expect a real config file.
- Run the web app with `python main.py web --host 0.0.0.0 --port 8080`.
- Run the scheduler once with `python run_scheduler.py --once --force`, or start it continuously with `python run_scheduler.py`.
- Run the maintained unit suite with `python -m pytest tests/unit -q`.
- Run a single test with `python -m pytest tests/unit/test_config_manager.py::TestBasicConfig::test_web_config -q`.
- Build and run the containers with `docker compose up --build`.

## High-level architecture

- `main.py` is the CLI entrypoint. It wires subcommands like data update, indicator update, backtest, risk report, and web startup to shared services in `quant_system`. It also refreshes `data/trading_dates.json` on startup.
- `quant_system\data_source.py` is the application-facing adapter, not the primary fetch engine. It normalizes stock codes and column names, then delegates history and live fetching to `data_sourcing\DataManager`, which handles source fallback, incremental updates, merging, and CSV persistence.
- `quant_system\web_app.py` is a large Flask integration layer, not a thin controller. It directly coordinates stocks, data, indicators, strategy execution, backtests, risk, notifications, scheduler operations, and several JSON-backed UI state stores.
- `quant_system\scheduler.py` reuses the same shared services as the CLI and web app. Scheduler configuration lives in `data\scheduler_config.json`, and jobs run in Beijing time.

## Key conventions

- Reuse the module-level singletons that the app already exports: `config`, `stock_manager`, `unified_data`, `technical_indicators`, `indicator_analyzer`, `feature_extractor`, `strategy_manager`, `ai_decision_maker`, `backtest_engine`, `risk_manager`, `notification_manager`, and `scheduler`. In tests, create isolated instances or reset singleton state instead of mutating shared globals.
- Config resolution order is `QUANT_CONFIG` -> repository `config.yaml` -> legacy `C:\Users\quantization_config.yaml`. `ConfigManager` also creates the configured data and log directories automatically.
- `config\stocks.yaml` is the source of truth for tracked stocks, sectors, and indices, including metadata such as `industry`, `notes`, `strategy`, `buy_strategy`, `sell_strategy`, and optional `list_date`.
- Prefer unified stock codes with explicit market suffixes at service boundaries: `600519.SH`, `000001.SZ`, `00700.HK`. Bare codes are sometimes accepted, but ambiguous codes exist and `stock_manager.get_stock_by_code()` is designed around suffix-form identifiers.
- Preserve the adapter contract in `UnifiedDataSource`: app consumers often expect compatibility columns like `date`, `volume`, and `code`, while the lower-level `data_sourcing` layer works with `trade_date`, `vol`, and `uniformed_stock_code`.
- A lot of runtime state is file-backed under `data\`, not just market CSVs. Important persisted files include `listing_dates.json`, `trading_dates.json`, `system_state.json`, `scheduler_config.json`, `groups.json`, `strategies.json`, and backtest result files under `data\backtests\bt_<run_id>.json`.
- The maintained automated tests are under `tests\unit`. Running repo-wide `pytest` also collects script-like files under `data_sourcing\` and `scripts\`, so scope routine validation to `tests\unit` unless you are intentionally fixing those broader test targets.
