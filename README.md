# VIX Futures PCA Analysis

[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A quantitative research tool for analyzing the Principal Components of the VIX Futures term structure. This project processes CFE (CBOE Futures Exchange) data to construct constant-maturity VIX curves and extracts the primary drivers of volatility surface movements (Level, Slope, Curvature).

## 📊 Strategy Logic

The VIX futures curve is highly correlated. This project uses **Principal Component Analysis (PCA)** to decompose the returns of the term structure into orthogonal factors:
1.  **PC1 (Level):** ~90-98% of variance (Parallel shifts in volatility).
2.  **PC2 (Slope):** ~1-7% of variance (Changes in Contango/Backwardation).
3.  **PC3 (Curvature):** ~0-3% of variance (Butterfly shifts).

Understanding these factors allows for better hedging and "Curve Trading" (e.g., betting on the slope steepening without taking direction risk).

## 🛠️ Installationn

This project uses modern packaging with `uv`.

```bash
git clone [https://github.com/lanteignel93/vix_pca_futures_trade](https://github.com/lanteignel93/vix_pca_futures_trade)
cd vix_pca_futures_trade

uv sync

source .venv/bin/activate

pre-commit install
