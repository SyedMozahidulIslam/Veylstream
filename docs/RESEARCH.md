# Research lab

## Question and hypotheses
**RQ:** On streaming operational metrics with injected faults, how do rule-based, robust-statistical and unsupervised-ML detectors compare in precision, recall, false-positive rate and detection delay?
* H1: Fixed-threshold rules have the highest precision but lower recall on faults that stay under the thresholds.
* H2: Robust statistics (MAD, IQR) match z-score recall with fewer false positives when baselines contain outliers.
* H3: Isolation Forest, trained on a trailing past window, adds little over univariate detectors when the faults are single-metric, and needs enough history.

## Design (implemented in `veylstream/experiments.py`, run with `scripts/benchmark.py detectors`)
1. A seeded synthetic dataset (65 min, ~25 events/s) with three injected incidents: `failure_spike` on payments/eu-west (5 min, failure prob 0.4), `latency_degradation` on checkout (5 min, ×4 median), `traffic_drop` platform-wide (3.3 min, ×0.2).
2. One `WindowAggregator` (30 s tumbling, 20 s lateness) closes windows; **every detector sees the identical windows**.
3. **Unit of evaluation:** (scope, metric, window) for scopes `service:*`, `region:*` (and `global` only if a scenario targets it) and the five default metrics. Positive = an injected interval that targets that scope+metric overlaps the window. Scopes that see only a *diluted* version of a scoped incident (e.g. region scopes during a checkout-only latency fault) are **excluded**, not counted negative.
4. Scoring starts after the first 30 % of the stream, identically for all detectors: this removes warm-up and keeps the ML training prefix out of the test period (no temporal leakage; the model is also only ever trained on windows preceding the one scored).
5. Multivariate detections (Isolation Forest, `metric=multivariate`) count for any metric of the same scope+window.

## Metrics
Per detector: TP/FP/FN/TN, precision = TP/(TP+FP), recall = TPR = TP/(TP+FN), F1, FPR = FP/(FP+TN), confusion matrix `[[TN,FP],[FN,TP]]`; **detection delay** = time from incident onset to the *end* of the first correctly flagged window (so ≥ one window); incident recall = incidents with ≥ 1 flagged window. Undefined ratios are `null`/"n/a", never 0. Without ground truth, precision/recall/F1/FPR are reported unavailable and only unsupervised diagnostics (flag rate, severity mix, detector agreement) are produced.
Streaming performance: aggregation events/s and detector ms/window per run (`results.performance`); ingest/processing throughput from `scripts/benchmark.py pipeline`.

## Reproducibility
Each experiment row stores: id, config (+SHA-256), dataset sha256 and time range, detector names/versions/configs, ML feature list, library versions, seed (also the Isolation Forest `random_state`), start/finish times, status, results and limitations. Same dataset + config ⇒ identical detector results (test `test_experiment_labeled_unlabeled_reproducible_and_exports`). Export: JSON, CSV, Markdown summary (API and UI).

## Measured results (3 seeds × 65 min; `experiments/results/detectors.json`; this machine, single run each)
| detector | precision | recall | F1 | FPR | incidents found (of 3, per seed) |
|---|---|---|---|---|---|
| rule | 0.907 | 0.708 | 0.795 | 0.0030 | 3, 3, 3 |
| zscore | 0.720 | 0.854 | 0.781 | 0.0135 | 3, 3, 3 |
| mad | 0.739 | 0.812 | 0.774 | 0.0116 | 3, 3, 3 |
| iqr | 0.724 | 0.862 | 0.787 | 0.0133 | 3, 3, 3 |
| baseline_deviation | 0.495 | 0.865 | 0.630 | 0.0358 | 3, 3, 3 |
| rate_of_change | 0.310 | 0.299 | 0.305 | 0.0270 | 3, 3, 3 |
| isolation_forest | 0.220 | 0.195 | 0.206 | 0.0283 | 1, 2, 1 |

**Reading it honestly.** H1 is supported on this data (rules: best precision and lowest FPR, lowest recall among the good detectors). H2 is *not* supported: MAD and IQR are within noise of z-score here (the injected faults are large and baselines are clean, so robustness has little to buy). H3 is consistent with the data, but with a caveat: the Isolation Forest was run with a short training prefix (`min_train=40` windows) and default settings; it was **not tuned**, so this says little about the method in general. `rate_of_change` and `baseline_deviation` are noisy on 30 s windows and are not in the default live set.

## Limitations and bias
Synthetic data with idealised, well-separated, rectangular faults; faults were designed by the same author as the detectors. Three seeds is a small sample (no confidence intervals computed). Window-level scoring rewards detectors that flag many windows of a long incident. Thresholds are defaults, not tuned or cross-validated. Delay is quantised to the window size. Seasonal detection is implemented and unit-tested (`SeasonalBaselineDetector`) but not benchmarked: it needs several repeated periods, which the dataset does not contain. Results do not transfer to real traffic without re-evaluation.

## Reproduce
```bash
python scripts/benchmark.py detectors     # ~1 min; rewrites experiments/results/detectors.json
python scripts/benchmark.py pipeline      # throughput (see docs/LIMITATIONS.md for the figures obtained here)
```
