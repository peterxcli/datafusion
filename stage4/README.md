# Shared queue: [PR #8](https://github.com/peterxcli/datafusion/pull/8)

Compared with Earlier prefetch. Negative elapsed-time changes mean faster. Warm values are separate passes; cold values use three evicted-cache runs.

[Methodology and full results](../report.md) · [Keep / defer / drop recommendations](../DECISIONS.md)

| Query | Warm pass 1 / 2 change | Evicted-cache change | CPU chart vs parent | CPU chart vs baseline |
|---|---:|---:|---|---|
| Q01 | +3.15% / +5.21% | +6.98% | ![warm](q00.png) | ![warm](../baseline-stage4/q00.png) |
| Q02 | +14.99% / +6.35% | -8.37% | ![warm](q01.png) | ![warm](../baseline-stage4/q01.png) |
| Q03 | +4.83% / +5.17% | -3.33% | ![warm](q02.png) | ![warm](../baseline-stage4/q02.png) |
| Q04 | +3.40% / +0.56% | -14.63% | ![warm](q03.png) | ![warm](../baseline-stage4/q03.png) |
| Q05 | +0.90% / -2.93% | -7.89% | ![warm](q04.png) | ![warm](../baseline-stage4/q04.png) |
| Q06 | +0.39% / +1.64% | +0.76% | ![warm](q05.png) | ![warm](../baseline-stage4/q05.png) |
| Q07 | +1.24% / -0.02% | +1.39% | ![warm](q06.png) | ![warm](../baseline-stage4/q06.png) |
| Q08 | +14.99% / +8.92% | -4.71% | ![warm](q07.png) | ![warm](../baseline-stage4/q07.png) |
| Q09 | +1.84% / +0.50% | -1.06% | ![warm](q08.png) | ![warm](../baseline-stage4/q08.png) |
| Q10 | +4.15% / +3.06% | +1.21% | ![warm](q09.png) | ![warm](../baseline-stage4/q09.png) |
| Q11 | +0.48% / +2.34% | -4.69% | ![warm](q10.png) | ![warm](../baseline-stage4/q10.png) |
| Q12 | -0.11% / +3.75% | -0.25% | ![warm](q11.png) | ![warm](../baseline-stage4/q11.png) |
| Q13 | +0.88% / +0.60% | -0.89% | ![warm](q12.png) | ![warm](../baseline-stage4/q12.png) |
| Q14 | -0.54% / +2.73% | +0.34% | ![warm](q13.png) | ![warm](../baseline-stage4/q13.png) |
| Q15 | +3.75% / -0.78% | -2.96% | ![warm](q14.png) | ![warm](../baseline-stage4/q14.png) |
| Q16 | -0.06% / +5.38% | +6.11% | ![warm](q15.png) | ![warm](../baseline-stage4/q15.png) |
| Q17 | -1.42% / +1.52% | -0.67% | ![warm](q16.png) | ![warm](../baseline-stage4/q16.png) |
| Q18 | -0.47% / -0.52% | -1.39% | ![warm](q17.png) | ![warm](../baseline-stage4/q17.png) |
| Q19 | +2.69% / -5.03% | +0.06% | ![warm](q18.png) | ![warm](../baseline-stage4/q18.png) |
| Q20 | +0.76% / +2.82% | -9.96% | ![warm](q19.png) | ![warm](../baseline-stage4/q19.png) |
| Q21 | -0.49% / -1.28% | -5.59% | ![warm](q20.png) · ![cold](cold-q20.png) | ![warm](../baseline-stage4/q20.png) · ![cold](../baseline-stage4/cold-q20.png) |
| Q22 | -2.03% / +4.08% | -2.01% | ![warm](q21.png) | ![warm](../baseline-stage4/q21.png) |
| Q23 | -0.09% / +3.61% | -2.69% | ![warm](q22.png) | ![warm](../baseline-stage4/q22.png) |
| Q24 | +59.42% / +53.94% | +27.75% | ![warm](q23.png) | ![warm](../baseline-stage4/q23.png) |
| Q25 | +17.87% / +22.08% | -3.52% | ![warm](q24.png) | ![warm](../baseline-stage4/q24.png) |
| Q26 | +2.66% / -1.28% | +1.47% | ![warm](q25.png) | ![warm](../baseline-stage4/q25.png) |
| Q27 | +21.99% / +25.51% | +5.32% | ![warm](q26.png) | ![warm](../baseline-stage4/q26.png) |
| Q28 | -2.46% / -1.05% | -4.54% | ![warm](q27.png) | ![warm](../baseline-stage4/q27.png) |
| Q29 | +0.15% / +3.30% | +4.13% | ![warm](q28.png) · ![cold](cold-q28.png) | ![warm](../baseline-stage4/q28.png) · ![cold](../baseline-stage4/cold-q28.png) |
| Q30 | +4.72% / +1.99% | -12.36% | ![warm](q29.png) | ![warm](../baseline-stage4/q29.png) |
| Q31 | -0.61% / +1.01% | -1.53% | ![warm](q30.png) | ![warm](../baseline-stage4/q30.png) |
| Q32 | +0.27% / +2.29% | -1.53% | ![warm](q31.png) | ![warm](../baseline-stage4/q31.png) |
| Q33 | -1.98% / -1.97% | -2.44% | ![warm](q32.png) | ![warm](../baseline-stage4/q32.png) |
| Q34 | -3.65% / -2.94% | -0.07% | ![warm](q33.png) | ![warm](../baseline-stage4/q33.png) |
| Q35 | +1.77% / +3.14% | +1.23% | ![warm](q34.png) · ![cold](cold-q34.png) | ![warm](../baseline-stage4/q34.png) · ![cold](../baseline-stage4/cold-q34.png) |
| Q36 | +0.74% / +0.87% | -3.52% | ![warm](q35.png) | ![warm](../baseline-stage4/q35.png) |
| Q37 | +4.14% / +4.45% | +18.21% | ![warm](q36.png) | ![warm](../baseline-stage4/q36.png) |
| Q38 | -0.91% / +3.95% | +1.90% | ![warm](q37.png) | ![warm](../baseline-stage4/q37.png) |
| Q39 | +9.16% / +6.36% | -11.84% | ![warm](q38.png) | ![warm](../baseline-stage4/q38.png) |
| Q40 | +8.34% / -6.96% | -0.79% | ![warm](q39.png) | ![warm](../baseline-stage4/q39.png) |
| Q41 | +34.69% / +41.43% | +4.81% | ![warm](q40.png) | ![warm](../baseline-stage4/q40.png) |
| Q42 | +1.16% / +70.58% | -7.55% | ![warm](q41.png) | ![warm](../baseline-stage4/q41.png) |
| Q43 | +8.29% / +10.76% | +18.55% | ![warm](q42.png) | ![warm](../baseline-stage4/q42.png) |
