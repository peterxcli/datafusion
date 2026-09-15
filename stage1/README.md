# I/O policy: [PR #4](https://github.com/peterxcli/datafusion/pull/4)

Compared with Baseline. Negative elapsed-time changes mean faster. Warm values are separate passes; cold values use three evicted-cache runs.

[Methodology and full results](../report.md) · [Keep / defer / drop recommendations](../DECISIONS.md)

| Query | Warm pass 1 / 2 change | Evicted-cache change | CPU chart vs parent | CPU chart vs baseline |
|---|---:|---:|---|---|
| Q01 | -2.96% / -1.80% | +0.69% | [warm](q00.png) | [warm](q00.png) |
| Q02 | -25.75% / +12.98% | -4.56% | [warm](q01.png) | [warm](q01.png) |
| Q03 | -1.23% / -0.23% | +7.62% | [warm](q02.png) | [warm](q02.png) |
| Q04 | -0.56% / -2.04% | -0.84% | [warm](q03.png) | [warm](q03.png) |
| Q05 | -0.14% / +3.37% | -6.52% | [warm](q04.png) | [warm](q04.png) |
| Q06 | -1.48% / +2.93% | -2.86% | [warm](q05.png) | [warm](q05.png) |
| Q07 | -1.77% / -1.98% | +0.66% | [warm](q06.png) | [warm](q06.png) |
| Q08 | +17.77% / +5.56% | +4.75% | [warm](q07.png) | [warm](q07.png) |
| Q09 | +0.37% / +0.38% | -0.19% | [warm](q08.png) | [warm](q08.png) |
| Q10 | -1.94% / +0.64% | -0.49% | [warm](q09.png) | [warm](q09.png) |
| Q11 | +0.76% / -5.25% | -5.69% | [warm](q10.png) | [warm](q10.png) |
| Q12 | -4.88% / -2.82% | -4.51% | [warm](q11.png) | [warm](q11.png) |
| Q13 | -4.09% / +0.48% | +0.68% | [warm](q12.png) | [warm](q12.png) |
| Q14 | -5.95% / -4.66% | +3.12% | [warm](q13.png) | [warm](q13.png) |
| Q15 | +0.21% / -0.07% | -2.08% | [warm](q14.png) | [warm](q14.png) |
| Q16 | -0.53% / -0.66% | +2.88% | [warm](q15.png) | [warm](q15.png) |
| Q17 | +0.27% / +0.79% | -1.32% | [warm](q16.png) | [warm](q16.png) |
| Q18 | -0.20% / -0.59% | -0.25% | [warm](q17.png) | [warm](q17.png) |
| Q19 | +3.67% / +5.07% | -0.88% | [warm](q18.png) | [warm](q18.png) |
| Q20 | +0.64% / -2.47% | -2.39% | [warm](q19.png) | [warm](q19.png) |
| Q21 | +0.98% / +1.24% | +0.26% | [warm](q20.png) · [cold](cold-q20.png) | [warm](q20.png) · [cold](cold-q20.png) |
| Q22 | +1.08% / -0.19% | -0.76% | [warm](q21.png) | [warm](q21.png) |
| Q23 | +3.15% / +1.84% | +2.64% | [warm](q22.png) | [warm](q22.png) |
| Q24 | +40.05% / +36.27% | +62.83% | [warm](q23.png) | [warm](q23.png) |
| Q25 | -1.02% / -3.13% | -4.45% | [warm](q24.png) | [warm](q24.png) |
| Q26 | -0.45% / +6.29% | +1.60% | [warm](q25.png) | [warm](q25.png) |
| Q27 | +0.77% / -2.06% | +1.94% | [warm](q26.png) | [warm](q26.png) |
| Q28 | -1.14% / +0.41% | -0.70% | [warm](q27.png) | [warm](q27.png) |
| Q29 | +0.02% / +0.39% | +0.36% | [warm](q28.png) · [cold](cold-q28.png) | [warm](q28.png) · [cold](cold-q28.png) |
| Q30 | +2.54% / +1.02% | +6.56% | [warm](q29.png) | [warm](q29.png) |
| Q31 | +1.94% / -1.17% | -1.04% | [warm](q30.png) | [warm](q30.png) |
| Q32 | -6.89% / -2.25% | -0.70% | [warm](q31.png) | [warm](q31.png) |
| Q33 | +1.56% / +1.10% | +0.20% | [warm](q32.png) | [warm](q32.png) |
| Q34 | -0.38% / +0.60% | -3.34% | [warm](q33.png) | [warm](q33.png) |
| Q35 | +4.61% / -2.63% | -0.92% | [warm](q34.png) · [cold](cold-q34.png) | [warm](q34.png) · [cold](cold-q34.png) |
| Q36 | +0.64% / -0.14% | -3.27% | [warm](q35.png) | [warm](q35.png) |
| Q37 | -4.38% / +10.32% | -2.91% | [warm](q36.png) | [warm](q36.png) |
| Q38 | -24.93% / -5.29% | +8.79% | [warm](q37.png) | [warm](q37.png) |
| Q39 | -2.05% / -6.85% | +0.74% | [warm](q38.png) | [warm](q38.png) |
| Q40 | +3.53% / +5.66% | -2.73% | [warm](q39.png) | [warm](q39.png) |
| Q41 | -5.88% / -4.79% | -11.88% | [warm](q40.png) | [warm](q40.png) |
| Q42 | -1.15% / -9.22% | -7.35% | [warm](q41.png) | [warm](q41.png) |
| Q43 | -0.08% / +5.21% | -9.61% | [warm](q42.png) | [warm](q42.png) |
