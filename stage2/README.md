# Bounded prefetch: [PR #5](https://github.com/peterxcli/datafusion/pull/5)

Compared with I/O policy. Negative elapsed-time changes mean faster. Warm values are separate passes; cold values use three evicted-cache runs.

[Methodology and full results](../report.md) · [Keep / defer / drop recommendations](../DECISIONS.md)

| Query | Warm pass 1 / 2 change | Evicted-cache change | CPU chart vs parent | CPU chart vs baseline |
|---|---:|---:|---|---|
| Q01 | +2.66% / +5.27% | +1.41% | [warm](q00.png) | [warm](../baseline-stage2/q00.png) |
| Q02 | +22.47% / -12.96% | +5.37% | [warm](q01.png) | [warm](../baseline-stage2/q01.png) |
| Q03 | -1.71% / -2.49% | -16.99% | [warm](q02.png) | [warm](../baseline-stage2/q02.png) |
| Q04 | -11.87% / -10.05% | -19.29% | [warm](q03.png) | [warm](../baseline-stage2/q03.png) |
| Q05 | +0.41% / -2.31% | +2.48% | [warm](q04.png) | [warm](../baseline-stage2/q04.png) |
| Q06 | -1.28% / -4.69% | -3.21% | [warm](q05.png) | [warm](../baseline-stage2/q05.png) |
| Q07 | +1.90% / +2.24% | +0.51% | [warm](q06.png) | [warm](../baseline-stage2/q06.png) |
| Q08 | -14.31% / -5.92% | +0.21% | [warm](q07.png) | [warm](../baseline-stage2/q07.png) |
| Q09 | -1.82% / +0.04% | -4.46% | [warm](q08.png) | [warm](../baseline-stage2/q08.png) |
| Q10 | +3.17% / -0.79% | -6.76% | [warm](q09.png) | [warm](../baseline-stage2/q09.png) |
| Q11 | -5.31% / -0.36% | -5.91% | [warm](q10.png) | [warm](../baseline-stage2/q10.png) |
| Q12 | -0.53% / -1.63% | -7.13% | [warm](q11.png) | [warm](../baseline-stage2/q11.png) |
| Q13 | +4.87% / -1.39% | -2.01% | [warm](q12.png) | [warm](../baseline-stage2/q12.png) |
| Q14 | +3.67% / -0.29% | -6.22% | [warm](q13.png) | [warm](../baseline-stage2/q13.png) |
| Q15 | +0.68% / +1.34% | -2.81% | [warm](q14.png) | [warm](../baseline-stage2/q14.png) |
| Q16 | -1.02% / -0.97% | -3.88% | [warm](q15.png) | [warm](../baseline-stage2/q15.png) |
| Q17 | -1.25% / -1.66% | -3.16% | [warm](q16.png) | [warm](../baseline-stage2/q16.png) |
| Q18 | -1.20% / +0.86% | -2.46% | [warm](q17.png) | [warm](../baseline-stage2/q17.png) |
| Q19 | -2.68% / -3.67% | -0.58% | [warm](q18.png) | [warm](../baseline-stage2/q18.png) |
| Q20 | -0.12% / +1.92% | -0.41% | [warm](q19.png) | [warm](../baseline-stage2/q19.png) |
| Q21 | +0.43% / +0.30% | +0.00% | [warm](q20.png) · [cold](cold-q20.png) | [warm](../baseline-stage2/q20.png) · [cold](../baseline-stage2/cold-q20.png) |
| Q22 | -1.45% / +0.95% | +0.39% | [warm](q21.png) | [warm](../baseline-stage2/q21.png) |
| Q23 | -1.73% / +1.19% | -0.03% | [warm](q22.png) | [warm](../baseline-stage2/q22.png) |
| Q24 | -1.03% / +5.62% | -1.33% | [warm](q23.png) | [warm](../baseline-stage2/q23.png) |
| Q25 | +1.74% / +5.21% | +4.10% | [warm](q24.png) | [warm](../baseline-stage2/q24.png) |
| Q26 | +0.82% / -6.52% | -0.84% | [warm](q25.png) | [warm](../baseline-stage2/q25.png) |
| Q27 | -4.88% / +1.14% | -5.14% | [warm](q26.png) | [warm](../baseline-stage2/q26.png) |
| Q28 | -2.35% / -4.23% | -7.04% | [warm](q27.png) | [warm](../baseline-stage2/q27.png) |
| Q29 | -0.75% / -0.68% | -2.51% | [warm](q28.png) · [cold](cold-q28.png) | [warm](../baseline-stage2/q28.png) · [cold](../baseline-stage2/cold-q28.png) |
| Q30 | -3.60% / -3.72% | -14.90% | [warm](q29.png) | [warm](../baseline-stage2/q29.png) |
| Q31 | -2.35% / -0.31% | -7.92% | [warm](q30.png) | [warm](../baseline-stage2/q30.png) |
| Q32 | -2.39% / -0.06% | -8.16% | [warm](q31.png) | [warm](../baseline-stage2/q31.png) |
| Q33 | +0.09% / -1.42% | -0.60% | [warm](q32.png) | [warm](../baseline-stage2/q32.png) |
| Q34 | -2.61% / -0.88% | -2.85% | [warm](q33.png) | [warm](../baseline-stage2/q33.png) |
| Q35 | -7.89% / -1.58% | -1.33% | [warm](q34.png) · [cold](cold-q34.png) | [warm](../baseline-stage2/q34.png) · [cold](../baseline-stage2/cold-q34.png) |
| Q36 | -1.90% / +1.48% | -5.98% | [warm](q35.png) | [warm](../baseline-stage2/q35.png) |
| Q37 | +2.09% / -14.30% | -1.14% | [warm](q36.png) | [warm](../baseline-stage2/q36.png) |
| Q38 | +14.81% / -16.53% | +2.68% | [warm](q37.png) | [warm](../baseline-stage2/q37.png) |
| Q39 | +2.17% / -25.69% | +3.05% | [warm](q38.png) | [warm](../baseline-stage2/q38.png) |
| Q40 | +0.22% / -2.85% | +2.93% | [warm](q39.png) | [warm](../baseline-stage2/q39.png) |
| Q41 | -17.10% / +5.65% | -2.93% | [warm](q40.png) | [warm](../baseline-stage2/q40.png) |
| Q42 | -6.53% / -25.57% | -3.43% | [warm](q41.png) | [warm](../baseline-stage2/q41.png) |
| Q43 | +11.04% / -9.21% | +1.10% | [warm](q42.png) | [warm](../baseline-stage2/q42.png) |
