# Memory governor: [PR #9](https://github.com/peterxcli/datafusion/pull/9)

Compared with Shared queue. Negative elapsed-time changes mean faster. Warm values are separate passes; cold values use three evicted-cache runs.

[Methodology and full results](../report.md) · [Keep / defer / drop recommendations](../DECISIONS.md)

| Query | Warm pass 1 / 2 change | Evicted-cache change | CPU chart vs parent | CPU chart vs baseline |
|---|---:|---:|---|---|
| Q01 | -0.86% / -2.55% | -3.21% | [warm](q00.png) | [warm](../baseline-stage5/q00.png) |
| Q02 | -12.13% / +8.23% | -1.35% | [warm](q01.png) | [warm](../baseline-stage5/q01.png) |
| Q03 | -2.17% / -1.49% | +2.98% | [warm](q02.png) | [warm](../baseline-stage5/q02.png) |
| Q04 | -0.53% / +1.56% | +0.93% | [warm](q03.png) | [warm](../baseline-stage5/q03.png) |
| Q05 | -0.81% / -3.63% | +1.22% | [warm](q04.png) | [warm](../baseline-stage5/q04.png) |
| Q06 | +0.66% / -2.23% | -1.33% | [warm](q05.png) | [warm](../baseline-stage5/q05.png) |
| Q07 | +0.95% / +1.96% | +2.10% | [warm](q06.png) | [warm](../baseline-stage5/q06.png) |
| Q08 | -3.08% / -8.75% | -0.05% | [warm](q07.png) | [warm](../baseline-stage5/q07.png) |
| Q09 | -0.87% / -0.51% | -1.09% | [warm](q08.png) | [warm](../baseline-stage5/q08.png) |
| Q10 | -2.22% / +0.11% | -0.34% | [warm](q09.png) | [warm](../baseline-stage5/q09.png) |
| Q11 | +0.07% / -3.10% | +0.39% | [warm](q10.png) | [warm](../baseline-stage5/q10.png) |
| Q12 | -0.12% / +4.08% | -4.84% | [warm](q11.png) | [warm](../baseline-stage5/q11.png) |
| Q13 | -0.53% / +6.81% | +1.23% | [warm](q12.png) | [warm](../baseline-stage5/q12.png) |
| Q14 | +3.96% / +0.44% | +0.82% | [warm](q13.png) | [warm](../baseline-stage5/q13.png) |
| Q15 | -0.63% / +1.93% | -1.12% | [warm](q14.png) | [warm](../baseline-stage5/q14.png) |
| Q16 | -0.30% / -4.50% | -3.83% | [warm](q15.png) | [warm](../baseline-stage5/q15.png) |
| Q17 | -0.02% / -0.58% | +1.45% | [warm](q16.png) | [warm](../baseline-stage5/q16.png) |
| Q18 | +0.96% / -1.68% | +0.49% | [warm](q17.png) | [warm](../baseline-stage5/q17.png) |
| Q19 | -7.16% / +4.16% | -1.26% | [warm](q18.png) | [warm](../baseline-stage5/q18.png) |
| Q20 | -2.76% / -1.45% | +1.21% | [warm](q19.png) | [warm](../baseline-stage5/q19.png) |
| Q21 | +0.07% / +1.15% | +1.67% | [warm](q20.png) · [cold](cold-q20.png) | [warm](../baseline-stage5/q20.png) · [cold](../baseline-stage5/cold-q20.png) |
| Q22 | +4.21% / -1.52% | -1.57% | [warm](q21.png) | [warm](../baseline-stage5/q21.png) |
| Q23 | -1.17% / -1.05% | +0.28% | [warm](q22.png) | [warm](../baseline-stage5/q22.png) |
| Q24 | -10.94% / +2.75% | -0.15% | [warm](q23.png) | [warm](../baseline-stage5/q23.png) |
| Q25 | -7.63% / -1.66% | +10.59% | [warm](q24.png) | [warm](../baseline-stage5/q24.png) |
| Q26 | -2.29% / -1.95% | -3.11% | [warm](q25.png) | [warm](../baseline-stage5/q25.png) |
| Q27 | -5.67% / -8.31% | -2.88% | [warm](q26.png) | [warm](../baseline-stage5/q26.png) |
| Q28 | +2.45% / +2.18% | -0.41% | [warm](q27.png) | [warm](../baseline-stage5/q27.png) |
| Q29 | -2.71% / -4.36% | -6.03% | [warm](q28.png) · [cold](cold-q28.png) | [warm](../baseline-stage5/q28.png) · [cold](../baseline-stage5/cold-q28.png) |
| Q30 | +0.01% / +5.41% | +1.35% | [warm](q29.png) | [warm](../baseline-stage5/q29.png) |
| Q31 | +2.29% / -0.84% | -1.95% | [warm](q30.png) | [warm](../baseline-stage5/q30.png) |
| Q32 | -0.10% / -4.82% | +2.18% | [warm](q31.png) | [warm](../baseline-stage5/q31.png) |
| Q33 | +0.73% / +0.14% | +0.16% | [warm](q32.png) | [warm](../baseline-stage5/q32.png) |
| Q34 | +0.42% / +2.88% | -1.20% | [warm](q33.png) | [warm](../baseline-stage5/q33.png) |
| Q35 | -2.20% / -2.19% | +0.73% | [warm](q34.png) · [cold](cold-q34.png) | [warm](../baseline-stage5/q34.png) · [cold](../baseline-stage5/cold-q34.png) |
| Q36 | -0.03% / -1.03% | +1.52% | [warm](q35.png) | [warm](../baseline-stage5/q35.png) |
| Q37 | -2.36% / -5.56% | -5.78% | [warm](q36.png) | [warm](../baseline-stage5/q36.png) |
| Q38 | -8.10% / +6.52% | +9.32% | [warm](q37.png) | [warm](../baseline-stage5/q37.png) |
| Q39 | -2.90% / -24.66% | +5.78% | [warm](q38.png) | [warm](../baseline-stage5/q38.png) |
| Q40 | -12.25% / +0.42% | +4.94% | [warm](q39.png) | [warm](../baseline-stage5/q39.png) |
| Q41 | -4.38% / +2.72% | -1.21% | [warm](q40.png) | [warm](../baseline-stage5/q40.png) |
| Q42 | -18.18% / -36.83% | -1.35% | [warm](q41.png) | [warm](../baseline-stage5/q41.png) |
| Q43 | +3.11% / +1.14% | -6.56% | [warm](q42.png) | [warm](../baseline-stage5/q42.png) |
