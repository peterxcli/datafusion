# Earlier prefetch: [PR #7](https://github.com/peterxcli/datafusion/pull/7)

Compared with Bounded prefetch. Negative elapsed-time changes mean faster. Warm values are separate passes; cold values use three evicted-cache runs.

[Methodology and full results](../report.md) · [Keep / defer / drop recommendations](../DECISIONS.md)

| Query | Warm pass 1 / 2 change | Evicted-cache change | CPU chart vs parent | CPU chart vs baseline |
|---|---:|---:|---|---|
| Q01 | -2.12% / -4.83% | -1.86% | [warm](q00.png) | [warm](../baseline-stage3/q00.png) |
| Q02 | -8.16% / -3.27% | -7.36% | [warm](q01.png) | [warm](../baseline-stage3/q01.png) |
| Q03 | -1.25% / -1.58% | -7.24% | [warm](q02.png) | [warm](../baseline-stage3/q02.png) |
| Q04 | +0.20% / -1.97% | +1.17% | [warm](q03.png) | [warm](../baseline-stage3/q03.png) |
| Q05 | -2.23% / +2.96% | -2.84% | [warm](q04.png) | [warm](../baseline-stage3/q04.png) |
| Q06 | +1.37% / +1.67% | -1.71% | [warm](q05.png) | [warm](../baseline-stage3/q05.png) |
| Q07 | -1.96% / -1.10% | -1.68% | [warm](q06.png) | [warm](../baseline-stage3/q06.png) |
| Q08 | -6.47% / +3.35% | -8.12% | [warm](q07.png) | [warm](../baseline-stage3/q07.png) |
| Q09 | -0.43% / -0.74% | +0.58% | [warm](q08.png) | [warm](../baseline-stage3/q08.png) |
| Q10 | -1.29% / -0.68% | +2.04% | [warm](q09.png) | [warm](../baseline-stage3/q09.png) |
| Q11 | -0.38% / +0.56% | -0.50% | [warm](q10.png) | [warm](../baseline-stage3/q10.png) |
| Q12 | -1.51% / -1.89% | -0.91% | [warm](q11.png) | [warm](../baseline-stage3/q11.png) |
| Q13 | -2.93% / +1.67% | -1.66% | [warm](q12.png) | [warm](../baseline-stage3/q12.png) |
| Q14 | -6.86% / -1.97% | -2.03% | [warm](q13.png) | [warm](../baseline-stage3/q13.png) |
| Q15 | -4.62% / -1.62% | +0.35% | [warm](q14.png) | [warm](../baseline-stage3/q14.png) |
| Q16 | -0.33% / -0.73% | -2.56% | [warm](q15.png) | [warm](../baseline-stage3/q15.png) |
| Q17 | +0.41% / +0.60% | -0.05% | [warm](q16.png) | [warm](../baseline-stage3/q16.png) |
| Q18 | +0.40% / -0.72% | -0.09% | [warm](q17.png) | [warm](../baseline-stage3/q17.png) |
| Q19 | -1.41% / +4.15% | -0.17% | [warm](q18.png) | [warm](../baseline-stage3/q18.png) |
| Q20 | -7.09% / -8.20% | -14.74% | [warm](q19.png) | [warm](../baseline-stage3/q19.png) |
| Q21 | -7.45% / -5.88% | -14.90% | [warm](q20.png) · [cold](cold-q20.png) | [warm](../baseline-stage3/q20.png) · [cold](../baseline-stage3/cold-q20.png) |
| Q22 | -2.70% / -4.55% | -13.73% | [warm](q21.png) | [warm](../baseline-stage3/q21.png) |
| Q23 | -3.48% / -6.16% | -11.90% | [warm](q22.png) | [warm](../baseline-stage3/q22.png) |
| Q24 | -1.72% / +1.73% | -3.07% | [warm](q23.png) | [warm](../baseline-stage3/q23.png) |
| Q25 | +0.89% / -5.68% | -7.95% | [warm](q24.png) | [warm](../baseline-stage3/q24.png) |
| Q26 | -1.08% / -0.39% | -9.70% | [warm](q25.png) | [warm](../baseline-stage3/q25.png) |
| Q27 | +1.75% / -0.32% | -0.62% | [warm](q26.png) | [warm](../baseline-stage3/q26.png) |
| Q28 | -0.51% / -0.61% | -6.39% | [warm](q27.png) | [warm](../baseline-stage3/q27.png) |
| Q29 | +0.27% / +0.13% | +0.13% | [warm](q28.png) · [cold](cold-q28.png) | [warm](../baseline-stage3/q28.png) · [cold](../baseline-stage3/cold-q28.png) |
| Q30 | -1.88% / -1.20% | +2.03% | [warm](q29.png) | [warm](../baseline-stage3/q29.png) |
| Q31 | -0.05% / -0.84% | -2.69% | [warm](q30.png) | [warm](../baseline-stage3/q30.png) |
| Q32 | +1.87% / +2.91% | -1.52% | [warm](q31.png) | [warm](../baseline-stage3/q31.png) |
| Q33 | -1.23% / +0.21% | +0.41% | [warm](q32.png) | [warm](../baseline-stage3/q32.png) |
| Q34 | +4.00% / -1.15% | +1.42% | [warm](q33.png) | [warm](../baseline-stage3/q33.png) |
| Q35 | +3.01% / -1.61% | -2.01% | [warm](q34.png) · [cold](cold-q34.png) | [warm](../baseline-stage3/q34.png) · [cold](../baseline-stage3/cold-q34.png) |
| Q36 | -0.11% / -1.49% | +3.04% | [warm](q35.png) | [warm](../baseline-stage3/q35.png) |
| Q37 | -3.95% / +16.52% | -6.37% | [warm](q36.png) | [warm](../baseline-stage3/q36.png) |
| Q38 | +6.44% / +26.91% | -7.99% | [warm](q37.png) | [warm](../baseline-stage3/q37.png) |
| Q39 | -6.07% / +37.50% | +9.19% | [warm](q38.png) | [warm](../baseline-stage3/q38.png) |
| Q40 | -4.95% / +5.21% | -1.44% | [warm](q39.png) | [warm](../baseline-stage3/q39.png) |
| Q41 | -6.64% / -28.37% | -1.25% | [warm](q40.png) | [warm](../baseline-stage3/q40.png) |
| Q42 | +0.39% / -6.37% | +31.13% | [warm](q41.png) | [warm](../baseline-stage3/q41.png) |
| Q43 | -1.41% / +6.10% | +3.39% | [warm](q42.png) | [warm](../baseline-stage3/q42.png) |
