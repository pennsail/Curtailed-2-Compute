# Data Directory

## Included Files

Supporting inputs referenced by the notebook workflow, extracted from the
[CAISO Revised Draft 2024-2025 Transmission Plan](https://www.caiso.com/documents/revised-draft-2024-2025-transmission-plan.pdf):

- `caiso_wind_solar_pcm.csv` — wind and solar curtailment by renewable zone (Table 4.6-2)
- `congestion_costs.csv` — congestion costs and durations by constrained area (Appendix G)
- `local_generation_mix_fresno.csv` — Fresno-area resource capacity mix by type
- `transmission_project_costs.csv` — approved transmission project cost estimates

Derived reproducibility assets in `data/processed/`:

- `vector_high_curtailment_week.csv` — representative 168-hour high-curtailment week
- `vector_high_volatility_week.csv` — representative 168-hour high-price-volatility week

## External Raw Inputs

To run the full analysis from scratch, download these separately:

- `productionandcurtailmentsdata_YYYY.xlsx` — place in `data/` ([CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx))
- `YYYYMM_LMP.csv` — place in `data/LMP_Data/` ([CAISO OASIS](http://oasis.caiso.com/))

See `../DATA_SOURCES.md` for full details on source systems and field descriptions.
