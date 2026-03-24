# Data Directory

## Included Files

Supporting inputs referenced by the notebook workflow:

- `caiso_wind_solar_pcm.csv`
- `congestion_costs.csv`
- `local_generation_mix_fresno.csv`
- `transmission_project_costs.csv`

Derived reproducibility assets in `data/processed/`:

- `vector_high_curtailment_week.csv` — representative 168-hour high-curtailment week
- `vector_high_volatility_week.csv` — representative 168-hour high-price-volatility week

## External Raw Inputs

To run the full analysis from scratch, download these separately:

- `productionandcurtailmentsdata_YYYY.xlsx` — place in `data/` ([CAISO Managing Oversupply](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx))
- `YYYYMM_LMP.csv` — place in `data/LMP_Data/` ([CAISO OASIS](http://oasis.caiso.com/))

See `../DATA_SOURCES.md` for full details on source systems and field descriptions.
