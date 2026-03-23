# Data Directory

This directory is intentionally a mix of:

- small derived files that are appropriate to keep in the public repository
- local folders where users place raw CAISO downloads when reproducing the workflow

## Included in the public repository

The public release can include small support files that are directly referenced by the canonical notebook workflow:

- `caiso_wind_solar_pcm.csv`
- `congestion_costs.csv`
- `local_generation_mix_fresno.csv`
- `transmission_project_costs.csv`

The lightweight reproducibility assets live in `data/processed/`:

- `vector_high_curtailment_week.csv`
- `vector_high_volatility_week.csv`

These files are compact inputs or derived products that help users run the public workflow without committing the full raw CAISO archive.

## Expected local raw inputs

These files should be downloaded separately and kept local unless you have explicitly decided to publish them:

- `productionandcurtailmentsdata_YYYY.xlsx` in `data/`
- `YYYYMM_LMP.csv` in `data/LMP_Data/`

See `../DATA_SOURCES.md` for the source systems and expected filename patterns.

## Keep Out of the Public Release by Default

Unless redistribution is reviewed, do not commit:

- raw CAISO workbooks and monthly LMP exports
- third-party PDFs and reports
- temporary Office lock files such as `~$*.xlsx`
- scratch data pulls that were only used during exploratory analysis

When in doubt, link to the original source and document how to download the file instead of committing it.
