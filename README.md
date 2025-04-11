# Curtailed-2-Compute
Leveraging Curtailed Renewable Energy for Datacenter Computing

# Curtailed-2-Compute
Leveraging Curtailed Renewable Energy for Datacenter Computing

## I. Data Sources 
#### CAISO Curtailment Data Description
- 5-minute interval data on renewable energy production and curtailment within California’s electricity grid
- Only wind and solar resources can be reported in this manner because these resources have a
forecast
- Other renewables (like geothermal or biomass) are baseload and rarely curtailed.
- Curtailment is defined as the difference between actual production and the forecast 
when actual production is less than the forecast
- Data files are available here:  
[CAISO Curtailment Reports](https://www.caiso.com/informed/Pages/ManagingOversupply.aspx)

Each file typically includes two sheets:

---

### Sheet 1: `Production`

This sheet contains **real-time energy supply and demand** data recorded in 5-minute intervals.

| Column | Description |
|--------|-------------|
| `Date` | Date of the record |
| `Hour` | Hour of the day (1–24) |
| `Interval` | 5-minute interval within the hour (1–12) |
| `Load` | Actual electricity demand (MW) |
| `Net Load` | Load minus wind and solar production (MW) |
| `Solar` | Solar generation at that interval (MW) |
| `Wind` | Wind generation at that interval (MW) |
| `Renewables` | Total renewable production (solar, wind, biomass, geothermal, small hydro) |
| `Thermal` | Generation from natural gas and other thermal resources (excluding nuclear) |
| `Nuclear` | Nuclear generation (MW) |
| `Large Hydro` | Large-scale hydropower generation (not included in `Renewables`) |
| `Imports` | Electricity imported into the CAISO grid (MW) |
| `Generation` | Total internal generation (MW) |
| `Load Less (Generation+Imports)` | Imbalance between demand and supply (sanity check) |

---

### Sheet 2: `Curtailments`

This sheet provides **records of curtailed solar and wind generation** — i.e., clean energy that could not be delivered to the grid, typically due to oversupply.

| Column | Description |
|--------|-------------|
| `Date` | Date of curtailment |
| `Hour` | Hour of the day (1–24) |
| `Interval` | 5-minute interval (1–12) |
| `Wind Curtailment` | Wind energy curtailed in that interval (MW) |
| `Solar Curtailment` | Solar energy curtailed in that interval (MW) |
| `Reason` | Indicates the type of curtailment event: <br> `Local`: Due to localized grid constraints, such as transmission congestion or limited load in a specific area <br> `System`: Due to system-wide oversupply, meaning that total renewable generation across the grid exceeded total demand |

