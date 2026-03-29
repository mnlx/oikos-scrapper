# Neighborhood and City Signals

This catalog tracks public datasets and reports that can materially affect residential pricing in Grande Florianópolis.

## Recommended signal families

- `market`: price indexes, rent yields, transaction references, ITBI signals
- `crime`: violent crime, property crime, public safety operations
- `flood_risk`: flood, landslide, erosion, contingency and risk reduction plans
- `urban_project`: road projects, bridges, drainage, urbanization, PAC investments
- `zoning`: plan director updates, land-use and occupation rules, geospatial zoning layers
- `regularization`: REURB, title delivery, informal settlement upgrading
- `health_access`: hospitals, clinics, beds, emergency coverage
- `education_access`: schools, enrollment, school infrastructure

## Priority public sources

### Florianópolis

- `GeoFloripa / GeoPortal`
  - Publisher: Prefeitura de Florianópolis / REPLAN
  - Type: geospatial portal
  - Use: zoning, official cartography, address validation, urban restrictions, official geodata layers

- `Floripa Para Todos`
  - Publisher: Prefeitura de Florianópolis
  - Type: program / project documentation
  - Use: public works, urban rehabilitation, housing and mobility projects

- `Floripa 400`
  - Publisher: Prefeitura de Florianópolis / REPLAN
  - Type: strategic planning program
  - Use: future neighborhood interventions, OODC-linked investments, long-range planning signals

### São José

- `Observatório Imobiliário de São José`
  - Publisher: Prefeitura de São José
  - Type: municipal real-estate observatory
  - Use: valuation references, map-based property evidence, ITBI and cadastral pricing support

- `Plano Diretor e legislação de uso do solo`
  - Publisher: Prefeitura de São José
  - Type: legal / planning documents
  - Use: zoning and density changes, large-project rules, corridor effects

- `Plano Municipal de Redução de Riscos / Defesa Civil`
  - Publisher: Prefeitura de São José
  - Type: risk mapping and mitigation planning
  - Use: flood and landslide exposure by neighborhood

### Palhoça

- `Programa de Prevenção de Enchentes`
  - Publisher: Prefeitura de Palhoça
  - Type: municipal operations / public works
  - Use: drainage interventions and flood mitigation by locality

- `Lar Legal / regularização fundiária`
  - Publisher: Prefeitura de Palhoça
  - Type: housing regularization program
  - Use: title issuance and formalization progress

### Biguaçu

- `REURB Social de Biguaçu`
  - Publisher: Prefeitura de Biguaçu
  - Type: regularization / urbanization
  - Use: formalization and infrastructure effects in vulnerable areas

### Cross-municipality and state/federal

- `IBGE Localidades API`
  - Type: API
  - Use: canonical city and territorial identifiers

- `IBGE SIDRA`
  - Type: statistical API / tabular service
  - Use: income, housing, demographics, density and socioeconomic baselines

- `SSP-SC Boletim Mensal de Indicadores`
  - Type: monthly report
  - Use: municipality-level violent crime and property crime signals

- `Defesa Civil SC`
  - Type: warnings, geological notices, state plans
  - Use: rainfall, flood and landslide risk for Grande Florianópolis

- `Periferia Viva / Novo PAC`
  - Publisher: Ministério das Cidades / Casa Civil
  - Type: federal program data
  - Use: urbanization and mobility projects with direct neighborhood impacts

- `CNES / OpenDataSUS`
  - Type: API / downloadable datasets
  - Use: health facility density, emergency coverage, hospital access

## Suggested first ingestion strategy

1. Seed municipality-level signals from IBGE, SSP-SC, Defesa Civil SC, CNES and federal PAC sources.
2. Add neighborhood-level signals where municipalities expose named neighborhoods or sectors.
3. Join geospatial layers later to infer neighborhood coverage from coordinates.

## Suggested initial records

- São José monthly crime deltas from SSP-SC reports
- São José risk sectors from PMRR / Defesa Civil
- Florianópolis zoning and public-investment layers from GeoFloripa and Floripa 400
- Biguaçu and São José Periferia Viva interventions
- Palhoça flood-prevention actions and meteorological monitoring
