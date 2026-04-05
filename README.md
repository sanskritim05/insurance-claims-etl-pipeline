<!-- Improved compatibility of back to top link: See: https://github.com/othneildrew/Best-README-Template/pull/73 -->
<a id="readme-top"></a>

<!-- PROJECT SHIELDS -->
[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]
[![LinkedIn][linkedin-shield]][linkedin-url]



<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/your_username/insurance-claims-etl-pipeline">
    <img src="images/logo.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">insurance-claims-etl-pipeline</h3>

  <p align="center">
    A local Python ETL pipeline built on CMS synthetic Medicare data — extract, transform, validate, and explore claims in a Streamlit dashboard.
    <br />
    <a href="https://github.com/your_username/insurance-claims-etl-pipeline"><strong>Explore the docs »</strong></a>
    <br />
    <br />
    <a href="https://github.com/your_username/insurance-claims-etl-pipeline">View Demo</a>
    &middot;
    <a href="https://github.com/your_username/insurance-claims-etl-pipeline/issues/new?labels=bug&template=bug-report---.md">Report Bug</a>
    &middot;
    <a href="https://github.com/your_username/insurance-claims-etl-pipeline/issues/new?labels=enhancement&template=feature-request---.md">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li>
      <a href="#getting-started">Getting Started</a>
      <ul>
        <li><a href="#prerequisites">Prerequisites</a></li>
        <li><a href="#installation">Installation</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#warehouse-design">Warehouse Design</a></li>
    <li><a href="#dashboard">Dashboard</a></li>
    <li><a href="#example-sql-queries">Example SQL Queries</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Screenshot][product-screenshot]](https://example.com)

`insurance-claims-etl-pipeline` is a local Python ETL project built on CMS synthetic Medicare sample data. It extracts four source files, transforms them into a clean analytics model, validates output with Great Expectations, and loads the results into a SQLite warehouse for exploration in Streamlit.

The project demonstrates a real, end-to-end ETL workflow:

* **Extract** — read beneficiary, inpatient, outpatient, and prescription event source files from a local `data/` folder
* **Transform** — standardize dates and identifiers, derive patient and claim metrics, and model dimensions and facts
* **Validate** — run Great Expectations data quality checks on medical claims and prescription events
* **Load** — write the final warehouse tables into a local `claims_warehouse.db`

Everything runs locally using free, open source tools only. No API keys, no cloud services, and no paid infrastructure required.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



### Built With

* [![Python][Python.org]][Python-url]
* [![SQLite][SQLite.org]][SQLite-url]
* [![Pandas][Pandas.pydata.org]][Pandas-url]
* [![Streamlit][Streamlit.io]][Streamlit-url]
* [![Great Expectations][GreatExpectations.io]][GreatExpectations-url]

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- GETTING STARTED -->
## Getting Started

Follow these steps to get a local copy up and running.

### Prerequisites

* Python 3.8 or later
* The four CMS synthetic Medicare sample files (see [Data Sources](#data-sources) below)

### Installation

1. Clone the repo
   ```sh
   git clone https://github.com/your_username/insurance-claims-etl-pipeline.git
   ```
2. Create and activate a virtual environment
   ```sh
   python -m venv .venv
   source .venv/bin/activate
   ```
3. Install dependencies
   ```sh
   pip install -r requirements.txt
   ```
4. Place CMS source files in the `data/` directory and confirm they are detected
   ```sh
   python run_pipeline.py --list-files
   ```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- USAGE -->
## Usage

### Data Sources

Place these CMS files in the `data/` directory before running the pipeline:

| File | Description |
|------|-------------|
| `2008 Beneficiary Summary File` | Patient demographics, age, gender, race, ESRD indicator, and chronic condition flags |
| `Inpatient Claims` | Hospital stays, diagnosis codes, procedure codes, claim dates, and claim costs |
| `Outpatient Claims` | Outpatient visits, diagnosis codes, claim dates, and claim costs |
| `Prescription Drug Events` | Drug codes, quantity dispensed, days supply, patient payment, and total drug cost |

### Run the ETL Pipeline

```sh
python run_pipeline.py
```

This creates:
* `claims_warehouse.db` — the SQLite analytics warehouse
* `data/processed/` — processed CSV exports
* `data/processed/transform_metadata.json` — transformation metadata

### Run the Dashboard

After the ETL completes:

```sh
python -m streamlit run dashboard/app.py
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- WAREHOUSE DESIGN -->
## Warehouse Design

The pipeline builds a local star-schema style warehouse.

### Dimensions

| Table | Description |
|-------|-------------|
| `dim_patient` | Demographics, age group, race, state, ESRD indicator, and chronic condition flags |
| `dim_provider` | Provider identifier, type, and inferred provider state |
| `dim_date` | Calendar dimension for year, quarter, month, and day analysis |
| `dim_diagnosis` | ICD code and description |
| `dim_drug` | Drug code, derived drug name, and derived drug category |

### Facts

| Table | Description |
|-------|-------------|
| `fact_inpatient_claims` | One row per inpatient claim |
| `fact_outpatient_claims` | One row per outpatient claim |
| `fact_prescriptions` | One row per prescription event |

### Derived Metrics

The transform step produces the following derived fields:

* Age group
* Chronic condition count
* Claim duration in days
* Cost per day

> **Note:** `provider_state` is inferred from the most common patient state associated with each provider in the claims data. `drug_name` and `drug_category` are derived from the product service code because the CMS sample does not ship with a user-friendly drug catalog.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- DASHBOARD -->
## Dashboard

The Streamlit dashboard includes always-visible KPI cards and global filters for year, claim type, age group, and state.

### KPI Cards

* Total number of claims
* Total cost across all claims
* Average cost per claim
* Average length of hospital stay
* Total unique patients
* Total unique providers

### Tabs

| Tab | Contents |
|-----|----------|
| **Patient** | Age group distribution, gender breakdown, top states by claim volume, chronic condition prevalence, average cost by age group |
| **Claims** | Claim volume by month, inpatient vs outpatient split, average claim duration, cost distribution histogram, top 10 most expensive claims |
| **Diagnosis** | Top diagnoses by cost and count, average cost per diagnosis, searchable ICD lookup table |
| **Provider** | Top providers by volume and cost, provider state breakdown |
| **Prescription** | Top prescribed drugs, total drug cost over time, average days supply by drug category |

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- EXAMPLE SQL QUERIES -->
## Example SQL Queries

### Top inpatient diagnoses by total cost

```sql
SELECT
    dg.icd_code,
    dg.icd_description,
    ROUND(SUM(fi.total_cost), 2) AS total_inpatient_cost
FROM fact_inpatient_claims fi
JOIN dim_diagnosis dg ON fi.diagnosis_key = dg.diagnosis_key
GROUP BY dg.icd_code, dg.icd_description
ORDER BY total_inpatient_cost DESC
LIMIT 10;
```

### Outpatient claim counts by month

```sql
SELECT
    dd.year,
    dd.month,
    COUNT(*) AS outpatient_claim_count
FROM fact_outpatient_claims fo
JOIN dim_date dd ON fo.start_date_key = dd.date_key
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month;
```

### Top drugs by total cost

```sql
SELECT
    dr.drug_code,
    dr.drug_name,
    ROUND(SUM(fp.total_drug_cost), 2) AS total_drug_cost
FROM fact_prescriptions fp
JOIN dim_drug dr ON fp.drug_key = dr.drug_key
GROUP BY dr.drug_code, dr.drug_name
ORDER BY total_drug_cost DESC
LIMIT 10;
```

### Patients with the highest chronic condition burden

```sql
SELECT
    patient_id,
    age_group,
    gender,
    race,
    chronic_conditions_count,
    chronic_conditions_summary
FROM dim_patient
ORDER BY chronic_conditions_count DESC
LIMIT 10;
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ROADMAP -->
## Roadmap

- [x] Multi-source CMS extraction
- [x] Star-schema SQLite warehouse
- [x] Great Expectations validation
- [x] Streamlit dashboard with KPI cards and tabbed views
- [ ] dbt model layer for warehouse transformations
- [ ] Docker Compose for one-command setup
- [ ] Support for additional CMS year files (2009, 2010)
- [ ] Export dashboard views to PDF

See the [open issues](https://github.com/your_username/insurance-claims-etl-pipeline/issues) for a full list of proposed features and known issues.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also open an issue with the tag `enhancement`. Don't forget to give the project a star!

1. Fork the project
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a pull request

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Your Name — [@your_twitter](https://twitter.com/your_username) — email@example.com

Project Link: [https://github.com/your_username/insurance-claims-etl-pipeline](https://github.com/your_username/insurance-claims-etl-pipeline)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [CMS Synthetic Medicare Data](https://www.cms.gov/Research-Statistics-Data-and-Systems/Downloadable-Public-Use-Files/SynPUFs)
* [Great Expectations](https://greatexpectations.io)
* [Streamlit](https://streamlit.io)
* [Choose an Open Source License](https://choosealicense.com)
* [Img Shields](https://shields.io)
* [Best-README-Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/your_username/insurance-claims-etl-pipeline.svg?style=for-the-badge
[contributors-url]: https://github.com/your_username/insurance-claims-etl-pipeline/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/your_username/insurance-claims-etl-pipeline.svg?style=for-the-badge
[forks-url]: https://github.com/your_username/insurance-claims-etl-pipeline/network/members
[stars-shield]: https://img.shields.io/github/stars/your_username/insurance-claims-etl-pipeline.svg?style=for-the-badge
[stars-url]: https://github.com/your_username/insurance-claims-etl-pipeline/stargazers
[issues-shield]: https://img.shields.io/github/issues/your_username/insurance-claims-etl-pipeline.svg?style=for-the-badge
[issues-url]: https://github.com/your_username/insurance-claims-etl-pipeline/issues
[license-shield]: https://img.shields.io/github/license/your_username/insurance-claims-etl-pipeline.svg?style=for-the-badge
[license-url]: https://github.com/your_username/insurance-claims-etl-pipeline/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/your_username
[product-screenshot]: images/screenshot.png
[Python.org]: https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white
[Python-url]: https://python.org
[SQLite.org]: https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white
[SQLite-url]: https://sqlite.org
[Pandas.pydata.org]: https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white
[Pandas-url]: https://pandas.pydata.org
[Streamlit.io]: https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white
[Streamlit-url]: https://streamlit.io
[GreatExpectations.io]: https://img.shields.io/badge/Great%20Expectations-FF6310?style=for-the-badge&logoColor=white
[GreatExpectations-url]: https://greatexpectations.io