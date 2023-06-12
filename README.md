# [Mesalytics (dbt)](#Mesalyticstop)

A DBT application that builds the ShopPad Data Warehouse created by [@JonCrawford](https://github.com/joncrawford).

[Report Bug](https://github.com/shoppad/sp-bi-dbt/issues)
·
[Request Feature](https://github.com/shoppad/sp-bi-dbt/issues)

## Table of Contents

- [About The Project](#about-the-project)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
- [Oddities](#oddities)
- [Compromises](#concessions)

<!-- ABOUT THE PROJECT -->

## About The Project

The Mesalytics DBT project is designed to take the pieces of data produced by the production application & 3rd-party data sources such as Segment to turn that raw data into usable data objects that help business stakeholders derive insights, diagnose issues and make decisions. The DBT application is automatically run on a schedule (e.g. "every 3 hours") through [getdbt.com](https://getdbt.com). The application is currently tailored to Snowflake but may (relatively) easily be changed to Postgres or Redshift syntax should the underlying source database change.

[back to top](#Mesalyticstop)

<!-- GETTING STARTED -->

## Getting Started

To contribute to the DBT project, you can choose between the browser-based editor at GetDBT.com or using a local development environment. The browser-based version is recommended for developers making infrequent, inconsequential changes whereas the local development environment is strongly encouraged for serious contributors.

To get a local copy up and running follow these steps.

### Prerequisites

This is an example of how to list things you need to use the software and how to install them.

- python 10.X
- Whitelisted access from your IP address to Snowflake database.

### Installation

1. Confirm that you can connect directly to the data warehouse from the console or a GUI such as TablePlus.
2. Clone this repo

   ```sh
   git clone https://github.com/shoppad/sp-bi-dbt
   ```

3. Install [Virtualenv](https://virtualenv.pypa.io/en/latest/)

   ```sh
   brew install virtualenv
   ```

4. `cd` into the repo

5. `virtualenv venv` to create a new virtual env for this folder.

6. Activate the virtual env. It is recommended that you use an auto-activating virtualenv activator plugin such as [this one for Zsh](https://github.com/MichaelAquilina/zsh-autoswitch-virtualenv).

7. Install project python dependencies.

   ```sh
   pip install -r requirements.txt
   ```

8. Install project DBT lib dependencies

   ```sh
   dbt deps
   ```

9. Set up your personal development profile in ~/.dbt/profiles.yml. [Instructions here](https://docs.getdbt.com/docs/get-started/connection-profiles)

   ```yml
   shoppad:
     outputs:
       dev:
         account: <fill>
         database: mongo
         password: <fill
         role: PIPELINE
         schema: dbt_<pick-a-personal-schema>
         threads: 4
         type: snowflake
         user: <fill></fill>
         warehouse: dbt_warehouse
         query_tag: dbt-dev-<pick-a-personal-schema>
       prod:
         account: <fill>
         database: mongo
         password: <fill>
         role: PIPELINE
         schema: analytics
         threads: 4
         type: snowflake
         user: <fill>
         warehouse: dbt_warehouse
         query_tag: dbt-prod-<pick-a-personal-username>
     target: dev
   ```

10. Test your connection with `dbt seed`. This will create a schema called `dbt_[yourname]_seeds`.

```sh
dbt seed
```

11. Browse the newly-created scheam (dbt\_[yourname]\_sseds) from the console or TablePlus to ensure it was successfully created.

12. Build the full application into your schema. This will build that data warehouse and also run the test suite to ensure the resulting data is valid.

    ```sh
    dbt build
    ```

[back to top](#Mesalyticstop)

<!-- USAGE EXAMPLES -->

## Usage

Try running the following commands:

- `dbt deps`
- `dbt seed`
- `dbt run`
- `dbt test`
- `dbt build`

### To target just the model you're working on

```sh
dbt run -m my_model
```

### To also build everything downstream that depends on the model you're working on

```sh
dbt run -m my_model+
#Add the + at the end.
```

[back to top](#Mesalyticstop)

## Structure

### Models

The application's models are structured into 3 types & folders.

- staging: The initial import of raw source data with filtering and slight formatting.
- intermdiate: intermediary formatting and supporting models that are not business user-facing.
- marts: The finalized production-ready business insight models.

## Oddities & Gotchas

These are list of counter-intuitive production data issues that may surprise developers working on the project.

[List any that arise.]

## Concessions

Some data concessions and compromises have been made during development to bridge gaps or errors in the production data. This is a running list of those compromises intended to be pruned and removed as the production application improves data holes. Code pertaining to a Concession is tagged with `#concession`.

- [ ] Workflows and Workflow Steps have been previously deleted and can not be reliabliy used as sources or counts for other models.

# Best Practices

- [The DBT Style Guide](https://www.markdownguide.org/basic-syntax/#reference-style-links) – The application has been developed working as closely as possible to the DBT organization's best practices and recommended code styles.

- There are several VS Code extensions that are recommended in the workspace. It is recommended to always open the application via the workspace for local development with `code mesa_dbt.workspace`.

[back to top](#Mesalyticstop)
