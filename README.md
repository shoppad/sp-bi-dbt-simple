# Mesa / ShopPad Analytics (dbt)

## Installation

0. Clone the repo.
1. `pip install virtualenv`
2. `cd` into repo folder.
3. `virtualenv venv` to create a new virtual env for this folder.
4. Activate the virtual env.
5. `pip install -r requirements.txt`
6. `dbt deps`
7. Create a [`profiles.yml` file](https://docs.getdbt.com/docs/get-started/connection-profiles) inside `~/.dbt/`

Try running the following commands:

- `dbt build` (runs the 2 below)
- `dbt run`
- `dbt test`
