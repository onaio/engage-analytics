from prefect import flow, task
import subprocess, os

DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", os.getcwd())

@task(retries=1, retry_delay_seconds=60)
def run_cmd(cmd: list[str]):
    print("Running:", " ".join(cmd))
    res = subprocess.run(cmd, cwd=DBT_PROJECT_DIR, check=True)
    return res.returncode

@flow(name="dbt build (opensrp_mh)")
def run_dbt():
    # Ensure deps are installed
    run_cmd(["dbt", "deps"])
    # Build all models; adjust selection as needed
    run_cmd(["dbt", "build", "--target", os.environ.get("DBT_TARGET", "prod")])

if __name__ == "__main__":
    run_dbt()
