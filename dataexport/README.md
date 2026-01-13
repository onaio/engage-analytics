# Engage Analytics Data Export

Export Engage Analytics data to S3 buckets. Supports both anonymized and PII exports to separate buckets.

## Prerequisites

- Python 3.10+
- [UV](https://docs.astral.sh/uv/) package manager
- AWS account with S3 access
- PostgreSQL database with Engage Analytics data

## Installation

```bash
cd dataexport
uv sync
```

## AWS Configuration

### Create IAM User

1. Log into [AWS Console](https://console.aws.amazon.com)
2. Go to **IAM** → **Users** → **Create user**
3. Name it (e.g., `engage-dataexport`)
4. Attach the `AmazonS3FullAccess` policy (or a custom policy scoped to your buckets)
5. Go to **Security credentials** → **Create access key**
6. Choose "Command Line Interface (CLI)"
7. Save the Access Key ID and Secret Access Key

### Create S3 Buckets

Using AWS CLI:
```bash
aws s3 mb s3://engage-analytics-exports-anon --region us-east-1
aws s3 mb s3://engage-analytics-exports-pii --region us-east-1
```

Or create them in the AWS Console under S3.

## Environment Configuration

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

Edit `.env`:

```
# Database Configuration
DBT_HOST=localhost
DBT_PORT=5432
DBT_USER=postgres
DBT_PASSWORD=your_password
DBT_DATABASE=airbyte

# AWS Configuration
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_REGION=us-east-1

# S3 Buckets
S3_BUCKET_ANON=engage-analytics-exports-anon
S3_BUCKET_PII=engage-analytics-exports-pii
```

## Usage

### Direct Export (export_to_s3.py)

Export both anonymized and PII data:
```bash
uv run python export_to_s3.py --type both
```

Export only anonymized data:
```bash
uv run python export_to_s3.py --type anon
```

Export only PII data:
```bash
uv run python export_to_s3.py --type pii
```

Delete local files after upload:
```bash
uv run python export_to_s3.py --type both --delete-local
```

### Full Pipeline (run_export.sh)

The wrapper script refreshes dbt models before exporting:

```bash
./run_export.sh          # Export both (default)
./run_export.sh anon     # Export anonymized only
./run_export.sh pii      # Export PII only
```

This runs:
1. `dbt run` to refresh all models
2. `export_to_s3.py` to export and upload

## S3 File Structure

Exports are organized by date:
```
s3://engage-analytics-exports-anon/
  └── 2024/01/12/
      └── engage_analytics_export_anon_20240112_143022.zip

s3://engage-analytics-exports-pii/
  └── 2024/01/12/
      └── engage_analytics_export_pii_20240112_143022.zip
```

## Data Exported

**Anonymized exports** include:
- `qr_*_anon` views (questionnaire responses with PII removed)
- `patient_anon` (de-identified patient data)
- Resource tables (encounters, practitioners, organizations, etc.)

**PII exports** include:
- `qr_*` views (full questionnaire responses)
- `patient` table (full patient data)
- Resource tables

## Troubleshooting

**"Missing environment variable: S3_BUCKET_ANON"**
- Check that `.env` file exists and has the bucket names set

**"Failed to connect to database"**
- Verify database credentials in `.env`
- Ensure PostgreSQL is running and accessible

**"Access Denied" on S3 upload**
- Check AWS credentials are correct
- Verify IAM user has S3 permissions
- Ensure bucket names match what's in `.env`

**dbt errors in run_export.sh**
- Ensure dbt is configured correctly in the `dbt/` directory
- Check that `profiles.yml` has valid database credentials
