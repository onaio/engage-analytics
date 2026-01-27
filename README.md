# Analytics for the Engage Project

This includes:
- dbt models for transforming healthcare data
- Metrics and indicators for program monitoring
- Claude Code skill for managing indicators

## Quick Start

### Prerequisites
- PostgreSQL database with Airbyte-synced FHIR data
- Python 3.10+ with uv package manager
- dbt-core and dbt-postgres

### Running dbt
```bash
cd dbt
source .envrc  # or set DBT_HOST, DBT_PORT, DBT_USER, DBT_PASSWORD, DBT_DATABASE

# Run all models
uv run dbt run --profiles-dir .

# Run metrics only
uv run dbt run --profiles-dir . --select +fct_metrics_long
```

## Claude Code Skill

This repo includes a Claude Code skill for managing Engage indicators and metrics.

### Installation

Add this marketplace to Claude Code:

```bash
claude mcp add-marketplace onaio/engage-analytics
```

Then enable the plugin:

```bash
claude mcp enable engage-indicators@engage-analytics
```

Or install directly from GitHub:

```bash
claude mcp add-marketplace https://github.com/onaio/engage-analytics
```

### What the Skill Helps With

1. **Understand existing indicators** - Query metrics, see indicator mappings
2. **Create new indicators** - Guided workflow asking the right questions
3. **Add new questionnaires** - Update metadata, generate models
4. **Update anonymization** - Manage PII field settings
5. **Test and validate** - Verify metric logic

### Usage

Once installed, the skill triggers automatically when you ask about:
- Indicator creation or metrics
- Questionnaire data models
- PHQ-9/GAD-7 scores
- mwTool eligibility
- dbt model development for Engage

Example prompts:
- "Show me the current indicators"
- "Help me create a new indicator for client retention"
- "Add the new intake form questionnaire"
- "What metrics are available for IPC?"

## Documentation

- [Metrics Documentation](dbt/docs/metrics.md) - Full indicator and metric reference
- [Data Analyst Guide](guide.md) - System architecture and workflow guide
- [dbt Project Guide](dbt/CLAUDE.md) - dbt project structure and commands
