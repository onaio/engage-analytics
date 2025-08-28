# Engage — dbt

This starter is opinionated for Airbyte-extracted FHIR resources in Postgres with JSONB payloads in `resource`.
It builds staging models, an intermediate long-format table for QuestionnaireResponse answers, and a few example marts/indicators.

Edit `models/sources.yml` to match your database and Airbyte schema names.
