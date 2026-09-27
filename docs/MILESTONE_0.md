# Milestone 0 — Environment and mental model (30 minutes)

## Outcome

You can start PostgreSQL locally and explain every component before writing pipeline code.

## Step 1: install/check Docker Desktop

On macOS, install Docker Desktop, open it, and wait until the engine is running. Then:

```bash
docker --version
docker compose version
git --version
```

## Step 2: create your GitHub repository

Create an empty public repository named `railops-data-platform`. Do not add a README or license
on GitHub because this local repo already contains them.

Later, from this directory:

```bash
git remote add origin https://github.com/YOUR_USERNAME/railops-data-platform.git
git branch -M main
git push -u origin main
```

## Step 3: configure and start only the source

```bash
cp .env.example .env
docker compose up -d source-db
docker compose ps
```

Expected: `source-db` is `healthy`. Nothing else should be running yet.

## Step 4: answer these questions in your own notes

1. Why is PostgreSQL the source and DuckDB the warehouse?
2. What is the grain of `train_events`?
3. Why should a failed load update the watermark only after commit?
4. Why is “the job ran successfully” not the same as “the data is correct”?

## Acceptance test

```bash
docker compose exec source-db pg_isready -U railops -d railops
```

Expected: `accepting connections`.

## Commit

```bash
git add .
git commit -m "chore: bootstrap local data platform"
```

When this passes, return to ChatGPT with the output of `docker compose ps`. We will do Milestone 1
together and inspect the source schema before implementing ingestion.

