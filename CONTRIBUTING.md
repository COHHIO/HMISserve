# Contributing

## How the runtime is pinned

The daily job runs in a Docker image built from `aws/Dockerfile`. Two things
fix what is inside it:

- The base image, `rocker/r-ver:4.4.3` (R 4.4.3 on Ubuntu 24.04).
- `renv.lock`, which records the exact version of every package the job needs
  and the commit SHA of the five GitHub packages (HMIS, HMISdata, HMISprep,
  clarity.looker, UU). CRAN packages come from the dated Posit Package Manager
  snapshots listed under `Repositories` in the lock.

Building the same commit on different days gives the same package versions.
Upstream fixes do not reach production on their own: someone has to bump the
lock. CI prints every installed package version and the five SHAs in the build
log, so you can check what an image contains.

## Bumping an upstream package

Do this in its own PR, so the change to `renv.lock` is easy to review.

1. Build the current image:
   `docker build -f aws/Dockerfile -t hmisserve-current .`
2. Start R in it with the repo mounted:
   `docker run --rm -it -v "$PWD":/out hmisserve-current R`
3. Install the new version.
   - GitHub package: `renv::install("COHHIO/HMISdata@<commit sha>")`
   - CRAN package: first point at a snapshot dated on or after the release,
     `options(repos = c(CRAN = "https://p3m.dev/cran/__linux__/noble/<YYYY-MM-DD>"))`,
     then `renv::install("<package>")`.
4. Re-snapshot into the mounted repo:
   `renv::snapshot(library = .libPaths(), packages = c("HMISserve", "logger", "aws.s3", "dplyr", "stringr"), lockfile = "/out/renv.lock", prompt = FALSE)`
5. Clean up `renv.lock`:
   - Remove the `HMISserve` and `renv` entries from `Packages`.
   - Under `R` > `Repositories`, list every snapshot URL the lock needs, oldest
     first. The snapshot only records the repositories set in the container, so
     the earlier ones have to be put back.
6. Rebuild and confirm the restore succeeds:
   `docker build -f aws/Dockerfile -t hmisserve-new .`

## Comparison report

Put this in the PR description.

1. List packages and the GitHub SHAs for both images and diff them:

   ```
   for img in hmisserve-current hmisserve-new; do
     docker run --rm $img Rscript -e '
       ip <- installed.packages()
       write.csv(ip[order(ip[, "Package"]), c("Package", "Version")])
       for (p in c("HMIS", "HMISdata", "HMISprep", "clarity.looker", "UU")) {
         cat("SHA", p, packageDescription(p)$RemoteSha, "\n")
       }' 2>/dev/null > $img.txt
   done
   diff hmisserve-current.txt hmisserve-new.txt
   ```

2. List each package whose version or SHA changed, and why.
3. Output comparison: once the snapshot and comparison tools from #17 exist,
   run the pipeline on both images (see [Running an image into
   `RME_test`](#running-an-image-into-rme_test)) and attach the result. Until
   then, say in the PR that outputs were not compared.
4. If the bump changes what RminorElevated shows, link the matching
   RminorElevated issue.

## Running an image into `RME_test`

Use this to try a PR's image against real inputs without touching production
output. The run writes to `s3://shiny-data-cohhio/RME_test/` (every output goes
through `DATA_ENV`), and `RME/` is not written. Runs are manual only. Nothing
runs on a schedule.

Each run overwrites what the last one left in `RME_test/`, and only one test
run can be active at a time.

### From GitHub Actions

Actions > "Run pipeline into RME_test" > Run workflow, then fill in:

- `image_tag`: the tag to run, such as `pr-42-<sha>`. A PR's Build and Push log
  prints it, and you can also read it in ECR. `latest` runs the current master
  image.
- `input_prefix` (optional): sets `HMISSERVE_INPUT_PREFIX`, for example
  `_snapshots/2026-11-11`, to read a frozen snapshot instead of live inputs.

The job starts the task and exits without waiting. Its summary shows the task
ID and the command to follow the logs. This needs the `ECS_SUBNETS` and
`ECS_SECURITY_GROUP` repository variables (the subnet and security group the
scheduled task uses).

### From the command line

This does the same thing by hand. It needs ECS permissions on the account.

```
REGION=us-east-2
CLUSTER=hmis-processing-cluster
IMAGE=647857229900.dkr.ecr.us-east-2.amazonaws.com/hmis-serve:pr-<n>-<sha>

# Copy the production task definition into a test family with the PR image.
# The scheduled task definition (hmis-serve) is not changed.
aws ecs describe-task-definition --task-definition hmis-serve --region $REGION \
  --query taskDefinition > td.json
jq --arg img "$IMAGE" '
  (.containerDefinitions[] | select(.name == "hmis-serve") | .image) = $img
  | .family = "hmis-serve-test"
  | del(.taskDefinitionArn, .revision, .status, .requiresAttributes,
        .compatibilities, .registeredAt, .registeredBy)' td.json > td-test.json
aws ecs register-task-definition --cli-input-json file://td-test.json --region $REGION

# Run it with DATA_ENV set. Add HMISSERVE_INPUT_PREFIX to the list if needed.
aws ecs run-task --region $REGION --cluster $CLUSTER --launch-type FARGATE \
  --task-definition hmis-serve-test \
  --network-configuration 'awsvpcConfiguration={subnets=[<subnet>],securityGroups=[<sg>],assignPublicIp=ENABLED}' \
  --overrides '{"containerOverrides":[{"name":"hmis-serve","environment":[
    {"name":"DATA_ENV","value":"RME_test"}]}]}'
```

### Logs

Test runs log to the same group as production, `/ecs/hmis-serve`. The stream is
`ecs/hmis-serve/<task-id>`, and `run-task` prints the task ARN, which ends in
the ID:

```
aws logs tail /ecs/hmis-serve --region us-east-2 \
  --log-stream-name-prefix ecs/hmis-serve/<task-id> --follow
```

### Viewing the result

Run RminorElevated with the `dev` golem config (`GOLEM_CONFIG_ACTIVE=dev`, set
before R starts, for example in `.Renviron`). That config sets `data_env:
RME_test`. RminorElevated takes its data folder from the golem config and does
not read the `DATA_ENV` environment variable.

To confirm you are on test data, look for the red "DEVELOPMENT MODE — USING
TEST DATA" banner. Do not use the "Data refreshed" time in the sidebar: it
comes from the upstream `hud.csv-daily/HMIS/` export and is the same whichever
folder the app reads.

The app loads every `.rds` and `.parquet` file under `RME_test/`. A test run
only writes what HMISserve produces, so anything the app expects from other
jobs will be missing unless it was copied into `RME_test/` first.

Known gap: RminorElevated reads some inputs from a hard-coded `RME` folder
(guidance in `mod_body_dq_program_level.R`, `mahoning_goals.csv` in
`mod_body_mpo.R`), so changes to those do not show up in the test environment
yet.

## After merging

A merge to `master` pushes the new image as `:latest`, and the next scheduled
run uses it. To roll back, retag the previous `master` build (its `<sha>` tag
in ECR) as `latest`.