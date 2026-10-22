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
3. Output comparison: once the snapshot and comparison tools from #17 and the
   `RME_test` run from #18 exist, run the pipeline on both images and attach
   the result. Until then, say in the PR that outputs were not compared.
4. If the bump changes what RminorElevated shows, link the matching
   RminorElevated issue.

## After merging

A merge to `master` pushes the new image as `:latest`, and the next scheduled
run uses it. To roll back, retag the previous `master` build (its `<sha>` tag
in ECR) as `latest`.