# Ministry of Justice Digital Prison Reporting Glue Jobs

[![repo standards badge](https://img.shields.io/endpoint?labelColor=231f20&color=005ea5&style=for-the-badge&label=MoJ%20Compliant&url=https%3A%2F%2Foperations-engineering-reports.cloud-platform.service.justice.gov.uk%2Fapi%2Fv1%2Fcompliant_public_repositories%2Fendpoint%2Fdigital-prison-reporting-jobs&logo=data:image/png;base64,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)](https://operations-engineering-reports.cloud-platform.service.justice.gov.uk/public-github-repositories.html#digital-prison-reporting-jobs)

#### CODEOWNER

- Team : [hmpps-digital-prison-reporting](https://github.com/orgs/ministryofjustice/teams/hmpps-digital-prison-reporting)
- Email : digitalprisonreporting@digital.justice.gov.uk

## Overview

Provides code defining a number of jobs to be launched by AWS Glue supporting a
number of processes within the Digital Prison Reporting platform.

## Local Development

This project uses gradle which is bundled with the repository and also makes use
of

- [micronaut](https://micronaut.io/) - for compile time dependency injection
- [lombok](https://projectlombok.org/) - to reduce boilerplate when creating data classes
- [jacoco](https://docs.gradle.org/current/userguide/jacoco_plugin.html) - for test coverage reports

### Packaging

This project makes use of the [shadow jar plugin](https://github.com/johnrengelman/shadow)
which takes care of creating a jar containing all dependencies.

The plugin adds the suffix `-all` to the jar file name e.g.

```
    digital-prison-reporting-jobs-1.0-SNAPSHOT-all.jar
```

### Running a job

First, build the jar locally

```
    ./gradlew clean shadowJar
```

and then execute your job by specifying the fully qualified classname e.g.

```
    java -cp ./build/libs/digital-prison-reporting-jobs-1.0-SNAPSHOT-all.jar uk.gov.justice.digital.Placeholder
```

ensuring that your job class has a main method that can be executed.

> **Note** - On AWS Glue the job class can be specified using the `--class`
> parameter and the script can then be left blank.


## Run app main methods locally

Set the following CLI arguments, e.g. in your Intellij run configuration.
```
--checkpoint.location ./.checkpoints/
--dpr.curated.s3.path s3a://dpr-dms-curated-zone-development/
--dpr.structured.s3.path s3a://dpr-dms-structured-zone-development/
--dpr.violations.s3.path s3a://dpr-dms-violation-development/
--dpr.aws.dynamodb.endpointUrl https://dynamodb.eu-west-2.amazonaws.com
--dpr.aws.kinesis.endpointUrl https://kinesis.eu-west-2.amazonaws.com
--dpr.aws.region eu-west-2
--dpr.datamart.db.name datamart
--dpr.domain.catalog.db domain
--dpr.domain.registry dpr-domain-registry-development
--dpr.domain.target.path s3a://dpr-dms-domain-development
--dpr.kinesis.reader.batchDurationSeconds 30
--dpr.kinesis.reader.streamName dpr-kinesis-ingestor-development
--dpr.log.level INFO
--dpr.raw.s3.path s3a://dpr-dms-raw-zone-development/
--dpr.redshift.secrets.name dpr-redshift-secret-development
--dpr.contract.registryName dpr-glue-registry-avro-development
```

## Testing

> **Note** - test coverage reports are enabled by default and after running the
> tests the report will be written to build/reports/jacoco/test/html

### Unit Tests

The unit tests use JUnit5 and Mockito where appropriate. Use the following to
run the tests.

```
    ./gradlew clean test
    ./gradlew clean check
```

### Integration Tests

```
    ./gradlew clean integrationTest
    ./gradlew clean check
```

### Acceptance Tests

```
    TBD
```


## Contributing

Please adhere to the following guidelines when making contributions to the
project.

### Documentation

- Keep all code commentary and documentation up to date

### Branch Naming

- Use a JIRA ticket number where available
- Otherwise a short descriptive name is acceptable

### Commit Messages

- Prefix any commit messages with the JIRA ticket number where available
- Otherwise use the prefix `NOJIRA`

### Pull Requests

- Reference or link any relevant JIRA tickets in the pull request notes
- At least one approval is required before a PR can be merged

### Releases

- v1.0.0

## DataStorageService Backoff Retry
- `dpr.datastorage.retry.maxAttempts` - The maximum number of attempts to make, i.e. 1 means no retries, 10 means make the 1st attempt + 9 retries.
- `dpr.datastorage.retry.minWaitMillis` - The minimum time to wait after an attempt before retrying in milliseconds, prior to jitter being applied.
- `dpr.datastorage.retry.maxWaitMillis` - The maximum time to wait after an attempt before retrying in milliseconds, prior to jitter being applied.
- `dpr.datastorage.retry.jitterFactor` - If greater than zero it applies some random jitter to the calculated wait time, e.g. 0.25 jitter factor will add or subtract up to 250ms from a 1 second wait time.

Note that jitter can result in the wait for one iteration exceeding maxWaitMillis. The actual maximum wait is `maxWaitMillis + (maxWaitMillis * jitterFactor)`.

Here are some examples settings and log outputs showing time taken (where the retried example operation takes zero time).

- maxAttempts: 10
- minWaitMillis: 100
- maxWaitMillis: 500
- jitterFactor: 0.25

```
Retrying after attempt 1. Elapsed time total: 200ms.
Retrying after attempt 2. Elapsed time total: 445ms.
Retrying after attempt 3. Elapsed time total: 874ms.
Retrying after attempt 4. Elapsed time total: 1,374ms.
Retrying after attempt 5. Elapsed time total: 1,925ms.
Retrying after attempt 6. Elapsed time total: 2,374ms.
Retrying after attempt 7. Elapsed time total: 3,045ms.
Retrying after attempt 8. Elapsed time total: 3,575ms.
Retrying after attempt 9. Elapsed time total: 4,205ms.
Retries exceeded on attempt 10. Elapsed time total: 4,207ms.
```

- maxAttempts: 10
- minWaitMillis: 100
- maxWaitMillis: 5000
- jitterFactor: 0.25

```
Retrying after attempt 1. Elapsed time total: 200ms.
Retrying after attempt 2. Elapsed time total: 454ms.
Retrying after attempt 3. Elapsed time total: 835ms.
Retrying after attempt 4. Elapsed time total: 1,879ms.
Retrying after attempt 5. Elapsed time total: 3,932ms.
Retrying after attempt 6. Elapsed time total: 7,753ms.
Retrying after attempt 7. Elapsed time total: 13,629ms.
Retrying after attempt 8. Elapsed time total: 19,280ms.
Retrying after attempt 9. Elapsed time total: 23,788ms.
Retries exceeded on attempt 10. Elapsed time total: 23,790ms.
```

- maxAttempts: 10
- minWaitMillis: 100
- maxWaitMillis: 10000
- jitterFactor: 0.25

```
Retrying after attempt 1. Elapsed time total: 204ms.
Retrying after attempt 2. Elapsed time total: 442ms.
Retrying after attempt 3. Elapsed time total: 1,013ms.
Retrying after attempt 4. Elapsed time total: 2,060ms.
Retrying after attempt 5. Elapsed time total: 3,948ms.
Retrying after attempt 6. Elapsed time total: 6,641ms.
Retrying after attempt 7. Elapsed time total: 13,488ms.
Retrying after attempt 8. Elapsed time total: 23,605ms.
Retrying after attempt 9. Elapsed time total: 31,772ms.
Retries exceeded on attempt 10. Elapsed time total: 31,774ms.
```

- maxAttempts: 10
- minWaitMillis: 500
- maxWaitMillis: 5000
- jitterFactor: 0.25

```
Retrying after attempt 1. Elapsed time total: 705ms.
Retrying after attempt 2. Elapsed time total: 1,829ms.
Retrying after attempt 3. Elapsed time total: 4,383ms.
Retrying after attempt 4. Elapsed time total: 9,286ms.
Retrying after attempt 5. Elapsed time total: 14,087ms.
Retrying after attempt 6. Elapsed time total: 18,729ms.
Retrying after attempt 7. Elapsed time total: 24,264ms.
Retrying after attempt 8. Elapsed time total: 29,515ms.
Retrying after attempt 9. Elapsed time total: 34,571ms.
Retries exceeded on attempt 10. Elapsed time total: 34,573ms.
```

- maxAttempts: 10
- minWaitMillis: 500
- maxWaitMillis: 10000
- jitterFactor: 0.25

```
Retrying after attempt 1. Elapsed time total: 469ms.
Retrying after attempt 2. Elapsed time total: 1,405ms.
Retrying after attempt 3. Elapsed time total: 3,665ms.
Retrying after attempt 4. Elapsed time total: 8,505ms.
Retrying after attempt 5. Elapsed time total: 18,406ms.
Retrying after attempt 6. Elapsed time total: 28,948ms.
Retrying after attempt 7. Elapsed time total: 41,448ms.
Retrying after attempt 8. Elapsed time total: 50,899ms.
Retrying after attempt 9. Elapsed time total: 60,200ms.
Retries exceeded on attempt 10. Elapsed time total: 60,202ms.
```

- maxAttempts: 10
- minWaitMillis: 1000
- maxWaitMillis: 20000
- jitterFactor: 0.25
```
Retrying after attempt 1. Elapsed time total: 1,121ms.
Retrying after attempt 2. Elapsed time total: 3,296ms.
Retrying after attempt 3. Elapsed time total: 6,489ms.
Retrying after attempt 4. Elapsed time total: 14,194ms.
Retrying after attempt 5. Elapsed time total: 32,057ms.
Retrying after attempt 6. Elapsed time total: 57,081ms.
Retrying after attempt 7. Elapsed time total: 75,936ms.
Retrying after attempt 8. Elapsed time total: 92,927ms.
Retrying after attempt 9. Elapsed time total: 111,546ms.
Retries exceeded on attempt 10. Elapsed time total: 111,548ms.
```

## TODO

- Modify the Dependabot file to suit the [dependency manager](https://docs.github.com/en/code-security/dependabot/dependabot-version-updates/configuration-options-for-the-dependabot.yml-file#package-ecosystem) you plan to use and for [automated pull requests for package updates](https://docs.github.com/en/code-security/supply-chain-security/keeping-your-dependencies-updated-automatically/enabling-and-disabling-dependabot-version-updates#enabling-dependabot-version-updates). Dependabot is enabled in the settings by default.
- Ensure as many of the [GitHub Standards](https://github.com/ministryofjustice/github-repository-standards) rules are maintained as possibly can.
