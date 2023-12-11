# Status API design

## Use cases

To be able to check automatically if the data is ready when running reports.

To check the status of the API at a glance.

## What we need to consider

The API shouldn't know too many details about the engine's implementation. In other
words, the API shouldn't have to change if the engine changes.

When all steps are succeeding, everything should be fine.

We have dependencies between the data generated at each step. If a step runs
(regardless of its success or failure), it should invalidate the data of
other steps.

Some steps want to verify their status after completing.

Steps can fail.

We cannot purely rely on global testing and verification as it's not always possible to check
data against a 2nd source or 3rd party.

A whole engine might fail part way through - we should make sure we can clean up
where possible and consider what happens if we don't.

### What other things could we consider

These are cases that we might want to consider in the future.

Each job only affects a subset of the API. Does a failure in once place need to invalidate everything?

## Engine design

### Engine

An engine is made up of steps and then a final verification.

### Engine invocation

A run of an engine has a unique global invocation ID.

The engine has a profile name.

If an engine invocation fails (SIGTERM or an unhandled exception),
it should mark any currently running steps as failed.

An engine has a global validation step which runs checks which can't other be
ran as part of a step.

### Engine step

An engine is made up of steps:

- A step is named with an ID.
- A step inherits the engine's invocation ID.
- A step has its own invocation ID.
- A step runs in isolation to the others.
- A step can have a valid lifetime, of how long the data is valid for after
  a run.
- A step can invalidate the data from other steps and invalidates other steps
  once started.
- A step can validate its success and report on statistics relevant to it.
- A step reports any errors that occurred during its lifetime.
- A step has a timeout.
- We only consider the latest run of a step.

When a step starts running, we should update the database to register a run.

### Failure handling

If the engine fails and is terminated by Google Cloud run or fails because
of a major exception, each

An engine has global

## Data design

### Data - Engine invocation

- Invocation ID: uuid
- Profile name: string
- Start time: datetime
- End time: datetime
- Global validation status: json
- Global validation info: json

### Data - Step invocation

- Invocation ID: uuid
- ID: string
- Engine invocation ID: uuid
- Start time: datetime
- End time: datetime
- Step dependencies: list of strings
- Info: json
- Tests: json array

## API design

The API will:

1. Query the steps table to get the latest invocation of each test
2. Check if any steps failed
3. Build a dependency/invalidation model between steps
4. Check which steps are invalid based on the end and start times
5. Check the global validation
6. Return a response according to whether the data is ready

## Ready endpoint

### Success

If the database is available and the data is ready, the API responds with a 200.

With the request:

```http
GET /v0/status HTTP/1.1
```

We would get the response:

```http
HTTP/1.1 200 OK

{
    "available": true, // whether we can connect to the database and other services we require
    "ready": true, // whether the data is ready to be used
    "engine_steps": [
        {
            "name": "<job name 1>",
            ... // job info
        },
        ...,
        {
            "name": "<job name n>"
            ...
        }
    ],
    "global_verification_succeeded": true,
    "global_verification_last_checked": "<time of last check>",
    "global_verification_info": {
        ... // generic information that the verifications wants to share
    }
}
```

### Failure due to verification or job failure

With the request:

```http
GET /v0/status HTTP/1.1
```

We would get the response:

```http
HTTP/1.1 200 OK

{
    "available": true, // whether we can connect to the database and other services we require
    "ready": false, // whether the data is ready to be used
    "engine_steps": [
        {
            "name": "<job name 1>",
            "invalidated_by": ["<steps that have invalidated this>"],
            ... // job info
        },
        {
            "name": "<job name 2>",
            "status": "running",
            ... // job info
        },
        {
            "name": "<job name 3>",
            "status": "failed",
            ... // job info
        },
        ...,
        {
            "name": "<job name n>"
            ...
        }
    ],
    "global_verification_succeeded": false,
    "global_verification_last_checked": "<time of last check>",
    "global_verification_info": {
        ... // generic information that the verifications wants to share
    }
}
```

### Failure due to database

If we can't connect to the database, the API should respond with a server error and limited
information.

With the request:

```http
GET /v0/status HTTP/1.1
```

We would get the response:

```http
HTTP/1.1 503 Service Unavailable

{
    "available": false,
    "reason": "The database is unavailable."
}
```
