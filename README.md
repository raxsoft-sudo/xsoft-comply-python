# xsoft_comply Python SDK

[한국어](README.ko.md) · [PyPI](https://pypi.org/project/xsoft-comply/)

The official Python SDK for [xsoft comply](https://xsoftcomply.com).

The SDK collects operating records of AI execution, tool calls, decisions and human review. It does not determine whether a legal obligation applies or has been met.

- Python 3.9 or later is supported.
- The default installation has no runtime dependencies beyond the Python standard library.
- Distribution name: `xsoft-comply`. Import name: `xsoft_comply`.

> **Status.** xsoft comply is an early-stage product. The service interface is currently available in Korean only. xsoft comply does not provide legal advice, certification or a guarantee of compliance with the Framework Act on the Development of Artificial Intelligence and the Creation of a Foundation for Trust, and does not act as a domestic representative under Article 36 of that Act.

## Contents

- [Installation](#installation)
- [Quick start](#quick-start)
- [Evidence records by provision](#evidence-records-by-provision)
- [Local checks before transmission](#local-checks-before-transmission)
- [Transmission and local spooling](#transmission-and-local-spooling)
- [Upgrading from 1.0.1 to 1.0.2](#upgrading-from-101-to-102)
- [Manual resend of local records](#manual-resend-of-local-records)
- [Content and personal data](#content-and-personal-data)
- [Record linking and retention](#record-linking-and-retention)
- [Input fields by provision](#input-fields-by-provision)
- [About this repository](#about-this-repository)
- [Licence and contact](#licence-and-contact)

## Installation

A virtual environment is recommended. On Linux and macOS:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install xsoft-comply==1.0.2
.venv/bin/python -c "import xsoft_comply; print(xsoft_comply.__version__)"
```

On Windows PowerShell, install with `.venv\Scripts\python.exe -m pip install xsoft-comply==1.0.2`. After installation, confirm that the printed version is `1.0.2`. After a build, the SHA-256 checksums for the distribution files can be found in `RELEASES.md`. If you use hash-pinned installation, replace the existing `xsoft-comply==1.0.1` entry with `1.0.2` and the actual checksum of the distribution file.

## Quick start

Do not write the API key in source code. Supply it through an environment variable or a similar mechanism. Set `endpoint` to the Event API base address provided by the service. The address in this example must be replaced with the actual address.

```python
import os
import xsoft_comply as xc

xc.init(
    api_key=os.environ["XSOFT_COMPLY_API_KEY"],
    endpoint="https://example.com/api/v1",
)

with xc.session(agent_id="example-agent-01") as s:
    s.llm_call(model="example-model", provider="example-provider",
               tokens_in=12, tokens_out=8, latency_ms=240)
    s.tool_call(tool="example.lookup", status="ok", latency_ms=14)
    s.action(action="answer.sent", status="ok")

xc.flush(timeout=5.0)
```

`xc.session()` records the start and end of a session. LLM calls, tool calls, data access, task processing, decisions and human review are recorded only when the corresponding method is called at that point or when a supported framework integration is used. The AI system to which records are attributed is determined by the API key, not by the session's `product_id`. Use a separate API key for each AI system.

| Event transmitted | How it is recorded |
|---|---|
| `session_start`, `session_end` | Start and end of `xc.session()` |
| `llm_call` | `s.llm_call()` |
| `tool_call` | `s.tool_call()` |
| `data_access` | `s.data_access()` |
| `action` | `s.action()`, `s.decision()`, `s.human_review()` |

`decision()` and `human_review()` do not add separate event types. They record an `action` event together with the related evidence record. Integration functions for LangChain and the OpenAI Agents SDK are also provided, but those frameworks must be installed separately.

## Evidence records by provision

Use `xc.evidence_block(article, **fields)` or `s.evidence(article, ...)` to record the predefined fields for Articles 31, 32 and 34. There are no SDK input fields for Articles 33, 35 and 36. An empty field or the absence of a record does not, by itself, indicate any legal status.

```python
with xc.session(agent_id="example-agent-01") as s:
    s.action("review.requested", status="ok", evidence=[
        xc.evidence_block(34, supervisor_id="supervisor-01",
                          decision_basis="internal criteria table v3"),
    ])
    s.evidence(32, source="declared", evidence_key="model-01",
               model_id="model-01", model_version="1.2")
```

`evidence_key` identifies a declaration so that it is not managed more than once. Field names must match the permitted list below exactly. Field names that are not defined are not stored. Do not enter raw content, personal data or legal conclusions as field values.

| Type | Accepted input | On a format error |
|---|---|---|
| `text` | String | Converted to a string; long values may be truncated |
| `numeric` | Number or numeric string | The field is omitted |
| `bigint` | Integer or integer string | The field is omitted |
| `boolean` | `True` / `False` or `"true"` / `"false"` | The field is omitted |
| `date` | Parseable date string; `YYYY-MM-DD` recommended | The field is omitted |

A field with an invalid format is omitted without stopping the entire batch. Do not infer from an acknowledgement response that every field was stored; check locally before transmission.

## Local checks before transmission

```python
xc.init(api_key=os.environ["XSOFT_COMPLY_API_KEY"],
        endpoint="https://example.com/api/v1", dry_run=True)
block = xc.evidence_block(34, supervisor_id="supervisor-01")
print(xc.validate(block))
print(xc.preview())
```

With `dry_run=True`, neither new records nor records already held in the local spool are transmitted. `validate()` is a local checking tool that shows whether input fields are populated and identifies unknown field names. It does not determine whether a legal obligation applies, has been met or has been breached. Use `preview()` to review the values that would be transmitted. To switch to live transmission, re-initialise with `dry_run=False` or restart the program with the normal configuration.

## Transmission and local spooling

The thread that records an event does not perform network transmission. A background worker transmits records and, on a temporary transmission failure, writes them to a local spool and retries transmission. A local record is removed only after the server has confirmed receipt. If a response omits confirmation for some records, the unconfirmed records are retained and retried. The SDK also handles cases in which multiple processes share one spool directory.

If responses are delayed and the queue becomes more than half full, some queued records are moved to the local spool. When the queue is full, new records are rejected and records already queued are retained. When the spool exceeds its capacity, records already stored are not deleted and new records are rejected. Records may be lost because of a disk error, a final rejection by the server or exhausted storage capacity. Review the counters below together with the service's operations screen.

```python
xc.dropped_events()        # records rejected because the queue was full
xc.failed_events()         # records finally rejected by the server
xc.spool_rejected_events() # records rejected at the local spool stage
xc.flush(timeout=5.0)
xc.shutdown()
```

After `shutdown()`, the SDK can be started again with `init()`. In serverless functions, call `flush()` immediately before returning or use `@xc.flush_on_return`. The endpoint must use `https://`. `http://` is permitted only for testing against `localhost` on the same machine. The handling of an `init()` configuration argument with an invalid format has been improved so that only that argument is replaced by its default value and collection continues. Confirm through a local test that the configuration has been applied as expected.

## Upgrading from 1.0.1 to 1.0.2

1. Stop all processes that use 1.0.1.
2. In the same Python environment, run `python -m pip install -U xsoft-comply==1.0.2`. For hash-pinned installation, update both the version and the actual checksum of the distribution file.
3. Restart the application and confirm the version and receipt status.
4. Check the start-up warning about untransmitted records in the 1.0.1 format and the retained count returned by `xc.legacy_unowned_events()`.
5. If the retained count is not zero, identify the original API key and AI system used with 1.0.1 and contact `contact@xsoftcomply.com` regarding recovery.

Because it is not possible to determine which API key the untransmitted 1.0.1 records belong to, version 1.0.2 retains them in `legacy_unowned/` and **does not send them, either automatically or with the manual resend tool**. Do not send these files with a different key. Spool files created by 1.0.2 cannot be read by 1.0.1, so do not downgrade to 1.0.1.

## Manual resend of local records

First set the current key in an environment variable. `--endpoint` is required and must be replaced with the actual Event API address.

```bash
python -m xsoft_comply.resend --endpoint https://example.com/api/v1 --dry-run
python -m xsoft_comply.resend --endpoint https://example.com/api/v1
```

`--include-quarantine` also re-examines records held in quarantine. `--all-owners` only inspects records belonging to other keys and does not send them with the current key. Handle records belonging to another key separately with that key. `--dry-run` neither modifies files nor transmits records, and returns exit code 0 for reporting purposes.

| Exit code | Meaning |
|---|---|
| `0` | No records remain to be resent and no records in the 1.0.1 format are retained. With `--dry-run`, it indicates that the inspection completed |
| `1` | Records remain to be retried, for example after a transmission failure. Check the API key, address and server status |
| `3` | No records remain to be retried, but records in the 1.0.1 format are retained. Identify the original key and contact support |

Review the printed `remaining` and `legacy_unowned` counts in addition to the exit code. If both records to be retried and retained 1.0.1 records exist, exit code 1 takes precedence. Check command input errors, such as a missing option or an empty API key, separately.

## Content and personal data

Raw LLM prompt and response content is not transmitted by default. For `content`, only derived information such as length and a short hash is recorded. In `metadata`, predefined sensitive keys are removed, and values in the format of a Korean resident registration number, telephone number, email address or valid card number are masked automatically. For example, `4111-1111-1111-1111` is masked as `4111-****-****-1111`. Not every format of personal data can be detected, so do not place personal data or raw content in `metadata`.

Automatic masking is not applied to the provision-level `evidence` fields. Values entered in manager or supervisor and reviewer fields are stored unmasked. They may be partially masked on the service screens, and exported data may contain the values as entered. The business operator should review what is entered in those fields.

## Record linking and retention

The server links each operating record to the hash of the preceding record so that changes can be detected. The SDK connection status and whether provision-level records are populated are separate pieces of information. Hash linking does not represent a legal determination or verification by an external body.

`retention_days` and a record's `_retain_until` value do not change the actual retention or deletion time on the server. Retention and deletion of operating evidence records are governed by the terms of service.

## Input fields by provision

The business operator should check input values and formats before transmission. An empty field does not indicate any legal status. The descriptions below are working translations of the Korean field descriptions; the Korean text in [README.ko.md](README.ko.md) prevails.

### Article 31

| Field | Type | Meaning | Example / format |
|---|---|---|---|
| prior_notice_provided | boolean | Whether prior notice was provided | `True` |
| disclosure_method | text | Method of notice | `"example-01"` |
| disclosure_version | text | Version of the notice wording applied | `"example-01"` |
| ai_generated_label | text | Whether generated output was labelled | `"example-01"` |
| label_method | text | Method of labelling | `"example-01"` |
| machine_readable_label | text | Whether guidance was provided when machine-readable labelling was used | `"example-01"` |
| synthetic_media | text | Whether the output is difficult to distinguish from reality | `"example-01"` |
| label_visibility | text | Whether the labelling method is clearly recognisable to users | `"example-01"` |
| accessibility_method | text | Method by which the label can be readily checked | `"example-01"` |
| audience_consideration | text | Consideration of the circumstances of the primary users | `"example-01"` |
| artistic_relaxation | boolean | Whether the relaxation for artistic or creative works was applied | `True` |
| exemption_type | text | Type of exception applied | `"example-01"` |
| exemption_basis | text | Basis for treating the case as an exception | `"example-01"` |
| output_id | text | Identifier of the output concerned | `"example-01"` |
| media_type | text | Media type of the output | `"example-01"` |

### Article 32

| Field | Type | Meaning | Example / format |
|---|---|---|---|
| training_compute | numeric | Cumulative compute used for training | `0.72` |
| frontier_technology_status | text | Whether cutting-edge technology is applied | `"example-01"` |
| frontier_technology_basis | text | Basis for that assessment | `"example-01"` |
| broad_serious_impact_status | text | Whether widespread and serious impact is a concern | `"example-01"` |
| broad_serious_impact_basis | text | Basis for that assessment | `"example-01"` |
| model_id | text | Identifier of the model concerned | `"example-01"` |
| model_version | text | Model version | `"example-01"` |
| risk_management_policy_ref | text | Reference to the risk management system document | `"example-01"` |
| submission_status | text | Whether implementation results were submitted | `"example-01"` |
| submission_date | date | Date of submission | `"2026-10-02"` |
| risk_id | text | Identifier of the risk item | `"example-01"` |
| risk_signal_source | text | Channel through which the risk was identified | `"example-01"` |
| risk_lifecycle_stage | text | Stage of the life cycle at which the risk arose | `"example-01"` |
| risk_type | text | Risk category | `"example-01"` |
| risk_level | text | Result of the risk assessment | `"example-01"` |
| risk_action | text | Risk mitigation measure | `"example-01"` |
| risk_status | text | Status of risk handling | `"example-01"` |
| incident_id | text | Identifier of the safety incident | `"example-01"` |
| incident_detection_source | text | Channel through which the incident was detected | `"example-01"` |
| incident_type | text | Incident category | `"example-01"` |
| incident_severity | text | Incident severity | `"example-01"` |
| incident_action | text | Incident response measure | `"example-01"` |
| incident_status | text | Status of incident handling | `"example-01"` |
| incident_close_reason | text | Reason for closing the incident | `"example-01"` |

### Article 34

| Field | Type | Meaning | Example / format |
|---|---|---|---|
| risk_mgmt_ref | text | Reference to the risk management plan document | `"example-01"` |
| control_applied | boolean | Whether the control was implemented | `True` |
| control_type | text | Control category | `"example-01"` |
| control_outcome | text | Outcome of the control | `"example-01"` |
| control_version | text | Version of the control rule applied | `"example-01"` |
| explanation_policy_ref | text | Reference to the explanation plan document | `"example-01"` |
| decision_basis | text | Main criteria used to produce the final result | `"example-01"` |
| training_data_reference | text | Reference to the overview of training data | `"example-01"` |
| explanation_available | boolean | Whether an explanation can be provided | `True` |
| explanation_delivery_status | text | Result of delivering the explanation | `"example-01"` |
| user_protection_policy_ref | text | Reference to the user protection plan document | `"example-01"` |
| user_protection_action | text | User protection measure | `"example-01"` |
| protection_reason | text | Reason for the protection measure | `"example-01"` |
| protection_outcome | text | Outcome of the protection measure | `"example-01"` |
| oversight_policy_ref | text | Reference to the document describing the human management and supervision framework | `"example-01"` |
| supervisor_id | text | Identifier of the manager or supervisor | `"example-01"` |
| supervisor_name | text | Name of the manager or supervisor | `"example-01"` |
| supervisor_contact | text | Contact details of the manager or supervisor | `"example-01"` |
| supervisor_role | text | Position of the manager or supervisor | `"example-01"` |
| intervention_trigger | text | Conditions for intervention | `"example-01"` |
| intervention_authority | text | Authority and scope of intervention | `"example-01"` |
| oversight_required | boolean | Whether human review is required | `True` |
| oversight_request_status | text | Status of the request for human review | `"example-01"` |
| oversight_handover_status | text | Status of handover of the item for review | `"example-01"` |
| reviewer_id | text | Identifier of the reviewer | `"example-01"` |
| reviewer_role | text | Position of the reviewer | `"example-01"` |
| review_status | text | Progress of the review | `"example-01"` |
| review_result | text | Result of the review | `"example-01"` |
| human_override | text | Whether the result was changed by a person | `"example-01"` |
| override_reason | text | Reason for the change | `"example-01"` |
| evidence_document_id | bigint | Reference to the document supporting the measure | `1` |
| document_version | text | Version of the retained document | `"example-01"` |
| retention_status | text | Retention status of the document | `"example-01"` |
| publication_status | text | Whether key information was published | `"example-01"` |
| publication_url | text | Location of publication | `"example-01"` |
| committee_matter | text | Matter subject to committee deliberation and resolution | `"example-01"` |
| alt_compliance_basis | text | Basis for treating measures taken under another statute as a substitute | `"example-01"` |
| downstream_exemption | boolean | Whether implementation is deemed complete because there was no material functional change | `True` |

### Articles 33, 35 and 36

There are no SDK input fields for these three provisions. Related information is managed in the service's administration screens and through its document registration workflow.

## About this repository

This repository mirrors the source of the `xsoft-comply` package published on PyPI. Comments in the source code are written in Korean. Releases are published on PyPI; see `RELEASES.md` for the release history and file checksums (in Korean).

## Licence and contact

Licensed under the MIT Licence. For usage and recovery enquiries, write to `contact@xsoftcomply.com`.
