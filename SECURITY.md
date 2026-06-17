# Security Policy

This repository contains synthetic demo code and generated test data only. Do not
open public issues that include real credentials, tokens, private keys, customer
data, or production infrastructure details.

If you find a security issue in the demo code, report the behavior without
including secrets or sensitive data. Rotate any credentials that were accidentally
used with the demo.

The demo expects local runtime configuration in `.env`, which is ignored by Git.
Keep service account keys, Git tokens, and other credentials outside the
repository. Use Google Cloud Secret Manager for the Dataform Git connection.
