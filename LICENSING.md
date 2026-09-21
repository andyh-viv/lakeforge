# Licensing

Lakeforge is **source-available, not open source**. It is licensed under the
[Lakeforge Revenue-Restricted License v1.0](LICENSE). This page is a
plain-language summary to help you work out whether you need a commercial
license. **The [LICENSE](LICENSE) text governs; this summary has no legal
force.**

## The short version

| Your Organization's Annual Revenue | What you may do |
| --- | --- |
| **Under USD $10,000,000** | Use Lakeforge freely, for any purpose including commercial and production use, under the LICENSE terms. |
| **USD $10,000,000 or more** | You need a **Commercial License** before you use it. Nothing in the public license grants you that right. |

"Annual Revenue" is the **total gross revenue of your whole Organization** —
you plus any entity that controls you, is controlled by you, or is under common
control with you (more than 50% ownership or control). It is measured for the
most recently completed fiscal year, in USD, before deductions for costs or
taxes. You cannot get under the threshold by splitting the group into separate
deployments, affiliates or contracting parties.

If you cross the threshold during a fiscal year, your rights continue to the end
of that fiscal year and then you need a commercial license to keep using it.

## Commercial licensing

Contact **andy.huangyh@gmail.com** with your Organization's legal name, its
Annual Revenue, and how you intend to use Lakeforge. Commercial licenses are
granted in writing; an enquiry, a download, or a message is not a license.

## What is not restricted

- **Third-party components.** Lakeforge depends on Rust crates, JavaScript
  packages and Python libraries that are licensed by their own authors (mostly
  Apache-2.0 and MIT). This license does not apply to them and does not reduce
  any rights you have to them. Their terms are separate, and in addition to,
  this license. See `Cargo.lock`, `web/package-lock.json` and
  `python/lakeforge-sdk/pyproject.toml`.
- **Your own code.** Code you write that merely uses Lakeforge — for example
  jobs, notebooks or pipelines that call its API or SQL surface — is yours.
  A *Modified Version* of Lakeforge itself (see LICENSE Section 1.5) stays under
  this license.

## Why this is not an open source license

Under this license, permission depends on who is asking (their revenue), so it
does not meet the Open Source Initiative's definition, which forbids
discrimination against fields of endeavour or persons. Lakeforge is therefore
"source-available" or "fair-source" — not OSI open source. GitHub may report the
license as `NOASSERTION` or as a custom license; that is expected.

## Contributing

By submitting a contribution you agree it is licensed under the LICENSE terms,
and you grant the Licensor the right to use and relicense it (LICENSE
Section 5) so that the project can offer commercial licenses. Open an issue
before a large contribution.

## Reporting license compliance

If you believe your Organization's use, or someone else's, does not comply with
the LICENSE, contact **andy.huangyh@gmail.com**.
