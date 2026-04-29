# Data — CDISC Pilot 01

This directory is populated by `make data` from the project root:

```
data/
├── sdtm/             # SDTM XPTs (DM, AE, EX, LB, SV, VS, SUPPDM)
├── adam_reference/   # CDISC's published ADaM XPTs — used only by the diff check
└── define/           # define.xml + value-level metadata
```

Source: <https://github.com/cdisc-org/sdtm-adam-pilot-project>. The CDISC Pilot 01 package is released by CDISC for teaching and conformance testing under a permissive license; redistribution is allowed but a fresh `make data` against the upstream repo is the recommended path.

The downloaded files are not committed to this repo. Re-run `make data` to refresh.
