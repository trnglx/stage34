# Contributing

Use isolated branches and reproducible offline tests. Changes to authority, fencing,
approval, transport or verification require independent review and regression tests that
fail before the fix. Do not introduce provider calls into default tests.

Run `python3 -m unittest discover -s tests` and build `runtime/` with its locked dependencies.
Source candidates must pass secret/privacy/license review before publication. Do not copy
private profiles, accounts, transcripts, machine inventories or repository history here.
