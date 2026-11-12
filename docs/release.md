# Release gates

This tree is a private, reviewable release candidate. Do not publish it automatically.

1. Verify source and artifact manifests/checksums; scan all source, archives and the complete
   new public Git history for secrets, personal data and private context. Review licenses and
   upstream ownership, including provider SDK terms. No binaries/dependencies are vendored.
2. Install and run the unrelated-repository offline quickstart on supported controller OSes.
   Build/test the locked native runtime and verify Linux Docker worker isolation on actual
   supported hosts. Record exact source and image digests, OS versions and limitations.
3. Complete independent authority/contract, security/privacy and deployment/release review.
   Mocks do not establish actual-provider quality or account-backed two-provider acceptance.
4. Prepare a clean-history repository with reviewed source only; publish no private ancestor
   history, credentials, local state or generated caches. Freeze repository target and SHA.
5. Obtain owner approval for the exact public tree, target repository and disclosures.

Paid inference and benchmark execution remain disabled. Enabling a provider requires a
secure connection, explicit repository/model authorization, verified pricing, bounded limits
and separate execution authorization. Installation or a passing mock demo grants none of these.

Support: Python 3.9+ on macOS/Linux controllers; Node 24/26; Linux Docker workers, including
Docker Desktop on macOS/Windows. Native Windows controller support is not established.
