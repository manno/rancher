# TODO: local-first chart lookup for OCI URLs in git ClusterRepos

`git.LocalChart` looks up a chart whose `index.yaml` URL is remote (`oci://`,
`http(s)://`) at `assets/<name>/<name>-<version>.tgz` in the local checkout,
and verifies the tarball against the index entry's `digest` when set. Two
decisions are open before this can be relied on.

## 1. Asset image layout contract

The lookup only hits if the `rancher-assets` build writes the tarballs it
pulls from OCI to `assets/<name>/<name>-<version>.tgz`, the layout release
branches use today. RFD 0068 says the build "includes them in the checkout it
bundles" but doesn't name a path.

- [ ] Agree on the path with the `rancher-assets` build.
- [ ] Record it in RFD 0068 (Runtime consumption section).

## 2. Digest source in `index.yaml`

A Helm `index.yaml` digest is the sha256 of the `.tgz`. An OCI manifest digest
is a different value. If `rancher/charts` CI records the OCI manifest digest,
every local lookup fails verification and falls back to the registry, which
breaks installs in a hard air gap.

- [ ] Decide what `rancher/charts` CI writes: the tarball sha256, or no digest
      (which skips verification).
- [ ] Make sure the tarball the asset image bundles is byte-identical to the
      one the digest was computed from.
