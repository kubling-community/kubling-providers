# Kubling Providers repository guidance

## Public repository boundary

This is a public open-source repository. Treat every tracked file, commit
message, branch, pull request, workflow log and release artifact as public.

Tracked documentation may describe this repository's public protocol, SDKs,
providers, deployment, contribution process and release workflows. Maintainer
documentation is welcome when it concerns public artifacts and automation.

Keep contributions scoped to this project and understandable using its public
context. Design documents and decision records are welcome when they are
complete, project-specific and intentionally published for community review.
Keep transient working notes, experiments and coordination drafts outside
tracked files until they are ready for that audience.

References to other systems must use public names and documentation. Examples
must be portable: use placeholders instead of machine-specific paths, real
service endpoints, account identifiers, credentials, certificates or
production configuration.

## Documentation style

- Write for external users, operators, contributors and community maintainers.
- Keep READMEs concise and generally version-neutral.
- Describe integration points through public contracts and documentation.
- Examples must use placeholders and public endpoints only.
- Do not remove useful public usage documentation while cleaning up internal
  wording.

## Approval and scope

- Prefer small, reviewable blocks with focused validation.
- Never hand-edit generated sources; use the repository generator and check the
  resulting diff.
- Do not update dependency locks for unpublished artifacts.
- Never commit or push without explicit user approval for that action.
- Pull requests, tags, releases and artifact publication also require separate
  explicit approval.
