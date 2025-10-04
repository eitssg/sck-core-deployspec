# Copilot Instructions (Submodule: sck-core-deployspec)

## Plan → Approval → Execute (Mandatory)
Provide a plan (numbered actions) and await approval before performing non-trivial actions. Refer to root for details & exceptions.

- Tech: Python package (deploy specs).
- Precedence: Local first; fallback to root `../../.github/`.
- Conventions: Align with `../sck-core-ui/docs/backend-code-style.md` when applicable.

## Google Docstring Requirements
**MANDATORY**: All docstrings must use Google-style format for Sphinx documentation generation:
- Use Google-style docstrings with proper Args/Returns/Example sections
- Napoleon extension will convert Google format to RST for Sphinx processing
- Avoid direct RST syntax (`::`, `:param:`, etc.) in docstrings - use Google format instead
- Example sections should use `>>>` for doctests or simple code examples
- This ensures proper IDE interpretation while maintaining clean Sphinx documentation

## Contradiction Detection
- Compare against backend standards + root precedence.
- If conflict, warn and offer alignment options with example.
- Example: "Non-standard deploy spec fields conflict with consumers; adhere to documented schema or update schema/docs."

## Standalone clone note
If cloned standalone, see:
- UI/backend conventions: https://github.com/eitssg/simple-cloud-kit/tree/develop/sck-core-ui/docs
- Root Copilot guidance: https://github.com/eitssg/simple-cloud-kit/blob/develop/.github/copilot-instructions.md
 
