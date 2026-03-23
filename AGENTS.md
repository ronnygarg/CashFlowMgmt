\# Project guidance for Codex



\## Core rules

\- Do not fabricate or force a merge key between consumption and vend datasets.

\- Keep the base directory configurable.

\- Respect the existing starter files:

&#x20; - requirements.txt

&#x20; - config/app\_config.yaml

&#x20; - README.md

\- Keep code modular and easy to refactor.

\- Make the Data Quality page prominent.

\- Treat vend datetime parsing as provisional if issuedate is incomplete or time-only.



\## Expected structure

\- Keep path handling centralised in a reusable helper module.

\- Preserve the multipage Streamlit structure.

\- Save processed outputs as Parquet in data/processed/.



\## Implementation style

\- Prefer reusable functions over monolithic scripts.

\- Include comments, docstrings, and clear TODOs for future combined analysis.

\- Do not hide data limitations.

