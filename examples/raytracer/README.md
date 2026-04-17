# raytracer

A future `wasm32-wasi` example job that renders a small 3D scene across the
ants mesh. Each task renders a tile or scanline of the final image; the
orchestrator stitches tiles back together once every worker reports in.

## Status

Stub only. Full implementation follows **Milestone 3** of
[PROJECT.md](../../PROJECT.md), when end-to-end distribution and result
aggregation are in place.

## Planned shape

- A Rust crate compiled to `wasm32-wasi`.
- Input: scene description plus the tile coordinates this task owns.
- Output: raw pixel bytes for that tile, returned to the orchestrator.
