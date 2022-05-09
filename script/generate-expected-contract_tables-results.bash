#!/usr/bin/env bash

echo "generating expected results for test_generate.."
cargo test -- --test test_generate --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash
