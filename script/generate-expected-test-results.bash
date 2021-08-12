#!/bin/bash

echo "generating expected results for test_generate.."
cargo +nightly test -- --test test_generate --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash

echo "generating expected results for test_block.."
cargo +nightly test -- --test test_block --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash
