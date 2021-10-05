#!/bin/bash

echo "generating expected results for test_generate.."
cargo test -- --test test_generate --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash

echo "generating expected results for test_block.."
cargo test -- --test test_process_block --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash
