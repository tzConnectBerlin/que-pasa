#!/bin/bash

cargo +nightly test -- --test test_block --nocapture 2>/dev/null \
    | sed -n '/^cat\ >\ [^$]*\<\<ENDOFJSON$/,/^ENDOFJSON$/p' \
    | bash
