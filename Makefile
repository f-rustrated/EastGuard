# Repeatedly runs a single test until it fails, with full tracing output.
# Useful for catching flaky tests that only fail occasionally.
#
# Usage:
#   make test-flaky TEST=metadata_visible          # run up to 100 times (default)
#   make test-flaky TEST=leader_elects_after_kill N=50
#
# TEST  - (required) name of the test function to run
# N     - (optional) number of iterations before declaring success (default: 100)
N ?= 100

test-flaky:
	@for i in $$(seq 1 $(N)); do \
		echo "=== run $$i ==="; \
		RUST_LOG=info cargo test $(TEST) -- --nocapture --test-threads=1 || { echo "FAILED on run $$i"; exit 1; }; \
	done
