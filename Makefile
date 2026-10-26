PY ?= python3
.PHONY: install demo serve test test-slow bench lint clean
install: ; $(PY) -m pip install -r requirements.txt
demo: ; $(PY) -m veylstream setup-demo       # creates the demo workspace + admin (random password shown once) and seeds simulated history
serve: ; $(PY) -m veylstream serve
test: ; $(PY) -m unittest discover -s tests -v
test-slow: ; VEYLSTREAM_SLOW=1 $(PY) -m unittest discover -s tests -p test_e2e_slow.py -v
bench: ; $(PY) scripts/benchmark.py all
lint: ; ruff check veylstream tests scripts && mypy veylstream --ignore-missing-imports
clean: ; rm -rf data/runtime
