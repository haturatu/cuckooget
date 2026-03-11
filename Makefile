.PHONY: all check-deps build install develop clean done

EGG_INFO  = cuckooget.egg-info src/cuckooget.egg-info

all: check-deps build

check-deps:
	@command -v python3 >/dev/null || (echo "Please install Python3" ; exit 1;)
	@command -v maturin >/dev/null || ( echo "Please install maturin (pip install maturin)"; exit 1; )

build:
	@echo "Building with maturin..."
	python3 -m maturin build --manifest-path lib/Cargo.toml

install:
	python3 -m pip install . --no-build-isolation --force-reinstall
	@echo "Done! How to use \`ck -h\`"

develop:
	python3 -m pip install -e . --no-build-isolation
	@echo "Editable install completed."

clean:
	rm -rf build lib/target $(EGG_INFO)

done:
	@echo "All steps completed."
