.PHONY: all check-deps build install clean done

WHEEL_DIR = lib/target/wheels
EGG_INFO  = cuckooget.egg-info src/cuckooget.egg-info

all: check-deps build

check-deps:
	@command -v pyenv >/dev/null || (echo "Please install pyenv" && exit 1)
	@command -v python3 >/dev/null || (echo "Please install Python3" && exit 1)
	@command -v maturin >/dev/null || { echo "Please install maturin (pip install maturin)"; exit 1; }

build:
	@echo "Building with maturin..."
	cd lib && maturin build

install:
	pip install . --force-reinstall
	pip install $(WHEEL_DIR)/*.whl --force-reinstall
	@echo "Done! How to use \`ck -h\`"

clean:
	rm -rf build lib/target $(EGG_INFO)

done:
	@echo "All steps completed."

