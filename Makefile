.PHONY: all check-deps build install clean done

all: check-deps build

check-deps:
	@command -v pyenv >/dev/null || (echo "Please install pyenv" && exit 1)
	@command -v python3 >/dev/null || (echo "Please install Python3" && exit 1)

build:
	cd lib && maturin build

install:
	pip install . --force-reinstall
	pip install lib/target/wheels/*.whl --force-reinstall
	@echo "Done! How to use \`ck -h\`"

clean:
	rm -rf build lib/target src/cuckooget.egg-info cuckooget.egg-info

done:
	@echo "All steps completed."

