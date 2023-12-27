.PHONY: clean
clean:
	rm -rf ./tmp


.PHONY: test
test:
	poetry run pytest
