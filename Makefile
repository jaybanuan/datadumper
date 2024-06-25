.PHONY: clean
clean:
	rm -rf ./output


.PHONY: test
test:
	poetry run pytest -s
