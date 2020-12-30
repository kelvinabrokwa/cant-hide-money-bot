.PHONY: lint
lint:
	flake8 cant_hide_money_bot
	mypy cant_hide_money_bot

.PHONY: test
test:
	pytest -s test