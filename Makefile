build:
	python3 -m build
release:
	python3 -m twine upload --repository pypi dist/*
ensure:
	python3 -m pip install --upgrade build twine
