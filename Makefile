requirements.txt: Pipfile Pipfile.lock
	pipenv lock -r > requirements.txt

requirements-dev.txt: Pipfile Pipfile.lock
	pipenv lock -d -r > requirements-dev.txt

install: requirements.txt
	pip install -r requirements.txt

dev: install requirements-dev.txt
	pip install -r requirements-dev.txt

clean:
	rm -rf requirements*.txt
