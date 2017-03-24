test:
	# hack to avoid needing to install this client as a package
	# from http://stackoverflow.com/a/34140498
	python -m pytest tests

lint:
	flake8 examples scitran_client

publish_docs:
	$(eval CURRENT_SHA := $(shell git rev-parse HEAD))
	git checkout gh-pages
	git reset --hard $(CURRENT_SHA)
	pdoc --html scitran_client --html-dir docs --overwrite
	git add docs
	git commit -am "add docs"
	git push origin gh-pages -f
