lint:
	flake8 examples scitran_client

CURRENT_SHA = $(git rev-parse HEAD)

publish_docs:
	-git checkout -b gh-pages
	git checkout gh-pages
	git reset --hard $(CURRENT_SHA)
	pdoc --html scitran_client --html-dir docs --overwrite
	git add docs
	git commit -am "add docs"
	git push origin gh-pages -f
