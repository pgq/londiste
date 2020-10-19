
all:

clean:
	rm -rf build *.egg-info */__pycache__ tests/*.pyc
	rm -rf .pybuild MANIFEST

xclean: clean
	rm -rf .tox dist

sdist:
	python3 setup.py sdist

upload:
	twine upload dist/*.gz

