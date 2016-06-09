
all:

clean:
	rm -rf build *.egg-info */__pycache__ tests/*.pyc
	rm -rf debian/python-* debian/files debian/*.log
	rm -rf debian/*.substvars debian/*.debhelper debian/*-stamp
	rm -rf .pybuild MANIFEST

deb:
	debuild -us -uc -b

xclean: clean
	rm -rf .tox dist
