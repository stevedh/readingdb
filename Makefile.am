
SUBDIRS = src
EXTRA_DIST = python

debbuild:
	dpkg-buildpackage -i -I -rfakeroot -uc -us -S
