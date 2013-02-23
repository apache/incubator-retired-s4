Code for the S4 website

This website uses the [nanoc](http://nanoc.stoneship.org/) static website generator

Entry pages are written with haml and the documentation is written with markdown.


# To compile the site:

* Install nanoc: `gem install nanoc`
* `nanoc compile`

The generated static website is in `output/`

# To upload the site to apache

## first, commit the generated website to svn

	cp -R output/* $S4_SVN_LOC/site
	cd $S4_SVN_LOC
	svn update
	svn status
	svn add <whatever is missing>
	svn commit --username <apache username> -m "commit message"

## then checkout into web server
	ssh people.apache.org
	cd /www/incubator.apache.org/content/s4
	svn checkout http://svn.apache.org/repos/asf/incubator/s4/site .