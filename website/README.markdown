Code for the S4 website

This website uses the [nanoc](http://nanoc.stoneship.org/) static website generator

Entry pages are written with haml and the documentation is written with markdown.


# To compile the site:

* Install nanoc: `gem install nanoc`
* `nanoc compile`

The generated static website is in `output/`


There are also a number of dependencies on other gem, error messages are explicit about which ones and how to install them.

We also use pygments for code syntax highlighting. It's a python program, see [here](http://pygments.org/docs/installation/) for installing.

# To upload the site to apache, commit the generated website to svn (site/ directory)

The svn is located at [https://svn.apache.org/repos/asf/incubator/s4/](https://svn.apache.org/repos/asf/incubator/s4/)

	cp -R output/* $S4_SVN_LOC/site
	cd $S4_SVN_LOC
	svn update
	svn status
	svn add <whatever is missing>
	svn commit --username <apache username> -m "commit message"

With svnpubsub, the website is automatically updated
