Implementation of the K-Means Algorithm in S4
=============================================

The [k-means algorithm](http://en.wikipedia.org/wiki/K-means_clustering) can be used for unsupervised clustering of multivariate data. In this example
we use a [data set to predict forest cover type](http://kdd.ics.uci.edu/databases/covertype/covertype.html).
There is also a paper published for the author of this work ([PDF](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.128.2475&rep=rep1&type=pdf))

## The Forest Cover Data Set

Here are the steps I used to prepare the data files. They are located under src/main/resources/

	# Download data set and uncoompress.
	wget http://kdd.ics.uci.edu/databases/covertype/covtype.data.gz
	gunzip covtype.data.gz 

	# Remove some columns and put the class label in the first column.
	gawk -F "," '{print $55, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10}' covtype.data  > covtype-modified.data

	# Randomize data set.
	sort -R covtype-modified.data > covtype-random.data

	# Check number of data points.
	wc -l covtype-*
	#  581012 covtype-modified.data
	#  581012 covtype-random.data

	# Create a train and a test set.
	tail -100000 covtype-random.data > covtype-test.data
	head -481012 covtype-random.data > covtype-train.data

	wc -l covtype-{train,test}.data
	#  481012 covtype-train.data
	#  100000 covtype-test.data
	#  581012 total

## Application Graph

To estimate the centroids of the clusters using k-means, we need to follow these steps:

* Initialize the centroids by picking k vectors at random from the data set.
* Inject the observation data vectors.
* Compute the euclidean distance between each observation and the centroids.
* Select the id of the cluster with the smallest distance (the hypothesized class for that observation).
* Repeat for all the vectors in the data set and compute the average distance for the data set.
* Reestimate the centroids by computing the mean for each clusters using the observations and their hypothesized class.
* Repeat the whole process by reinjecting the data until the average distance converges.

Clearly, estimating the k-means centroids requires batch processing. That is we pass the same data through the application several times and we know exactly how many observation vectors we have available. With this knowledge we design the following application graph:

![S4 Counter](https://github.com/leoneu/s4-piper/raw/master/etc/s4-kmeans-example.png)

We choose to use events of type ObsEvent to communicate between Processing Elements. The event is immutable and can only be created using the constructor. The fields are:

* _obsVector_ is the observation vecotr. The size of the float array should be the same for all the vectors.
* _distance_ is the euclidean distance between the vector and the centroid.
* _index_ is a unique identifider for the event. 
* _classId_ is the true class for the vector as it was labeled in the original data set.
* _hypId_ is the hypothesized class for the vector after using the classification algorithm.

Also notice that the graph has a loop. This creates a minor challenge to create the application graph. To solve 
this problem we added a setter method to set the distanceStream in ClusterPE.


