Modeling Framework for S4
=========================

In this package we implemented an application that can estimate model parameters in batch mode and run the 
model in streaming mode. To evaluate the application we use a [publicly available labeled data set to predict 
forest cover type](http://kdd.ics.uci.edu/databases/covertype/covertype.html).
For details about the data set please download the paper published by the author of this work. 
([PDF](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.128.2475&rep=rep1&type=pdf))

## The Forest Cover Data Set

Here is a description of the data. There are 7 types of forest cover. Each observation vector represents an area of 
forest with a type of cover and ten measurements (Elevation, Aspect, Slope, etc.) There is a total of 581,012 observation
vectors in the data set. 

<pre>
Name                                    Data Type       Measurement        Description

Cover_Type (7 types)                    integer         1 to 7             Forest Cover Type designation
Elevation                               quantitative    meters             Elevation in meters
Aspect                                  quantitative    azimuth            Aspect in degrees azimuth
Slope                                   quantitative    degrees            Slope in degrees
Horizontal_Distance_To_Hydrology        quantitative    meters             Horz Dist to nearest surface water features
Vertical_Distance_To_Hydrology          quantitative    meters             Vert Dist to nearest surface water features
Horizontal_Distance_To_Roadways         quantitative    meters             Horz Dist to nearest roadway
Hillshade_9am                           quantitative    0 to 255 index     Hillshade index at 9am, summer solstice
Hillshade_Noon                          quantitative    0 to 255 index     Hillshade index at noon, summer soltice
Hillshade_3pm                           quantitative    0 to 255 index     Hillshade index at 3pm, summer solstice
Horizontal_Distance_To_Fire_Points      quantitative    meters             Horz Dist to nearest wildfire ignition points


Forest Cover Types:	
    1 -- Spruce/Fir
    2 -- Lodgepole Pine
    3 -- Ponderosa Pine
    4 -- Cottonwood/Willow
    5 -- Aspen
    6 -- Douglas-fir
    7 -- Krummholz

Class Distribution

Number of records of Spruce-Fir         211840 
Number of records of Lodgepole Pine     283301 
Number of records of Ponderosa Pine     35754 
Number of records of Cottonwood/Willow  2747 
Number of records of Aspen              9493 
Number of records of Douglas-fir        17367 
Number of records of Krummholz          20510 	
 		
Total records                           581012
</pre>

Here are the steps I used to download and prepare the data files. The files are located in the project under src/main/resources/.

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

## Probabilistic Models

The modeling package (io.s4.model) provides a basic interface for implementing probabilistic models. The main methods are:

* **update()** - updates sufficient statistics given an observation vector.
* **estimate()** - estimates model parameters using teh sufficient statistics.
* **evaluate()** - computes the probability.

The first implementation is a Gaussian model which we use in this example to classify the tree cover given the features.


## Application Graph

### Training

We use a Gaussian distribution with a diagonal covariance matrix to model the the cover types.
To estimate the mean and variance for each cover type we follow these steps:

* Determine number of train vectors, vector size, and number of classes from the 
  train data set. (io.s4.example.classifier.Controller)
* Create events of type ObsEvent and inject them into ModelPE with key = classId
* There is a ModelPE instance for each cover type (a total of seven cover types). 
  For each observation vector that matches the cover type, call the update() method in teh Gaussian model.
* Once all the train events are received, update the mean and variance for each model.


We choose to use events of type ObsEvent to communicate between Processing Elements. The event is immutable and can only be created using the constructor. The fields are:

* _obsVector_ is the observation vector. The size of the float array should be the same for all the vectors.
* _prob_ is the probability of the vector given the model.
* _index_ is a unique identifier for the event. 
* _classId_ is the true class for the vector as it was labeled in the original data set.
* _hypId_ is the hypothesized class for the vector after using the classification algorithm.
* _isTraining_ is a boolean to differentiate between train and test modes.


Here is a snippet of ObsEvent.java:

	public class ObsEvent extends Event {

		final private float[] obsVector;
		final private float prob;
		final private long index;
		final private int classId;
		final private int hypId;

		public ObsEvent(long index, float[] obsVector, float prob, int classId,
				int hypId) {
			this.obsVector = obsVector;
			this.prob = prob;
			this.index = index;
			this.classId = classId;
			this.hypId = hypId;
		}
	
Also notice that the graph has a loop. This creates a minor challenge to create the application graph. To solve 
this problem we added a setter method to set the distanceStream in ClusterPE.

### Testing

* Compute the posterior probability of the observation for each model.
* For an observation vector, select model id with the highest posterior probability.
* Send ObsEvent with HypID back to ModelPE instance using ClassID as key.
* Update results

We compute the confusion matrix where a row corresponds to a class and columns to hypotheses. 
The results are shown in percent. The diagonal shows the accuracy of the classifier for each class. 

Here is confusion matrix when we only estimate the mean of each class,
For most classes the accuracy is better than chance (1/7 => 14%). As expected, a model that only uses
the mean of each class is not very good at explaining the data. We can do better with
a probabilistic model.

<pre>
           0     1     2     3     4     5     6
        ----------------------------------------
    0:  13.2  19.2   7.2   0.0  24.0   3.0  33.4
    1:  11.5  19.7   9.9   0.8  27.9   5.9  24.2
    2:   1.9   4.2  23.5  37.3   7.3  25.7   0.0
    3:   0.0   0.0   5.6  50.4   0.0  43.9   0.0
    4:   3.4  11.7  21.8   0.1  44.8  15.3   2.9
    5:   0.8   9.3  22.5  29.0  10.1  28.3   0.0
    6:   9.3   9.5   3.7   0.0  25.8   1.7  50.0
</pre>
When we run the classifier using the probabilistic model (just one Gaussian distribution per cover type) We get the following confusion matrix:

<pre>
           0     1     2     3     4     5     6
        ----------------------------------------
    0:  67.4  25.2   0.7   0.0   1.0   0.3   5.5 
    1:  24.1  65.8   4.6   0.0   2.3   1.9   1.4
    2:   0.0  19.7  64.2   3.7   0.3  12.0   0.0
    3:   0.0   0.4  38.9  48.8   0.0  11.8   0.0
    4:   0.0  68.8   4.8   0.0  24.2   2.2   0.0 
    5:   0.0  18.3  47.4   2.3   0.5  31.5   0.0
    6:  70.5   0.6   0.7   0.0   0.0   0.0  28.2
</pre>

This is a big improvement given how simple our model is. There are still significant errors for some cover types. For example and 38.9% of 
type 3 is classified as type 2 and 68.8% of class 4 is classified as class 1, etc. We need a better model to improve accuracy.

