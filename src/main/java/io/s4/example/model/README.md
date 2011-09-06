# Modeling Framework for S4

## The Pattern Recognition Problem

In this example we show how to design a pluggable probabilistic modeling framework in S4. The task is to classify incoming objects into 
well defined categories. Each object is represented by an observation vector that represents the object, also called 
features of the object. The variability and consistency of the features within a category and between categories will 
determine the accuracy of the classifier and the complexity of the models. Because it is impossible to achieve perfect accuracy in real-world systems, 
we use probabilistic models to classify the objects. So instead of just assigning a category to each object, the model will 
provide the probability that the object belongs to a category. The final decision may depend on several factors, for example,
the cost of wrongly assign a category to an object may not be the same for all categories. In this example, we assume that 
the cost is the same for all categories and simply select the category whose model has the highest probability. 

Probabilistic models are widely used in many application areas. A distributed systems may be needed when decisions need to be made 
with low delay and by processing a large number of incoming events. S4 is designed to scale to an unlimited number of nodes providing
the flexibility to run complex algorithms, process at high data rates, and deliver results with low latency. Typical applications include
processing user events in web applications, analyzing financial market data, monitoring network traffic, and many more. 

To learn more, get a copy of this classic book: "Pattern Classification" by R. Duda, P. Hart, and D. Stork.

## The Approach 
 
In this example we implemented an application that uses a train data set to estimate model parameters. Because most estimation 
algorithms learn iteratively, we inject the train data set several times until a stop condition is achieved. To train the models,
we run S4 in batch mode. That is, we push the data at the highest possible rate and when a queue fills up, we let the system block 
until more space in the queues become available. In other words, no event data is ever lost in this process. To achieve this, we remove all 
latency constraints and let the process run for as long as needed until all the data is processed. This approach is quite similar 
to MapReduce except that the data is injected sequentially from a single source.

Once the model parameters are estimated we are ready to run a test. In real-time applications, we would have no control over the speed of the
incoming data vectors. If we didn't have sufficient computing resources to process all the data within the time constraints, we would be 
forced to either lose some of the data (load shedding) or switch to a less complex classification algorithm. In this example, we simply assume
that there is no data loss.

## The Forest Cover Data Set

To evaluate the application we use a [publicly available labeled data set to predict 
forest cover type](http://kdd.ics.uci.edu/databases/covertype/covertype.html).
For details about the data set please download the paper published by the author of this work. 
([PDF](http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.128.2475&rep=rep1&type=pdf))

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

Here are the steps I used to download and prepare the data files. The files are located in the project under 
[src/main/resources/](https://github.com/leoneu/s4-piper/tree/master/src/main/resources).

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

## Implementation of the Probabilistic Models

The modeling package [io.s4.model](https://github.com/leoneu/s4-piper/tree/master/src/main/java/io/s4/model) provides 
a basic interface for implementing probabilistic models. The main methods are:

* **update()** - updates sufficient statistics given an observation vector.
* **estimate()** - estimates model parameters using the sufficient statistics.
* **prob()** - computes the probability.

The abstract class [io.s4.model.Model](https://github.com/leoneu/s4-piper/tree/master/src/main/java/io/s4/model/Model.java)
is used to build the models. There are no dependencies with any specific implementation of a 
model because the code only uses Model to estimate and run the classifier.

There are two concrete implementation of Model:

 * [Gaussian Model](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/model/GaussianModel.java) a single
   Gaussian distribution with parameters _mean_ and _variance_.
 * [Gaussian Mixture Model](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/model/GaussianMixtureModel.java) 
   A mixture of Gaussian distributions with parameters _mixture weights_ and each Gaussian component with parameters _mean_ and
   _variance_.

More model can be implemented and thanks to object oriented design, swapping models is as easy as
editing one line of code in the [Guice](http://code.google.com/p/google-guice/) 
[Module](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/example/model/Module.java).

When deployed in a cluster the same code will be  run on many physical servers without changing any application code. 
As an application developer you don't have to worry about how distributed processing works. Scientist can focus on writing
model code and dropping it in the right place. Moreover, the same code can be used to run experiments in batch mode and to
deploy in a real-time production environment.

## Training

Here is the basic structure of the program:

* Determine number of train vectors, and number of categories from the 
  train data set. [io.s4.example.model.Controller](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/example/model/Controller.java)
* Create events of type ObsEvent and inject them into ModelPE with key = classId (classId refers to the category)
* There is a [ModelPE](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/example/model/ModelPE.java) 
  instance for each cover type (a total of seven cover types in this example with classId: 0-6). 
  For each observation vector that matches the cover type, call the update() method in the Model.
* Once all the train events are received, estimate the parameters for each model.


We choose to use events of type ObsEvent to communicate between Processing Elements and ResultEvent to send the results to the MetricsPE. 
The events are immutable and can only be created using the constructor. The ObsEvent fields are:

* _obsVector_ is the observation vector. The size of the float array should be the same for all the vectors.
* _prob_ is the probability of the vector given the model.
* _index_ is a unique identifier for the event. 
* _classId_ is the true category for the vector as it was labeled in the original data set.
* _hypId_ is the hypothesized category for the vector returned by classification algorithm.
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


The application graph is defined in [MyApp](https://github.com/leoneu/s4-piper/blob/master/src/main/java/io/s4/example/model/MyApp.java)
To define a graph we simply connect processing elements to streams. When teh objects in the graph are created, they are simply a representation
of the application. The real work is done by the processing element instances that may reside anywhere in the cluster.

Notice that the application graph has a cycle (events go from ModelPE to MaximizerPE and back). This creates a minor challenge to create the 
application graph. To solve this problem we added a setter method to set the distanceStream in ClusterPE.

## Testing

For testing we follow the following steps:

* Compute the posterior probability of the observations for each model.
* For an observation, select model id with the highest posterior probability. This is done by MaximizerPE: for each observation 
  get all seven probabilities (one from each model), select the category with the highest probability. Self destroy the instance of MaximizerPE 
  once the work is done.
* MaximizerPE sends ObsEvent with HypID back to the ModelPE instance using ClassID as key.
* Send results to MetricsPE.
* MetricsPE accumulate counts to compute the confusion matrix and the overall accuracy. In the confusion matrix rows correspond 
  to true categories and columns to hypotheses. The diagonal 
  shows the percentage of observations that were correctly categorized and the off-diagonal numbers are the errors. 

## Experiments

We first run the classifier using the Gaussian model, that is, we model each class using a Gaussian probability density 
function for which we need to estimate its parameters (mean and variance). 

To run the experiment, we bind the Model type to the GaussianModel class in Module.java as follows:

<pre>
    protected void configure() {
        if (config == null)
            loadProperties(binder());

        int vectorSize = config.getInt("model.vector_size");
        int numGaussians = config.getInt("model.num_gaussians");

        bind(Model.class).toInstance(
        new io.s4.model.GaussianModel(vectorSize, true));

    }
</pre>

With this binding the GaussianModel instance will be injected in the Controller constructor.

Next, we edit the properties file as follows:

   model.train_data = /covtype-train.data.gz 
   model.test_data = /covtype-test.data.gz
   model.logger.level = DEBUG
   model.num_iterations = 1
   model.vector_size = 10
   model.output_interval_in_seconds = 2

In the properties file we configure the data sets for training and testing, the logger level, the number of iterations 
(we only need one iteration to estimate the mean and variance), the vector size which is 10 for this data set and
how often we want to print partial results, we choose 2-second intervals. A final result will be printed by the 
controller at the end of the experiment.

To run using Gradle, make sure you set the Main class in build.gradle to:

    mainClassName = "io.s4.example.model.Main"

To run the experiment type:

    gradlew run
   
   
 and after a few seconds we get the result:
 
    Confusion Matrix [%]:

           0     1     2     3     4     5     6
        ----------------------------------------
    0:  67.4  25.2   0.7   0.0   1.0   0.3   5.5
    1:  24.1  65.8   4.6   0.0   2.3   1.9   1.4
    2:   0.0  19.6  64.3   3.7   0.3  12.0   0.0
    3:   0.0   0.4  38.6  48.8   0.0  12.2   0.0
    4:   0.0  69.0   4.8   0.0  24.0   2.2   0.0
    5:   0.0  18.3  47.3   2.3   0.5  31.5   0.0
    6:  70.5   0.6   0.7   0.0   0.0   0.0  28.2

    Accuracy:   63.1% - Num Observations: 100000

The observation vectors are correctly categorized in an independent data set at a rate of 63.1%. Note that 85% of the observations 
are in categories 0 and 1. The classifier learned this fact and relied on the prior probabilities to optimize the overall accuracy 
of the classifier. That's why accuracy is higher for these categories. For example, only 3.5% of the observations are in category 6
so the low accuracy of 28.2% has little impact on the overall accuracy. Depending on the application, this may or may not be the right
optimization approach.

Next we, want to try the more sophisticated GaussianMixtureModel. We changed the properties file as follows:

   model.train_data = /covtype-train.data.gz 
   model.test_data = /covtype-test.data.gz
   model.logger.level = DEBUG
   model.num_gaussians = 1
   model.num_iterations = 2
   model.vector_size = 10
   model.output_interval_in_seconds = 2

Note that we are only using one Gaussian per mixture which is equivalent to using the GaussianModel so we expect the results to be identical. 
We need 2 iterations because the model uses the first pass to estimate the mean and variance of the data. This is only useful when using 
more than one mixture component.  

We changed the Module class as follows:

<pre>
    protected void configure() {
        if (config == null)
            loadProperties(binder());

        int vectorSize = config.getInt("model.vector_size");
        int numGaussians = config.getInt("model.num_gaussians");

        bind(Model.class).toInstance(
                new io.s4.model.GaussianMixtureModel(vectorSize, numGaussians,
                        io.s4.model.GaussianMixtureModel.TrainMethod.STEP));
    }
</pre>

and we run the experiment again:

    gradlew run

The result is identical as expected:

    Confusion Matrix [%]:

           0     1     2     3     4     5     6
        ----------------------------------------
    0:  67.4  25.2   0.7   0.0   1.0   0.3   5.5
    1:  24.1  65.8   4.6   0.0   2.3   1.9   1.4
    2:   0.0  19.6  64.3   3.7   0.3  12.0   0.0
    3:   0.0   0.4  38.6  48.8   0.0  12.2   0.0
    4:   0.0  69.0   4.8   0.0  24.0   2.2   0.0
    5:   0.0  18.3  47.3   2.3   0.5  31.5   0.0
    6:  70.5   0.6   0.7   0.0   0.0   0.0  28.2

    Accuracy:   63.1% - Num Observations: 100000


Now let's increase the number of mixture components to two Gaussian distributions per category:


   model.train_data = /covtype-train.data.gz 
   model.test_data = /covtype-test.data.gz
   model.logger.level = DEBUG
   model.num_gaussians = 2
   model.num_iterations = 6
   model.vector_size = 10
   model.output_interval_in_seconds = 2


    Confusion Matrix [%]:

           0     1     2     3     4     5     6
        ----------------------------------------
    0:  66.4  27.8   0.1   0.0   1.5   0.4   3.8
    1:  24.9  63.3   3.6   0.1   4.5   3.0   0.6
    2:   0.0  13.1  65.7   8.2   1.0  12.0   0.0
    3:   0.0   0.4  17.0  80.9   0.0   1.7   0.0
    4:   5.1  50.0   3.0   0.0  37.2   4.7   0.0
    5:   0.0  15.8  39.4   7.5   1.0  36.3   0.0
    6:  71.4   1.1   0.0   0.0   0.0   0.0  27.6

    Accuracy:   62.2% - Num Observations: 100000

The overall accuracy went down from 63.1% to 62.2%. However, we can see a dramatic improvement in category 3 
(from 48.8% to 80.9%) at the cost of a slight degradation in categories 0 and 1. Clearly, using two Gaussians 
per category helped category three. 

To improve the accuracy of the classifier, one could do some additional analysis and come up with an improved 
model until the accuracy is acceptable for the target application. For example, why are so many category 6 observations
classified as category 0? Maybe we need a different number of mixtures per category to allocate more parameters to the 
categories with more training data and fewer to the other ones. Give it a try and let me know. I will add any
models that get better overall accuracy than this one.

## Performance

I tested the execution speed on a single node configuration in a MacBook air with a Core i5 CPU and 4GB of RAM. The data
is read from the solid state disk and uncompressed using gzip in every iteration. Initialization time is 
excluded from the measurements. Because I used only one node and all the data is local, there is no network overhead
involved.

The following results are for the GaussianMixture Model with 2 components per mixture and 6 iterations.

    Total training time was 33 seconds.
    Training time per observation was 69 microseconds.
    Training time per observation per iteration was 11 microseconds.
    Total testing time was 4 seconds.
    Testing time per observation was 8 microseconds.
    
Based on this number, the data rate at which we injected data for training was (1/11 microseconds) or 90,000 
observations per second. If we look at the ObsEvent class, the effective number of bits transmitted per event is:

    10 x float + 1 x float + 2 x int + 1 x long = 11 x 32 + 2 x 32 + 1 x 64 = 480 bits / observation

This results in an injected data rate of 90,000 x 480 bits/sec = 43 mbps

This is just to get an idea of the execution speed before even thinking about how to optimize. The throughput will vary 
greatly depending on the complexity of the algorithm and the hardware configuration.


Please share your feedback at:
http://groups.google.com/group/s4-project/topics
