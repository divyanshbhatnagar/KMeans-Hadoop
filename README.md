MapReduce is a programming model and implementation for processing and generating large
datasets applicable to a large variety of real-world problems. The implementation of a
MapReduce approach involves the user specifying the computations in terms of map and reduce
function and the underlying runtime system automatically parallelizes the computation across
large-scale clusters of machines, handles machine failures, and schedules inter-machine
communication to make efficient use of the network and disks.
K-Means algorithm is the most commonly used clustering algorithm. It works as follows; first, k
random points are selected from a set of data points, these points act as the initial centroids. In
the next step, each data point is assigned to one of these centroids based on their similarity i.e.
the distance between the two data points. This distance can be euclidean distance or manhattan
distance, based on the implementation. Then we calculate the mean of each formed cluster. This
process is repeated until the function converges i.e. no more centroids are formed.
In k-means algorithm, the most intensive calculation to occur is the calculation of distances. In
each iteration, it would require a total of (nk) distance computations where n is the number of
objects and k is the number of clusters being created. It is obviously that the distance
computations between one object with the centers is irrelevant to the distance computations
between other objects with the corresponding centers. Therefore, distance computations between
different objects with centers can be parallel executed. In each iteration, the new centers, which
are used in the next iteration, should be updated. Hence the iterative procedures must be
executed serially.

The algorithm is as follows:

1. We read the data points sequentially(row by row) from the data file.

2. We look up each data point in the distance matrix calculated for k initial centroids and all
data points, to find the minimum.

3. Once the minimum value is found we associate that data point with the cluster index at
which minimum value is found. The cluster index and data point is then emitted.

4. Now clusters have been formed with their associated data points so we calculate the mean
of each of these clusters which acts as the new centroid for the given data.

5. This process is run until the function converges, i.e. no more centroids are calculated.
The steps 2-3 are the steps of Mapper Function, which reads the data points and emits the cluster
and corresponding data point.

Steps 4-5 are the step for reducer, which calculates the new centroids based on the output of the
mapper.

To improve the efficiency of the MapReduce program we implemented a Combiner task between
Mapper and Reducer to handle intermediate cluster formations, while this approach reduced the
workload for reducer, the computation time increased therefore this wasn’t the best approach.
Next, we tried to check how does the algorithm work to measure speedup, theoretically, it should
scale well as we increase the number of associated computers in the system while keeping the
dataset constant. But, this could not be achieved practically due to scarce resource allocation.

The advantages of using K-means MapReduce are:

● Scalability : Running a parallel K-Means job can be scaled efficiently by adding the
number of resources required. Therefore, hadoop MapReduce K-Means scales well with
larger data.

● Cost effective: Running a MapReduce job on a huge dataset can be easily achieved with
hadoop without much overhead. Also, there is no need to rely on assumptions to reduce
the large dataset into a small one for processing on, say a local machine/limited
resources.

● Resilient to failures : Using Hadoop, helps in maintaining availability, in case of using
multiple nodes, when data is sent to a node its replicas are maintained at other nodes, so
that in case of a failure the replicas can be fetched and data is not lost.

The disadvantages of using K-means MapReduce are:

● Security concerns : The implementation of hadoop framework does not have a very
secure foundation. The data on hadoop can be vulnerable. Hadoop file system lacks
encryption at the storage and network levels.

● Not fit for small data : Due to the high capacity design, hadoop is not suitable for
working with smaller data set requirements, as it lacks the ability to efficiently support
random reading of small files.
