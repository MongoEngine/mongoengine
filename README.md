MongoMallard
============

MongoMallard is a fast ORM-like layer on top of PyMongo, based on MongoEngine.

* Repository: https://github.com/elasticsales/mongomallard
* See [README_MONGOENGINE](https://github.com/elasticsales/mongomallard/blob/master/README_MONGOENGINE.rst) for MongoEngine's README.
* See [DIFFERENCES](https://github.com/elasticsales/mongomallard/blob/master/DIFFERENCES.md) for differences between MongoEngine and MongoMallard.


Benchmarks
----------

Sample run on a 2.7 GHz Intel Core i5 running OS X 10.8.3

<table>
    <tr>
        <th></th>
        <th>MongoEngine 0.8.2 (ede9fcf)</th>
        <th>MongoMallard (478062c)</th>
        <th>Speedup</th>
    </tr>
    <tr>
        <td>Doc initialization</td>
        <td>52.494us</td>
        <td>25.195us</td>
        <td>2.08x</td>
    </tr>
    <tr>
        <td>Doc getattr</td>
        <td>1.339us</td>
        <td>0.584us</td>
        <td>2.29x</td>
    </tr>
    <tr>
        <td>Doc setattr</td>
        <td>3.064us</td>
        <td>2.550us</td>
        <td>1.20x</td>
    </tr>
    <tr>
        <td>Doc to mongo</td>
        <td>49.415us</td>
        <td>26.497us</td>
        <td>1.86x</td>
    </tr>
    <tr>
        <td>Load from SON</td>
        <td>61.475us</td>
        <td>4.510us</td>
        <td>13.63x</td>
    </tr>
    <tr>
        <td>Save to database</td>
        <td>434.389us</td>
        <td>289.972us</td>
        <td>2.29x</td>
    </tr>
    <tr>
        <td>Load from database</td>
        <td>558.178us</td>
        <td>480.690us</td>
        <td>1.16x</td>
    </tr>
    <tr>
        <td>Save/delete big object to database</td>
        <td>98.838ms</td>
        <td>65.789ms</td>
        <td>1.50x</td>
    </tr>
    <tr>
        <td>Serialize big object from database</td>
        <td>31.390ms</td>
        <td>20.265ms</td>
        <td>1.55x</td>
    </tr>
    <tr>
        <td>Load big object from database</td>
        <td>41.159ms</td>
        <td>1.400ms</td>
        <td>29.40x</td>
    </tr>
</table>

See [tests/benchmark.py](https://github.com/elasticsales/mongomallard/blob/master/tests/benchmark.py) for source code.
