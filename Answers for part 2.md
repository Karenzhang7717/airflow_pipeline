

1)  Please briefly document, for X-Materials’ IT team, why materials data can be particularly challenging data to wrangle.

Since the scenario is to use AI in the development of a new semiconductor compound of unprecedented performance, data wrangling  can be challenging for the reasons below:

a) Machine learning/ AI needs massive amount of data to make right predictions. For materials data in particular, the cost for one experiment trial is expensive. Therefore, it will be hard to gather a large amount of data to make accurate predictions. Also, since material research needs to analyze relationships between composition, structure, properties, the ML model can be very complicated.

b) For materials research, it usually involves multiple different processing tests to analyze different properties. For our case, only two processing techniques (ball milling and hot press) are used. This may not be efficient to find out the best solution for choice of material.

c) For the the development of a new semiconductor compound, existing materials on market now, such as silicon, is a good semiconducting material already since it is abundant and cost-effective. It may be hard to find other materials to have better performance than existing materials under a cost-effective scenario.



2)   Please briefly document, for X-Materials’ IT team, known failure modes of your pipeline, why they may occur, and advice on how they may want to approach these.

a) Since X-Lab's data is a data jungle and they need to manually input their data into txt files, it is likely that wrong information is entered. If the primary key (uid) is entered wrong, information on the massive csv file may be missing. To solve this issue, X-Lab should switch to manageable databases such as Postgress, to make data management more efficient.

b) Under the case that wrong format of data is entered in Postgres database (e.g. supposed to have a real number but entered a character), an execption will occur. In this case, re-examine the input of data and make corresponding corrections.

c)For an unexpected failure of a DAG, while fixing the issue of the failed DAG,  we can re-arrange the sequence of other working  DAGs to continue the data gathering process.

